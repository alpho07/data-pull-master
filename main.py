import requests
import os
import datetime
import pytz
import pandas as pd
from dateutil.relativedelta import relativedelta
from tqdm import tqdm
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from mysql.connector import pooling, Error
from dotenv import load_dotenv
import time

# Load environment variables from .env file
load_dotenv()

# DHIS2 API Credentials from .env
KHIS_API_BASE_URL = os.getenv("KHIS_API_BASE_URL")
KHIS_USERNAME = os.getenv("KHIS_USERNAME")
KHIS_PASSWORD = os.getenv("KHIS_PASSWORD")

# MySQL Database Connection Parameters from .env
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))

# Create a MySQL connection pool
connection_pool = pooling.MySQLConnectionPool(
    pool_name="khis_pool",
    pool_size=10,  # Increased pool size for concurrent connections
    pool_reset_session=True,
    host=MYSQL_HOST,
    database=MYSQL_DATABASE,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    port=MYSQL_PORT
)

# Function to execute SQL queries using pooled connections with retry logic and batch insert (executemany)
def execute_many_query(query, values, retries=5):
    for i in range(retries):
        try:
            connection = connection_pool.get_connection()
            cursor = connection.cursor()
            cursor.executemany(query, values)
            connection.commit()
            cursor.close()
            connection.close()
            break  # Exit the loop if successful
        except Error as e:
            print(f"Error executing query (attempt {i+1}): {e}")
            time.sleep(2)  # Wait for 2 seconds before retrying
            if i == retries - 1:
                print("Failed after max retries. Skipping query.")
            continue

# Function to update resume tracking
def update_resume_tracking(dx_value, period, status):
    sql = """
    REPLACE INTO khis_resume (dx_value, period, status) VALUES (%s, %s, %s);
    """
    execute_many_query(sql, [(dx_value, period, status)])

# Function to batch API calls with progress tracking
def batch_api_calls(dx_batch, period, start_date, end_date, row_data, progress_bar=None):
    ou = "dimension=ou:LEVEL-5;"
    dx = f"dimension=dx:{';'.join(dx_batch)};"
    pe = f"dimension=pe:{period};"
    query = f"/analytics?{ou}&{dx}&{pe}&displayProperty=NAME&showHierarchy=true&tableLayout=true&columns=dx;pe&rows=ou&hideEmptyRows=true&paging=false"

    url = KHIS_API_BASE_URL + query
    auth = (KHIS_USERNAME, KHIS_PASSWORD)
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.get(url, headers=headers, auth=auth, timeout=1800)
        response.raise_for_status()

        data = response.json()
        if 'rows' in data:
            batch_data = []
            for api_row in data['rows']:
                for row in row_data:
                    # Using the original parsed_data format
                    parsed_data = {
                        'countyName': api_row[1].replace(" County", ""),
                        'subCountyName': api_row[2].replace(" Sub County", ""),
                        'wardName': api_row[3].replace(" Ward", ""),
                        'facilityName': api_row[4],
                        'orgUnitID': api_row[5],
                        'siteCode': api_row[7],
                        'dx_value': row['KHIS UID'],
                        'start_date': start_date,
                        'end_date': end_date,
                        'period': period,
                        'value': int(api_row[9]) if api_row[9] else None,
                        'data_element_name': row['Data Element Name'],
                        'data_element_code': row['Data Element Code'],
                        'program_area': row['Program Area'],
                        'dataset': row['Dataset']
                    }

                    batch_data.append((
                        parsed_data['countyName'], parsed_data['subCountyName'], parsed_data['wardName'],
                        parsed_data['facilityName'], parsed_data['orgUnitID'], parsed_data['siteCode'],
                        parsed_data['dx_value'], parsed_data['start_date'], parsed_data['end_date'], 
                        parsed_data['period'], parsed_data['value'], parsed_data['data_element_name'],
                        parsed_data['data_element_code'], parsed_data['program_area'], parsed_data['dataset']
                    ))

                    # Update resume tracking for each row
                    update_resume_tracking(row['KHIS UID'], period, 'completed')

            # Use executemany to insert all rows in the batch at once
            sql = """
            INSERT INTO khis_data (
                countyName, subCountyName, wardName, facilityName, 
                orgUnitID, siteCode, dx_value, start_date, end_date, period, value,
                data_element_name, data_element_code, program_area, dataset
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            execute_many_query(sql, batch_data)

        if progress_bar:
            progress_bar.update(len(dx_batch))  # Update the progress bar after each batch is processed

    except requests.exceptions.RequestException as e:
        print(f"API batch call for period {period} failed: {e}")
        if progress_bar:
            progress_bar.update(len(dx_batch))  # Still update progress bar to avoid halting

# Function to handle processing with threading and adding a progress bar for API calls
def process_ctx_khis_dash_from_spreadsheet(start_date, end_date, relevant_columns, resume=False, update=False):
    start = datetime.datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    end = datetime.datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    periods = get_khis_periods(start, end)

    dx_per_batch = 5  # Number of dx values per API batch
    with ThreadPoolExecutor(max_workers=8) as executor:  # Limit workers based on pool size
        for period in tqdm(periods, desc="Processing periods"):
            progress_bar = tqdm(total=len(relevant_columns), desc=f"Processing API calls for period {period}")
            futures = []
            for i in range(0, len(relevant_columns), dx_per_batch):
                dx_batch = relevant_columns.iloc[i:i + dx_per_batch]['KHIS UID'].tolist()
                row_data = relevant_columns.iloc[i:i + dx_per_batch].to_dict('records')
                futures.append(executor.submit(batch_api_calls, dx_batch, period, start_date, end_date, row_data, progress_bar))

            for future in as_completed(futures):
                try:
                    future.result()  # Get the result (for error handling)
                except Exception as e:
                    print(f"Error processing batch: {e}")
            progress_bar.close()

# Function to calculate start and end date based on LAST_n_MONTHS
def calculate_last_n_months(n):
    today = datetime.datetime.today()
    end_date = today.replace(day=1) + relativedelta(months=1) - datetime.timedelta(days=1)  # Last day of current month
    start_date = end_date.replace(day=1) - relativedelta(months=n - 1)
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

# Function to get KHIS periods
def get_khis_periods(start, end):
    periods = []
    while start <= end:
        periods.append(start.strftime("%Y%m"))
        start = start + relativedelta(months=1)  # Increment by one month
    return periods

# Argument parser for command-line inputs
def parse_args():
    parser = argparse.ArgumentParser(description='Process KHIS data')
    parser.add_argument('--start_date', help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end_date', help='End date in YYYY-MM-DD format')
    parser.add_argument('--last_n_months', type=int, help='Specify LAST_n_MONTHS for dynamic time range (alternative to start and end date)')
    parser.add_argument('--program_area', default='all', help='Program area to filter (default: all)')
    parser.add_argument('--dataset', help='Dataset to filter (optional)')
    parser.add_argument('--limit', type=int, help='Limit the number of datasets to process for testing (optional)')
    parser.add_argument('--resume', action='store_true', help='Resume processing from last failure')
    parser.add_argument('--update', action='store_true', help='Only pull updated data')
    return parser.parse_args()

# Argument parsing
args = parse_args()

# Load the spreadsheet
file_path = 'khis_data_elements.xlsx'
spreadsheet_data = pd.read_excel(file_path)

# Clean up column names and filter the data based on command-line arguments
spreadsheet_data.columns = spreadsheet_data.columns.str.strip()
filtered_data = spreadsheet_data
if args.program_area != 'all':
    filtered_data = filtered_data[filtered_data['Program Area'] == args.program_area]
if args.dataset:
    filtered_data = filtered_data[filtered_data['Dataset'] == args.dataset]
if args.limit:
    filtered_data = filtered_data.head(args.limit)

# Handle dynamic period calculation based on LAST_n_MONTHS
if args.last_n_months:
    start_date, end_date = calculate_last_n_months(args.last_n_months)
else:
    if not args.start_date or not args.end_date:
        raise ValueError("You must specify either start_date and end_date or last_n_months")
    start_date = args.start_date
    end_date = args.end_date

# Process the data based on the filtered KHIS UIDs
process_ctx_khis_dash_from_spreadsheet(
    start_date, end_date, filtered_data, resume=args.resume, update=args.update)

# Close the connection pool after completing the process
#connection_pool.close()
