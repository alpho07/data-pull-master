import requests
import os
from dotenv import load_dotenv
import datetime
import pytz
import mysql.connector
import pandas as pd
from dateutil.relativedelta import relativedelta
from tqdm import tqdm
import argparse

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

# Establish MySQL Connection
db_connection = mysql.connector.connect(
    host=MYSQL_HOST,
    database=MYSQL_DATABASE,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    port=MYSQL_PORT
)

cursor = db_connection.cursor()

# Create the khis_data table if it does not exist
create_table_query = """
CREATE TABLE IF NOT EXISTS khis_data (
    id INT AUTO_INCREMENT PRIMARY KEY,   
    countyName VARCHAR(100) NOT NULL,
    subCountyName VARCHAR(100) NOT NULL,
    wardName VARCHAR(100) NOT NULL,
    facilityName VARCHAR(255) NOT NULL,
    orgUnitID VARCHAR(50) NOT NULL,
    siteCode VARCHAR(50) NOT NULL,
    dx_value VARCHAR(50) NOT NULL,  -- Column for dx value (KHIS UID)
    start_date DATE NOT NULL,       -- Start date
    end_date DATE NOT NULL,         -- End date
    period VARCHAR(6) NOT NULL,     -- Period in format YYYYMM
    value INT,
    data_element_name VARCHAR(255), -- Data Element Name from the spreadsheet
    data_element_code VARCHAR(50),  -- Data Element Code from the spreadsheet
    program_area VARCHAR(100),      -- Program Area from the spreadsheet
    dataset VARCHAR(50),            -- Dataset from the spreadsheet
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
"""

try:
    cursor.execute(create_table_query)
    db_connection.commit()
    print("Table 'khis_data' is ready.")
except mysql.connector.Error as err:
    print(f"Error creating table: {err}")
    db_connection.rollback()

# Create the resume table if it does not exist
create_resume_table_query = """
CREATE TABLE IF NOT EXISTS khis_resume (
    id INT AUTO_INCREMENT PRIMARY KEY,
    dx_value VARCHAR(50),
    period VARCHAR(6),
    status ENUM('completed', 'failed') DEFAULT 'failed',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
"""

try:
    cursor.execute(create_resume_table_query)
    db_connection.commit()
    print("Resume tracking table 'khis_resume' is ready.")
except mysql.connector.Error as err:
    print(f"Error creating resume table: {err}")
    db_connection.rollback()

# Sample model for handling data insertion, saving data to MySQL


class KHIS_SOURCE:
    @staticmethod
    def create(data):
        sql = """
        INSERT INTO khis_data (
            countyName, subCountyName, wardName, facilityName, 
            orgUnitID, siteCode, dx_value, start_date, end_date, period, value,
            data_element_name, data_element_code, program_area, dataset
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        val = (
            data.get('countyName'),
            data.get('subCountyName'),
            data.get('wardName'),
            data.get('facilityName'),
            data.get('orgUnitID'),
            data.get('siteCode'),
            data.get('dx_value'),        # KHIS UID
            data.get('start_date'),      # Start date
            data.get('end_date'),        # End date
            data.get('period'),          # Period in YYYYMM format
            data.get('value'),
            data.get('data_element_name'),
            data.get('data_element_code'),
            data.get('program_area'),
            data.get('dataset')
        )
        try:
            cursor.execute(sql, val)
            db_connection.commit()
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            db_connection.rollback()

    @staticmethod
    def is_data_updated(period, dx_value):
        sql = """
        SELECT updated_at FROM khis_data 
        WHERE period = %s AND dx_value = %s
        ORDER BY updated_at DESC LIMIT 1;
        """
        cursor.execute(sql, (period, dx_value))
        result = cursor.fetchone()
        return result

# Helper function to track processing status for resume


def update_resume_tracking(dx_value, period, status):
    sql = """
    REPLACE INTO khis_resume (dx_value, period, status) VALUES (%s, %s, %s);
    """
    cursor.execute(sql, (dx_value, period, status))
    db_connection.commit()

# Function to handle KHIS periods using relativedelta


def get_khis_periods(start, end):
    periods = []
    while start <= end:
        periods.append(start.strftime("%Y%m"))
        start = start + relativedelta(months=1)  # Increment by one month
    return periods

# Function to calculate start and end date based on LAST_n_MONTHS


def calculate_last_n_months(n):
    today = datetime.datetime.today()
    end_date = today.replace(day=1) + relativedelta(months=1) - \
        datetime.timedelta(days=1)  # Last day of current month
    # First day of the n-th month
    start_date = end_date.replace(day=1) - relativedelta(months=n - 1)
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

# Function to process data using KHIS UIDs from the spreadsheet


def process_ctx_khis_dash_from_spreadsheet(start_date, end_date, relevant_columns, resume=False, update=False):
    start = datetime.datetime.strptime(
        start_date, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    end = datetime.datetime.strptime(
        end_date, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    periods = get_khis_periods(start, end)

    # Add progress bars for tracking
    period_progress = tqdm(periods, desc="Processing periods")

    for period in period_progress:
        # Loop through KHIS UIDs from the spreadsheet with progress indicator
        for index, row in tqdm(relevant_columns.iterrows(), total=relevant_columns.shape[0], desc=f"Processing KHIS UIDs for period {period}"):
            dx = row['KHIS UID']  # Use the KHIS UID from the spreadsheet
            dataset = row['Dataset']

            # Skip if using resume and this has already been processed
            if resume:
                sql = """
                SELECT status FROM khis_resume WHERE dx_value = %s AND period = %s;
                """
                cursor.execute(sql, (dx, period))
                result = cursor.fetchone()
                if result and result[0] == 'completed':
                    continue  # Skip already processed data

            # Skip if using update and data has not been updated
            if update:
                last_updated = KHIS_SOURCE.is_data_updated(period, dx)
                if last_updated:
                    print(
                        f"Data already updated for period {period} and dx {dx}. Skipping...")
                    continue  # Skip already updated data

            # Skip Ver.2023 datasets if the periods don't include Feb 2024 or earlier
            if dataset == "Ver.2023":
                feb_2024 = datetime.datetime(2024, 2, 29)
                if end < feb_2024:
                    continue  # Skip if the period is beyond February 2024

            # API call with the current dx (KHIS UID)
            ou = "dimension=ou:LEVEL-5;"
            de = f"dimension=dx:{dx};"
            pe = f"dimension=pe:{period};"
            query = f"/analytics?{ou}&{de}&{pe}&displayProperty=NAME&showHierarchy=true&tableLayout=true&columns=dx;pe&rows=ou&hideEmptyRows=true&paging=false"

            url = KHIS_API_BASE_URL + query
            auth = (KHIS_USERNAME, KHIS_PASSWORD)
            headers = {"Content-Type": "application/json"}

            try:
                response = requests.get(
                    url, headers=headers, auth=auth, timeout=1800)  # 30 min timeout
                response.raise_for_status()

                data = response.json()
                if 'rows' in data:
                    for api_row in data['rows']:
                        # Prepare data for database insertion
                        parsed_data = {
                            # 'Siaya'
                            'countyName': api_row[1].replace(" County", ""),
                            # 'Ugenya'
                            'subCountyName': api_row[2].replace(" Sub County", ""),
                            # 'West Ugenya'
                            'wardName': api_row[3].replace(" Ward", ""),
                            # 'Bar Achuth Dispensary'
                            'facilityName': api_row[4],
                            'orgUnitID': api_row[5],  # 'DqwooToZ1N9'
                            'siteCode': api_row[7],  # '13495'
                            'dx_value': dx,  # KHIS UID from the spreadsheet
                            'start_date': start_date,
                            'end_date': end_date,
                            'period': period,  # Save the period in YYYYMM format
                            # '79'
                            'value': int(api_row[9]) if api_row[9] else None,
                            # Extra fields from the spreadsheet
                            'data_element_name': row['Data Element Name'],
                            'data_element_code': row['Data Element Code'],
                            'program_area': row['Program Area'],
                            'dataset': row['Dataset']
                        }

                        # Save to the MySQL database
                        KHIS_SOURCE.create(parsed_data)

                        # Update resume tracking as completed
                        update_resume_tracking(dx, period, 'completed')

            except requests.exceptions.RequestException as e:
                print(
                    f"ProcessCTXKHISDash for {period} with UID {dx} failed at {datetime.datetime.now()} Reason: {e}")
                # Mark resume tracking as failed
                update_resume_tracking(dx, period, 'failed')

# Argument parser for command-line inputs


def parse_args():
    parser = argparse.ArgumentParser(description='Process KHIS data')
    parser.add_argument('--start_date', help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end_date', help='End date in YYYY-MM-DD format')
    parser.add_argument('--last_n_months', type=int,
                        help='Specify LAST_n_MONTHS for dynamic time range (alternative to start and end date)')
    parser.add_argument('--program_area', default='all',
                        help='Program area to filter (default: all)')
    parser.add_argument('--dataset', help='Dataset to filter (optional)')
    parser.add_argument(
        '--limit', type=int, help='Limit the number of datasets to process for testing (optional)')
    parser.add_argument('--resume', action='store_true',
                        help='Resume processing from last failure')
    parser.add_argument('--update', action='store_true',
                        help='Only pull updated data')

    return parser.parse_args()


# Load the spreadsheet
file_path = 'khis_data_elements.xlsx'
spreadsheet_data = pd.read_excel(file_path)

# Clean up column names (trim spaces) and extract KHIS UID column
spreadsheet_data.columns = spreadsheet_data.columns.str.strip()

# Argument parsing
args = parse_args()

# Filter by program area and dataset if specified
filtered_data = spreadsheet_data
if args.program_area != 'all':
    filtered_data = filtered_data[filtered_data['Program Area']
                                  == args.program_area]

if args.dataset:
    filtered_data = filtered_data[filtered_data['Dataset'] == args.dataset]

# Apply the limit if specified
if args.limit:
    filtered_data = filtered_data.head(args.limit)

# Handle dynamic period calculation based on LAST_n_MONTHS
if args.last_n_months:
    start_date, end_date = calculate_last_n_months(args.last_n_months)
else:
    if not args.start_date or not args.end_date:
        raise ValueError(
            "You must specify either start_date and end_date or last_n_months")
    start_date = args.start_date
    end_date = args.end_date

# Process the data based on the filtered KHIS UIDs
process_ctx_khis_dash_from_spreadsheet(
    start_date, end_date, filtered_data, resume=args.resume, update=args.update)

# Close the cursor and the database connection at the end
cursor.close()
db_connection.close()
