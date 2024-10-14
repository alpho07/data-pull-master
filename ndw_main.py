import os
import requests
from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta
import mysql.connector
from mysql.connector import pooling
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm  # Import tqdm for the progress bar
import pprint

# Load environment variables from .env file
load_dotenv()

# Database connection parameters
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))

# Set up connection pool
dbconfig = {
    "host": MYSQL_HOST,
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "database": MYSQL_DATABASE,
    "port": MYSQL_PORT
}

connection_pool = pooling.MySQLConnectionPool(pool_name="mypool", pool_size=5, **dbconfig)

# Function to establish a database connection from the pool
def create_database_connection():
    return connection_pool.get_connection()

# Function to drop and create the table with the new name ndw_data
def create_table():
    connection = create_database_connection()
    cursor = connection.cursor()
    
    # Drop the table if it exists
    drop_table_query = "DROP TABLE IF EXISTS ndw_data"
    cursor.execute(drop_table_query)

    # Create the table
    create_table_query = """
    CREATE TABLE ndw_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        siteCode VARCHAR(255),
        facilityName VARCHAR(255),
        countyName VARCHAR(255),
        ReportMonth_Year VARCHAR(6),
        month VARCHAR(2),
        year VARCHAR(4),
        value INT,
        program VARCHAR(255) DEFAULT NULL,
        indicatorName VARCHAR(255)  -- New column for the indicator name
    )
    """
    
    cursor.execute(create_table_query)
    connection.commit()
    cursor.close()
    connection.close()

# Function to calculate periods between start and end dates
def get_ndw_periods(start_date, end_date):
    periods = []

    # Adjust start and end period to the beginning and end of the months
    start_period = start_date.replace(day=1)  # Start of the month
    end_period = end_date + relativedelta(day=31)  # End of the month

    # Calculate the difference in months
    diff = (end_period.year - start_period.year) * 12 + (end_period.month - start_period.month)

    # Generate the periods in "YYYYMM" format
    for i in range(diff + 1):  # +1 to include the end period
        current_period = start_period + relativedelta(months=i)
        periods.append(current_period.strftime("%Y%m"))

    return periods

# Function to insert data into the new table with the updated name
def insert_data(data_objs):
    connection = create_database_connection()
    cursor = connection.cursor()
    
    insert_query = """
    INSERT INTO ndw_data (siteCode, facilityName, countyName, ReportMonth_Year, month, year, value, program, indicatorName)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Use tqdm to display a progress bar
    for data_chunk in tqdm(data_objs, desc="Inserting data", unit="record"):
        cursor.execute(insert_query, data_chunk)

    connection.commit()
    cursor.close()
    connection.close()

# Function to process each period for a given indicator
def process_indicator_ndw_dash_for_period(period, indicator_name):
    ps = "pageSize=5000"
    pn = "pageNumber=1"
    pe = f"period={period}"
    cd = "code=DEA"
    nm = "name=DataExchange"
    iN = f"indicatorName={indicator_name}"  # Use the indicator name from the parameter
    query = f"/api/Dataset/v2?{pn}&{ps}&{cd}&{nm}&{iN}&{pe}"
    
    try:
        # Step 1: Get access token
        auth_url = os.getenv("NDW_AUTH_URL")
        token_data = {
            "grant_type": "client_credentials",
            "scope": os.getenv("NDW_SCOPE")
        }
        token_response = requests.post(
            auth_url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            auth=(os.getenv("NDW_CLIENT_ID"), os.getenv("NDW_CLIENT_SECRET")),
            data=token_data
        )
        
        token_response.raise_for_status()  # Check if request was successful
        access_token = token_response.json().get("access_token")
        
        # Step 2: Make an authenticated API request using the access token
        api_base_url = os.getenv("NDW_API_BASE_URL")
        response = requests.get(
            api_base_url + query,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json"
            },
            timeout=1800  # 30 minutes
        )
        
        response.raise_for_status()  # Check if request was successful
        
        if response.json() and response.json().get("extract"):
            extract = response.json()["extract"]
            data_objs = []
            for item in extract:
                # Construct the data object
                data_obj = (
                    item.get("FacilityCode", "").strip() if isinstance(item.get("FacilityCode"), str) else None,
                    item.get("FacilityName", "").strip() if isinstance(item.get("FacilityName"), str) else None,
                    item.get("County", "").strip() if isinstance(item.get("County"), str) else None,
                    item.get("period"),
                    item.get("period", "")[4:6] if item.get("period") else None,
                    item.get("period", "")[0:4] if item.get("period") else None,
                    item.get("indicator_value"),
                    None,  # Default value for 'program'
                    indicator_name  # Include the current indicator name
                )
                data_objs.append(data_obj)

            # Insert data with a progress bar
            insert_data(data_objs)
                
    except requests.exceptions.RequestException as e:
        print(f"ProcessIndicator NDW Dash for {period} and indicator {indicator_name} failed at {datetime.now()} Reason: {str(e)}")

# Function to process data for a date range with batching and threading for multiple indicators
def process_indicator_ndw_dash(start_date, end_date, indicator_names):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    periods = get_ndw_periods(start, end)

    # Create the table before processing data
    create_table()
    
    # Using ThreadPoolExecutor for concurrent processing of periods and indicators
    with ThreadPoolExecutor(max_workers=5) as executor:
        for indicator in indicator_names:
            for period in periods:
                executor.submit(process_indicator_ndw_dash_for_period, period, indicator)

# Example usage
indicator_names = ["HTSTSTPOS", "PrEPNEW", "TXCURR", "HTSTST", "TXNEW"]
process_indicator_ndw_dash("2024-01-01", "2024-12-31", indicator_names)
