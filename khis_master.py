import requests
from requests.auth import HTTPBasicAuth
from mysql.connector import pooling
from datetime import datetime
from tqdm import tqdm
import time
import concurrent.futures
from dotenv import load_dotenv
import os
import argparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Load environment variables
load_dotenv()

# MySQL database connection pool configuration
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))

db_config = {
    'host': MYSQL_HOST,
    'database': MYSQL_DATABASE,
    'user': MYSQL_USER,
    'password': MYSQL_PASSWORD,
    'port': MYSQL_PORT,
    'pool_name': 'mypool',
    'pool_size': 10
}

# Create a connection pool
connection_pool = pooling.MySQLConnectionPool(**db_config)

# API credentials and parameters
KHIS_API_BASE_URL = os.getenv("KHIS_API_BASE_URL")
username = os.getenv("KHIS_USERNAME")
password = os.getenv("KHIS_PASSWORD")

uids = {
    "CT": "ptIUGFkE6jn",
    "PMTCT": "xUesg8lcmDs",
    "HIV_TB": "Vo4KDrUFwnA"
}

# Base URL template
base_url_template = "https://hiskenya.org/api/dataValueSets?dataSet={uid}&startDate={startdate}&endDate={enddate}&orgUnit={orgunit}&children=true"

# Create a session object
session = requests.Session()

# Manually add the SESSION cookie
#session.cookies.set('SESSION', 'MzhiMWNhOTMtYmY0Mi00YmFiLThlMjktMjc1ZDE1MjQ4NTE3')

# Set headers to mimic the R request
headers = {
    'User-Agent': 'libcurl/7.84.0 r-curl/5.0.0 httr/1.4.7',
    'Accept': 'application/json, text/xml, application/xml, */*',
    'Accept-Encoding': 'deflate, gzip'
}

# Create a retry strategy
retry_strategy = Retry(
    total=5,  # Number of retry attempts
    backoff_factor=1,  # Wait 1, 2, 4, 8... seconds between retries
    status_forcelist=[429, 500, 502, 503, 504],  # Retry on these HTTP status codes
    allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
)

# Attach the retry strategy to the session
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

# Function to create the table if not exists
def create_table_if_not_exists():
    try:
        connection = connection_pool.get_connection()
        cursor = connection.cursor()
        
        # Create table if it does not exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS khis_data_all (
            id INT AUTO_INCREMENT PRIMARY KEY,
            attributeOptionCombo VARCHAR(255),
            categoryOptionCombo VARCHAR(255),
            comment TEXT,
            created DATETIME,
            dataElement VARCHAR(255),
            followup BOOLEAN,
            lastUpdated DATETIME,
            orgUnit VARCHAR(255),
            period VARCHAR(50),
            storedBy VARCHAR(255),
            value VARCHAR(255),
            UNIQUE KEY unique_key (dataElement, orgUnit, period)
        )
        """
        
        cursor.execute(create_table_query)
        connection.commit()
        print("Table is ready.")
        
    except Exception as e:
        print(f"Error creating table: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Function to upsert datim data into MySQL
def upsert_datim_data(batch):
    try:
        connection = connection_pool.get_connection()
        cursor = connection.cursor()
        
        sql_insert_query = """
        INSERT INTO khis_data_all (
            attributeOptionCombo, categoryOptionCombo, comment, created, 
            dataElement, followup, lastUpdated, orgUnit, period, storedBy, value
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            comment=VALUES(comment),
            created=VALUES(created),
            followup=VALUES(followup),
            lastUpdated=VALUES(lastUpdated),
            storedBy=VALUES(storedBy),
            value=VALUES(value)
        """
        
        cursor.executemany(sql_insert_query, batch)
        connection.commit()
        
    except Exception as e:
        print(f"Error during upsert: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Function to prepare batches of data for upserting
def prepare_batches(data, batch_size=1000):
    batch = []
    for record in data.get('dataValues', []):
        values = (
            record.get('attributeOptionCombo'),
            record.get('categoryOptionCombo'),
            record.get('comment'),
            datetime.strptime(record.get('created'), "%Y-%m-%dT%H:%M:%S.%f%z") if record.get('created') else None,
            record.get('dataElement'),
            record.get('followup'),
            datetime.strptime(record.get('lastUpdated'), "%Y-%m-%dT%H:%M:%S.%f%z") if record.get('lastUpdated') else None,
            record.get('orgUnit'),
            record.get('period'),
            record.get('storedBy'),
            record.get('value')
        )
        batch.append(values)
        
        if len(batch) == batch_size:
            yield batch
            batch = []
    
    # Yield any remaining records
    if batch:
        yield batch

# Function to fetch and insert data for each UID using concurrency
def fetch_and_insert_data_concurrently(uid, startdate, enddate, orgunit):
    base_url = base_url_template.format(uid=uid, startdate=startdate, enddate=enddate, orgunit=orgunit)

    # Make the GET request with the session and the manually set cookie
    try:
        response = session.get(base_url, auth=HTTPBasicAuth(username, password), headers=headers, allow_redirects=True, timeout=3600)

        if response.status_code == 200:
            print(f"Successfully retrieved data for UID: {uid}")
            data = response.json()

            # Prepare batches of data
            batches = list(prepare_batches(data))

            # Use ThreadPoolExecutor to upsert batches concurrently
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                list(tqdm(executor.map(upsert_datim_data, batches), total=len(batches), desc="Upserting batches"))

        else:
            print(f"Failed to retrieve data for UID: {uid}. Status Code: {response.status_code}")
            print(f"Response Content: {response.text}")
    except requests.exceptions.ChunkedEncodingError:
        print("Chunked Encoding Error occurred. Retrying...")

# Main function to run the data fetch and upsert for all UIDs
def main(startdate, enddate, orgunit):
    # Ensure the table exists before inserting data
    create_table_if_not_exists()
    
    for uid_name, uid in uids.items():
        print(f"\nPulling data for {uid_name}...")
        
        # Simulate a progress bar for pulling data
        for _ in tqdm(range(5), desc="Pulling data", unit="step"):
            time.sleep(0.2)  # Simulate time delay for pulling
        
        fetch_and_insert_data_concurrently(uid, startdate, enddate, orgunit)

# Argument parsing
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and upsert DATIM data")
    parser.add_argument('--startdate', required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument('--enddate', required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument('--orgunit', default='HfVjCurKxh2', help="Organization unit ID (default: HfVjCurKxh2)")    
    args = parser.parse_args()
    
    main(args.startdate, args.enddate, args.orgunit)
