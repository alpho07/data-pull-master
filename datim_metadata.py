import os
import requests
from requests.auth import HTTPBasicAuth
from concurrent.futures import ThreadPoolExecutor, as_completed
import mysql.connector
from mysql.connector import pooling
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

# Fetch MySQL environment variables
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))

# Set up MySQL connection pooling
connection_pool = pooling.MySQLConnectionPool(
    pool_name="mypool",
    pool_size=5,  # Adjust the pool size as per requirements
    host=MYSQL_HOST,
    database=MYSQL_DATABASE,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    port=MYSQL_PORT
)

# Base URLs and credentials
base_urls = {
    "dataElements": "https://www.datim.org/api/dataElements?fields=id,name,shortName&paging=false",
    "categoryOptionCombos": "https://www.datim.org/api/categoryOptionCombos?fields=id,name,shortName&paging=false",
    "organisationUnits": "https://www.datim.org/api/organisationUnits/HfVjCurKxh2?fields=id,name,code&paging=false&includeDescendants=true",
}

username = "interagency-api-ken-interagency-9165"
password = "IJH;zkRvns8ZFA6"

# Create a session object
session = requests.Session()

# Function to make API request and return data
def fetch_data(url):
    response = session.get(url, auth=HTTPBasicAuth(username, password), allow_redirects=True, timeout=3600)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to retrieve data from {url}, Status Code: {response.status_code}")

# Function to save data to the database
def save_to_db(connection, table_name, data):
    cursor = connection.cursor()

    # Drop table if exists
    cursor.execute(f"DROP TABLE IF EXISTS datim_{table_name}")

    # Modify table creation and insertion logic if the job is 'organisationUnits'
    if table_name == "organisationUnits":
        cursor.execute(f"""
            CREATE TABLE datim_{table_name} (
                id VARCHAR(255) PRIMARY KEY,
                name VARCHAR(255),
                short_name VARCHAR(255),
                code VARCHAR(255)
            )
        """)

        # Insert data with 'code' field
        insert_query = f"INSERT INTO datim_{table_name} (id, name, short_name, code) VALUES (%s, %s, %s, %s)"
        cursor.executemany(insert_query, [
            (item['id'], item['name'], item.get('shortName', None), item.get('code', None)) for item in data
        ])
    else:
        # General case for other tables
        cursor.execute(f"""
            CREATE TABLE datim_{table_name} (
                id VARCHAR(255) PRIMARY KEY,
                name VARCHAR(255),
                short_name VARCHAR(255)
            )
        """)

        # Insert data using executemany for bulk insert
        insert_query = f"INSERT INTO datim_{table_name} (id, name, short_name) VALUES (%s, %s, %s)"
        cursor.executemany(insert_query, [
            (item['id'], item['name'], item.get('shortName', None)) for item in data
        ])
    
    connection.commit()
    cursor.close()

# Main function to fetch and save data concurrently
def process_job(job_name, url):
    print(f"Starting {job_name}...")

    # Fetch data
    data = fetch_data(url)

    # Get connection from pool
    connection = connection_pool.get_connection()

    try:
        save_to_db(connection, job_name, data.get(job_name, []))
        print(f"Completed {job_name}")
    finally:
        # Return connection to the pool
        connection.close()

# Using ThreadPoolExecutor for concurrency and tqdm for progress tracking
def main():
    # Prepare progress bar
    jobs = list(base_urls.items())
    progress = tqdm(total=len(jobs), desc="Saving data", unit="job")

    # Concurrent execution
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(process_job, job_name, url): job_name for job_name, url in jobs}
        for future in as_completed(futures):
            job_name = futures[future]
            try:
                future.result()  # This raises any exceptions that occurred during execution
            except Exception as e:
                print(f"Error processing {job_name}: {e}")
            progress.update(1)  # Update progress bar
    
    progress.close()

# Run the main function
if __name__ == "__main__":
    main()
