import os
import mysql.connector
from mysql.connector import pooling
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

from dotenv import load_dotenv

load_dotenv()

# Load MySQL environment variables
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))

# Initialize MySQL connection pool
pool = pooling.MySQLConnectionPool(
    pool_name="mypool",
    pool_size=10,  # Adjust pool size based on needs
    pool_reset_session=True,
    host=MYSQL_HOST,
    database=MYSQL_DATABASE,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    port=MYSQL_PORT
)

# Create the datim_master table
def create_master_table():
    query = """
    CREATE TABLE IF NOT EXISTS khis_master (
        id INT PRIMARY KEY AUTO_INCREMENT,
        khis_data_id INT,
        categoryOptionComboId VARCHAR(255),
        categoryOptionComboName VARCHAR(255),
        age_group VARCHAR(255),
        sex VARCHAR(255),
        hiv_status VARCHAR(255),
        dataElementId VARCHAR(255),
        dataElementName VARCHAR(255),
        program_area VARCHAR(255),
        service_del VARCHAR(255),
        numerdom VARCHAR(255),
        disaggregation VARCHAR(255),
        modality VARCHAR(255),
        siteCode VARCHAR(255),
        orgUnitId VARCHAR(255),
        orgUnitName VARCHAR(255),
        period VARCHAR(50),
        quarter VARCHAR(50),
        value VARCHAR(255)
    );
    """
    connection = pool.get_connection()
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()
    cursor.close()
    connection.close()

# Function to fetch data for insertion into datim_master
def fetch_data():
    query = """
    SELECT
        d.id khis_data_id,  
        d.categoryOptionCombo AS categoryOptionComboId, 
        c.name AS categoryOptionComboName,
        SUBSTRING_INDEX(c.name, ',', 1) AS age_group,    -- Extracts the first part before the first comma
        SUBSTRING_INDEX(SUBSTRING_INDEX(c.name, ',', 2), ',', -1) AS sex,  -- Extracts the second part before the second comma
        SUBSTRING_INDEX(c.name, ',', -1) AS hiv_status,   -- Extracts the last part after the last comma
        d.dataElement AS dataElementId, 
        e.name AS dataElementName, 
        SUBSTRING_INDEX(e.name, '(', 1) AS program_area, 
        SUBSTRING_INDEX(SUBSTRING_INDEX(e.name, ',', 2), ',', -1) AS service_del,
        SUBSTRING_INDEX(e.name, ' ', -1) AS numerdom,
        TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(e.name, ',', 3), ',', -1)) AS disaggregation,
        CASE 
            WHEN LOWER(SUBSTRING_INDEX(e.name, ':', -1)) LIKE '%received results%' THEN
                TRIM(SUBSTRING_INDEX(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(e.name, ',', 3), ',', -1)), '/', 1))
            ELSE NULL
        END AS modality,
        TRIM(REPLACE(o.code,'Keipsl','')) siteCode,
        d.orgUnit AS orgUnitId, 
        o.name AS orgUnitName, 
        d.period,  
        CASE 
           WHEN d.period='2023Q4' THEN '2024Q1' 
           WHEN d.period='2024Q1' THEN '2024Q2' 
           WHEN d.period='2024Q2' THEN '2024Q3' 
           WHEN d.period='2024Q3' THEN '2024Q4' 
        ELSE 'N/A' END quarter,           
        d.value
    FROM khis_data_all d
    LEFT JOIN khis_categoryOptionCombos c ON d.categoryOptionCombo = c.id
    LEFT JOIN khis_dataElements e ON d.dataElement = e.id
    LEFT JOIN khis_organisationUnits o ON d.orgUnit = o.id;
    """
    connection = pool.get_connection()
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    return result

# Function to insert data into datim_master in chunks
def insert_data(chunk):
    query = """
    INSERT INTO khis_master (
        khis_data_id, categoryOptionComboId, categoryOptionComboName, age_group, sex, hiv_status,
        dataElementId, dataElementName, program_area, service_del, numerdom, disaggregation, modality,siteCode,
        orgUnitId, orgUnitName, period, quarter, value
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    connection = pool.get_connection()
    cursor = connection.cursor()
    cursor.executemany(query, chunk)
    connection.commit()
    cursor.close()
    connection.close()

# Function to split data into chunks
def chunkify(data, chunk_size=1000):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

# Main function to handle fetching, chunking, and inserting data
def main():
    create_master_table()
    print('KHIS Master table created successfully....')
    print('Fetching data... Please wait...')
    data = fetch_data()
    
    
    # Use concurrency for inserting data
    with ThreadPoolExecutor(max_workers=5) as executor:
        chunks = list(chunkify(data))
        # Use tqdm to show progress bar
        for _ in tqdm(executor.map(insert_data, chunks), total=len(chunks), desc="Inserting data"):
            pass

if __name__ == "__main__":
    main()
