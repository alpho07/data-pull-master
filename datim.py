import os
import mysql.connector
from mysql.connector import pooling
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

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
    CREATE TABLE IF NOT EXISTS datim_master AS
    SELECT
        d.id AS datim_data_id,
        d.attributeOptionCombo,
        d.categoryOptionCombo,
        c.name AS categoryOptionCombo_name,
        c.short_name AS categoryOptionCombo_short_name,
        d.comment,
        d.created,
        d.dataElement,
        e.name AS dataElement_name,
        e.short_name AS dataElement_short_name,
        d.followup,
        d.lastUpdated,
        d.orgUnit,
        o.name AS orgUnit_name,
        o.short_name AS orgUnit_short_name,
        d.period,
        d.storedBy,
        d.value
    FROM datim_data d
    LEFT JOIN datim_categoryOptionCombos c ON d.categoryOptionCombo = c.id
    LEFT JOIN datim_dataElements e ON d.dataElement = e.id
    LEFT JOIN datim_organizationUnits o ON d.orgUnit = o.id;
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
        d.id, d.attributeOptionCombo, d.categoryOptionCombo, c.name, c.short_name,
        d.comment, d.created, d.dataElement, e.name, e.short_name,
        d.followup, d.lastUpdated, d.orgUnit, o.name, o.short_name,
        d.period, d.storedBy, d.value
    FROM datim_data d
    LEFT JOIN datim_categoryOptionCombos c ON d.categoryOptionCombo = c.id
    LEFT JOIN datim_dataElements e ON d.dataElement = e.id
    LEFT JOIN datim_organizationUnits o ON d.orgUnit = o.id;
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
    INSERT INTO datim_master (
        id, attributeOptionCombo, categoryOptionCombo, categoryOptionCombo_name, categoryOptionCombo_short_name,
        comment, created, dataElement, dataElement_name, dataElement_short_name,
        followup, lastUpdated, orgUnit, orgUnit_name, orgUnit_short_name,
        period, storedBy, value
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
    data = fetch_data()
    
    # Use concurrency for inserting data
    with ThreadPoolExecutor(max_workers=5) as executor:
        chunks = list(chunkify(data))
        # Use tqdm to show progress bar
        for _ in tqdm(executor.map(insert_data, chunks), total=len(chunks), desc="Inserting data"):
            pass

if __name__ == "__main__":
    main()
