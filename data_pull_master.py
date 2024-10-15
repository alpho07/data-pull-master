import os
import mysql.connector
from mysql.connector import pooling
import pandas as pd
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Load environment variables
load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))

# Create a connection pool using mysql.connector.pooling
connection_pool = pooling.MySQLConnectionPool(
    pool_name="mypool",
    pool_size=5,  # Maximum number of connections in the pool
    pool_reset_session=True,
    host=MYSQL_HOST,
    database=MYSQL_DATABASE,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    port=MYSQL_PORT
)

# Queries
QUERIES = {
    'KHIS': """
        SELECT *, 'HTS_TST_POS' FROM hiv_data_pull_v2.khis_master
        WHERE numerdom IN('HV01-17','HV01-18','HV01-19','HV01-20','HV01-21','HV01-22','HV01-23','HV01-24','HV01-25')
        UNION ALL
        SELECT *, 'TX_CURR' FROM hiv_data_pull_v2.khis_master
        WHERE numerdom IN('HV03-028','HV03-029','HV03-030','HV03-031','HV03-032','HV03-033','HV03-034','HV03-035','HV03-036','HV03-037')
        AND period='202406'
        UNION ALL
        SELECT *, 'PMTCT_ART(MAT HAART)' FROM hiv_data_pull_v2.khis_master
        WHERE numerdom IN('HV02-16','HV02-17','HV02-18','HV02-19','HV02-21')        
        UNION ALL
        SELECT *,'Infant Profilaxis' FROM hiv_data_pull_v2.khis_master
        WHERE numerdom IN('HV02-39','HV02-40','HV02-41')        
    """,
    'DATIM': """
        SELECT *, 'HTS_TST_POS' FROM hiv_data_pull_v2.datim_master
        WHERE program_area='HTS_TST '
        AND hiv_status=' Positive'
        AND quarter='2024Q3'
        UNION ALL
        SELECT *, 'TX_CURR' FROM hiv_data_pull_v2.datim_master
        WHERE dataElementId LIKE '%Hyvw9VnZ2ch%'
        AND quarter='2024Q3'
        UNION ALL
        SELECT *, 'PMTCT_ART(MAT HAART)' FROM hiv_data_pull_v2.datim_master
        WHERE program_area LIKE '%PMTCT_ART%'
        AND quarter='2024Q3';
    """,
    'NDW': """
        SELECT * FROM hiv_data_pull_v2.ndw_data
        WHERE ReportMonth_Year IN('202404','202405','202406')
        AND indicatorName='HTSTSTPOS'
        UNION ALL
        SELECT * FROM hiv_data_pull_v2.ndw_data
        WHERE ReportMonth_Year IN('202406')
        AND indicatorName='TXCURR';
    """
}

# Function to execute a query and return the result
def execute_query(sheet_name, query):
    connection = connection_pool.get_connection()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query)
        result = cursor.fetchall()
        df = pd.DataFrame(result)
        return sheet_name, df
    finally:
        cursor.close()
        connection.close()

# Function to export each query result to an Excel sheet concurrently
def export_to_excel_concurrently():
    # Use ThreadPoolExecutor for concurrent query execution
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        
        # Submit each query execution to the executor
        for sheet_name, query in QUERIES.items():
            futures.append(executor.submit(execute_query, sheet_name, query))

        # Write results to Excel with progress tracking
        with pd.ExcelWriter('hiv_data_export_concurrent.xlsx', engine='openpyxl') as writer:
            for future in tqdm(as_completed(futures), total=len(QUERIES), desc="Exporting Data"):
                sheet_name, df = future.result()
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"{sheet_name} export completed.")
    
    print("Data export completed successfully!")

# Main execution
if __name__ == "__main__":
    export_to_excel_concurrently()
