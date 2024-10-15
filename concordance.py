import os
import mysql.connector
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

# MySQL connection settings
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))

# SQL queries for various sources
queries = [
    # KHIS: HTS_TST_POS
    """
    SELECT  
        'KHIS' as source,
        'HTS_TST_POS' as indicator,
        siteCode,
        orgUnitName as siteName,
        SUM(value) as value,
        period as quarter
    FROM hiv_data_pull_v2.khis_master
    WHERE numerdom IN('HV01-17','HV01-18','HV01-19','HV01-20','HV01-21','HV01-22','HV01-23','HV01-24','HV01-25')
    GROUP BY siteCode, orgUnitName, period
    """,

    # KHIS: TX_CURR
    """
    SELECT 
        'KHIS' as source,
        'TX_CURR' as indicator,
        siteCode,
        orgUnitName as siteName,
        SUM(value) as value,
        period as quarter
    FROM hiv_data_pull_v2.khis_master
    WHERE numerdom IN('HV03-028','HV03-029','HV03-030','HV03-031','HV03-032','HV03-033','HV03-034','HV03-035','HV03-036','HV03-037')
    AND period = '202406'
    GROUP BY siteCode, orgUnitName, period
    """,

    # KHIS: MAT_HAART
    """
    SELECT 
        'KHIS' as source,
        'MAT_HAART' as indicator,
        siteCode,
        orgUnitName as siteName,
        SUM(value) as value,
        period as quarter
    FROM hiv_data_pull_v2.khis_master
    WHERE numerdom IN('HV02-16','HV02-17','HV02-18','HV02-19','HV02-21')
    GROUP BY siteCode, orgUnitName, period
    """,

    # KHIS: Infant Profilaxis
    """
    SELECT 
        'KHIS' as source,
        'Infant Profilaxis' as indicator,
        siteCode,
        orgUnitName as siteName,
        SUM(value) as value,
        period as quarter
    FROM hiv_data_pull_v2.khis_master
    WHERE numerdom IN('HV02-39','HV02-40','HV02-41')
    GROUP BY siteCode, orgUnitName, period
    """,

    # DATIM: HTS_TST_POS
    """
    SELECT 
        'DATIM' as source,
        'HTS_TST_POS' as indicator,
        siteCode,
        orgUnitName as siteName,
        SUM(value) as value,
        quarter
    FROM hiv_data_pull_v2.datim_master
    WHERE program_area = 'HTS_TST ' AND hiv_status = ' Positive' AND quarter = '2024Q3'
    GROUP BY siteCode, orgUnitName, quarter
    """,

    # DATIM: TX_CURR
    """
    SELECT 
        'DATIM' as source,
        'TX_CURR' as indicator,
        siteCode,
        orgUnitName as siteName,
        SUM(value) as value,
        quarter
    FROM hiv_data_pull_v2.datim_master
    WHERE dataElementId LIKE '%Hyvw9VnZ2ch%' AND quarter = '2024Q3'
    GROUP BY siteCode, orgUnitName, quarter
    """,

    # DATIM: MAT_HAART
    """
    SELECT 
        'DATIM' as source,
        'MAT_HAART' as indicator,
        siteCode,
        orgUnitName as siteName,
        SUM(value) as value,
        quarter
    FROM hiv_data_pull_v2.datim_master
    WHERE program_area LIKE '%PMTCT_ART%' AND quarter = '2024Q3'
    GROUP BY siteCode, orgUnitName, quarter
    """,

    # NDW: HTS_TST_POS
    """
    SELECT 
        'NDW' as source,
        'HTS_TST_POS' as indicator,
        siteCode,
        facilityName as siteName,
        SUM(value) as value,
        ReportMonth_Year as quarter
    FROM hiv_data_pull_v2.ndw_data
    WHERE ReportMonth_Year IN('202404', '202405', '202406') AND indicatorName = 'HTSTSTPOS'
    GROUP BY siteCode, facilityName, ReportMonth_Year
    """,

    # NDW: TX_CURR
    """
    SELECT 
        'NDW' as source,
        'TX_CURR' as indicator,
        siteCode,
        facilityName as siteName,
        SUM(value) as value,
        ReportMonth_Year as quarter
    FROM hiv_data_pull_v2.ndw_data
    WHERE ReportMonth_Year = '202406' AND indicatorName = 'TXCURR'
    GROUP BY siteCode, facilityName, ReportMonth_Year
    """
]

# Establishing MySQL connection


def get_mysql_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        database=MYSQL_DATABASE,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        port=MYSQL_PORT
    )

# Fetch data from MySQL


def fetch_data(query):
    conn = get_mysql_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return pd.DataFrame(data)

# Fetch data from all queries and consolidate into one DataFrame


def consolidate_data(queries):
    all_data = pd.DataFrame()

    for query in tqdm(queries, desc="Fetching data from MySQL"):
        data = fetch_data(query)
        all_data = pd.concat([all_data, data], ignore_index=True)

    return all_data


def pivot_and_calculate_concordance(data):
    # Pivot the data
    pivot_data = data.pivot_table(
        index=["indicator", "siteCode"],
        columns="source",
        values="value",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    # Convert only the numeric columns to float
    numeric_columns = ['KHIS', 'DATIM', 'NDW']
    pivot_data[numeric_columns] = pivot_data[numeric_columns].astype(float)

    # Calculate concordance between KHIS and DATIM
    pivot_data['concordance_KHIS_to_DATIM'] = pivot_data.apply(
        lambda row: (row['DATIM'] / row['KHIS'] *
                     100) if row['KHIS'] != 0 else 0,
        axis=1
    )

    # Calculate concordance between KHIS and NDW
    pivot_data['concordance_KHIS_to_NDW'] = pivot_data.apply(
        lambda row: (row['NDW'] / row['KHIS'] *
                     100) if row['KHIS'] != 0 else 0,
        axis=1
    )

    return pivot_data


# Main function
def main():
    print("Starting data consolidation...")
    all_data = consolidate_data(queries)

    print("Pivoting data and calculating concordance...")
    final_data = pivot_and_calculate_concordance(all_data)

    # Save the final output to a CSV file
    final_data.to_csv("final_consolidated_data.csv", index=False)
    print("Data consolidation complete. Output saved to 'final_consolidated_data.csv'.")


if __name__ == "__main__":
    main()
