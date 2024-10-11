import os
import requests
import pandas as pd
import pyodbc
import sys
import logging
from dotenv import load_dotenv

# =======================
# Configuration and Setup
# =======================

# Specify the path to the .env file
load_dotenv("C:/Users/david/nasa_project/.env")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("nasa_mars_rover_data.log")
    ]
)

# =======================
# 1. Fetch Data from NASA API
# =======================

def fetch_nasa_mars_rover_photos(sol=1000):
    API_KEY = os.getenv('NASA_API_KEY')
    
    if not API_KEY:
        logging.error("NASA_API_KEY not found in environment variables.")
        sys.exit(1)
    
    params = {
        'sol': sol,
        'api_key': API_KEY
    }
    
    url = 'https://api.nasa.gov/mars-photos/api/v1/rovers/curiosity/photos'
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        photos = data.get('photos', [])
        logging.info(f"Number of photos fetched: {len(photos)}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from NASA API: {e}")
        photos = []
    
    if photos:
        photos_df = pd.json_normalize(photos)
        logging.info("Data fetched successfully and converted to DataFrame.")
        logging.debug(f"DataFrame Head:\n{photos_df.head()}")
        return photos_df
    else:
        logging.warning("No photos to process. Exiting.")
        sys.exit(1)

# =======================
# 2. Connect to SQL Server using pyodbc
# =======================

def connect_to_sql_server():
    driver = os.getenv('DB_DRIVER')
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_DATABASE')
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')
    
    missing_vars = [var for var in ['DB_DRIVER', 'DB_SERVER', 'DB_DATABASE', 'DB_USERNAME', 'DB_PASSWORD']
                    if not os.getenv(var)]
    
    if missing_vars:
        logging.error(f"Missing environment variables: {', '.join(missing_vars)}")
        sys.exit(1)
    
    try:
        connection_string = (
            f"DRIVER={driver};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"TrustServerCertificate=yes;"
        )
        cnxn = pyodbc.connect(connection_string)
        logging.info("Connection to SQL Server successful.")
        return cnxn
    except pyodbc.Error as e:
        logging.error(f"Error connecting to SQL Server: {e}")
        sys.exit(1)

# =======================
# 3. Create Table in SQL Server
# =======================

def create_table_if_not_exists(cursor, table_name, dataframe):
    # Rename columns to use underscores instead of dots
    dataframe.rename(columns={
        'camera.name': 'camera_name',
        'camera.full_name': 'camera_full_name',
        'rover.id': 'rover_id',
        'rover.name': 'rover_name',
        'rover.launch_date': 'rover_launch_date',
        'rover.landing_date': 'rover_landing_date',
        'rover.status': 'rover_status'
    }, inplace=True)
    
    # Define SQL data types mapping based on DataFrame dtypes
    sql_types = {
        'int64': 'BIGINT',
        'float64': 'FLOAT',
        'object': 'NVARCHAR(MAX)',
        'datetime64[ns]': 'DATETIME',
        'bool': 'BIT',
        'datetime64[ns, UTC]': 'DATETIME'
    }
    
    columns_with_types = []
    for column, dtype in dataframe.dtypes.items():
        sql_type = sql_types.get(str(dtype), 'NVARCHAR(MAX)')  # Default to NVARCHAR(MAX)
        if column == 'id':
            column_def = f"[{column}] {sql_type} PRIMARY KEY"
        else:
            column_def = f"[{column}] {sql_type}"
        columns_with_types.append(column_def)
    
    columns_sql = ",\n    ".join(columns_with_types)
    
    create_table_query = f"""
    IF OBJECT_ID('[{table_name}]', 'U') IS NULL
    BEGIN
        CREATE TABLE [{table_name}] (
            {columns_sql}
        );
        PRINT 'Table {table_name} created successfully.'
    END
    ELSE
    BEGIN
        PRINT 'Table {table_name} already exists.'
    END
    """
    
    print(create_table_query)  # For debugging
    
    try:
        cursor.execute(create_table_query)
        cursor.commit()
        logging.info(f"Table '{table_name}' is ready for data insertion.")
    except Exception as e:
        logging.error(f"Error creating table '{table_name}': {e}")
        cursor.rollback()
        cursor.close()
        sys.exit(1)

# =======================
# 4. Insert Data into SQL Server
# =======================

def insert_data(cursor, table_name, dataframe):
    # Prepare columns and placeholders
    columns = ", ".join(dataframe.columns)
    placeholders = ", ".join(["?"] * len(dataframe.columns))
    
    insert_query = f"""
    INSERT INTO [{table_name}] ({columns})
    VALUES ({placeholders})
    """
    
    data_tuples = list(dataframe.itertuples(index=False, name=None))
    
    successful_inserts = 0
    failed_inserts = 0
    
    for data in data_tuples:
        try:
            cursor.execute(insert_query, data)
            successful_inserts += 1
        except pyodbc.IntegrityError:
            logging.warning(f"Duplicate entry for ID {data[0]}. Skipping insertion.")
            failed_inserts += 1
        except Exception as e:
            logging.error(f"Error inserting data {data}: {e}")
            failed_inserts += 1
    
    try:
        cursor.commit()
        logging.info(f"Inserted {successful_inserts} rows into '{table_name}'.")
        if failed_inserts > 0:
            logging.info(f"Skipped {failed_inserts} duplicate or erroneous rows.")
    except Exception as e:
        logging.error(f"Error committing data to SQL Server: {e}")
        cursor.rollback()

# =======================
# 5. Main Execution Flow
# =======================

def main():
    table_name = 'mars_rover_photos_raw'
    
    # Fetch data
    photos_df = fetch_nasa_mars_rover_photos(sol=1000)
    
    # Rename columns to use underscores instead of dots
    photos_df.rename(columns={
        'camera.name': 'camera_name',
        'camera.full_name': 'camera_full_name',
        'rover.id': 'rover_id',
        'rover.name': 'rover_name',
        'rover.launch_date': 'rover_launch_date',
        'rover.landing_date': 'rover_landing_date',
        'rover.status': 'rover_status'
    }, inplace=True)
    
    # Connect to SQL Server
    cnxn = connect_to_sql_server()
    cursor = cnxn.cursor()
    
    # Create table
    create_table_if_not_exists(cursor, table_name, photos_df)
    
    # Prepare data for insertion
    required_columns = [
        'id',
        'sol',
        'camera_name',
        'camera_full_name',
        'earth_date',
        'img_src',
        'rover_id',
        'rover_name',
        'rover_launch_date',
        'rover_landing_date',
        'rover_status'
    ]
    
    missing_columns = [col for col in required_columns if col not in photos_df.columns]
    if missing_columns:
        logging.error(f"Missing columns in data: {missing_columns}")
        cursor.close()
        cnxn.close()
        sys.exit(1)
    
    data_to_insert = photos_df[required_columns].copy()
    
    # Convert date columns to datetime objects
    date_columns = ['earth_date', 'rover_launch_date', 'rover_landing_date']
    for col in date_columns:
        if col in data_to_insert.columns:
            data_to_insert[col] = pd.to_datetime(data_to_insert[col], errors='coerce')
    
    # Insert data
    insert_data(cursor, table_name, data_to_insert)
    
    # Close connections
    cursor.close()
    cnxn.close()
    logging.info("SQL Server connection closed.")

# =======================
# Execute the Script
# =======================

if __name__ == "__main__":
    main()
