import pandas as pd
import snowflake.connector
import logging
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
LOG_BASE_PATH = os.getenv("LOG_BASE_PATH")
FILE_NAME = "03_raw_data_storage.log"
LOG_FILE_NAME = os.path.join(LOG_BASE_PATH, FILE_NAME)
logging.basicConfig(
    filename=LOG_FILE_NAME,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.info("Starting raw data storage script")
logging.info("Environment variables loaded")
logging.info(f"Log file path: {LOG_FILE_NAME}")

# Snowflake connection config
logging.info("Setting up Snowflake connection config")
SNOWFLAKE_CONFIG = {
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "account": os.getenv("account"),
    "warehouse": os.getenv("warehouse"),
    "database": os.getenv("database"),
    "schema":os.getenv("schema")
}
logging.info("Snowflake connection config set")

# Current date partition (e.g., 2025-08-16)
logging.info("Setting up date partition and output paths")
partition_date = datetime.now().strftime('%Y-%m-%d')
output_folder_base = os.getenv("OUTPUT_FOLDER_BASE")
logging.info(f"Output folder base: {output_folder_base}")
output_folder = os.path.join(output_folder_base, partition_date)
output_file = os.path.join(output_folder, "customer_churn_raw.csv")
logging.info(f"Output folder: {output_folder}")
logging.info(f"Output file: {output_file}") 
logging.info("Date partition and output paths set")

try:
    # Connect to Snowflake
    logging.info("Connecting to Snowflake") 
    conn = snowflake.connector.connect(
          user=os.getenv("user"),
        password=os.getenv("password"),
        account=os.getenv("account"),
        warehouse=os.getenv("warehouse"),
        database=os.getenv("database"),
        schema=os.getenv("schema"))
    
    logging.info("Connected to Snowflake")
except Exception as e:
    logging.error(f"Failed to connect to Snowflake: {e}")
    raise

try:
    # Run query
    query = '''SELECT * FROM TELECOM."customer_churn";'''
    snowflake_df = pd.read_sql(query, conn)
    logging.info("Data read from Snowflake successfully")

    # Ensure partition folder exists
    os.makedirs(output_folder, exist_ok=True)

    # Save as CSV in partitioned folder
    snowflake_df.to_csv(output_file, index=False)
    logging.info(f"Data saved to partitioned folder: {output_file}")
    logging.info(f"Read {len(snowflake_df)} rows from Snowflake")

    conn.close()
    logging.info("Snowflake connection closed after successful data extraction")

except Exception as e:
    logging.error(f"Failed to read data from Snowflake: {e}")
    conn.close()
    logging.info("Snowflake connection closed due to error")
    raise

logging.info("Script completed successfully")
