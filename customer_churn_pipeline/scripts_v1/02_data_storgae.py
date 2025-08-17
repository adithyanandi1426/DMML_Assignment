import pandas as pd
import snowflake.connector
import logging
import os
from datetime import datetime

# Setup logging
log_file = r"C:\Users\adity\Mtech\DMML\DMML_Assignment\customer_churn_pipeline\data\logs\02_raw_data_storage.log"
logging.basicConfig(filename=log_file, level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')
logging.info("Starting raw data extraction script")

# Snowflake connection config
SNOWFLAKE_CONFIG = {
    "user": "admin",
    "password": "Aditya@9705064197",  # Use environment variables in production!
    "account": "jerzgki-ye33911",
    "warehouse": "COMPUTE_WH",
    "database": "CUSTOMER_DB",
    "schema": "TELECOM"
}
logging.info("Snowflake connection config set")

# Current date partition (e.g., 2025-08-16)
partition_date = datetime.now().strftime('%Y-%m-%d')
output_folder = fr"C:\Users\adity\Mtech\DMML\DMML_Assignment\customer_churn_pipeline\data\raw_ingested_data\{partition_date}"
output_file = os.path.join(output_folder, "customer_churn_raw.csv")

try:
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_CONFIG["user"],
        password=SNOWFLAKE_CONFIG["password"],
        account=SNOWFLAKE_CONFIG["account"],
        warehouse=SNOWFLAKE_CONFIG["warehouse"],
        database=SNOWFLAKE_CONFIG["database"],
        schema=SNOWFLAKE_CONFIG["schema"]
    )
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
