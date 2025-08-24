import pandas as pd
import numpy as np
import logging
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
LOG_BASE_PATH = os.getenv("LOG_BASE_PATH")
FILE_NAME = "02_ingestion.log"
LOG_FILE_NAME = os.path.join(LOG_BASE_PATH, FILE_NAME)
logging.basicConfig(
    filename=LOG_FILE_NAME,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.info("Starting data ingestion process...Into snowflake")
data = pd.read_csv(os.getenv("CSV_DATA_FROM_API"))
logging.info("customer churn data read from csv successfully.")

logging.info(f"Data shape: {data.shape}")
logging.info(f"Data columns: {data.columns.tolist()}") 
logging.info(f"establishing connection to snowflake")
try:
    conn = snowflake.connector.connect(
        user=os.getenv("user"),
        password=os.getenv("password"),
        account=os.getenv("account"),
        warehouse=os.getenv("warehouse"),
        database=os.getenv("database"),
        schema=os.getenv("schema"))
    logging.info("Connection to Snowflake established successfully.")
except Exception as e:  
    logging.error(f"Error connecting to Snowflake: {e}")
    raise

logging.info("Uploading data to Snowflake...")
# 3. Upload dataframe using write_pandas (best method!)
success, nchunks, nrows, _ = write_pandas(
    conn=conn,
    df=data,
    table_name="customer_churn",
    auto_create_table=True,    # Will create table if it doesn't exist
    overwrite=True             # Replace existing data
)

# 4. Result
if success:
    logging.info(f"Uploaded {nrows} rows to 'customer_churn' in Snowflake.")
else:
    print("Upload failed.")
    logging.error("Upload to Snowflake failed.")

# 5. Close connection
conn.close()
logging.info("Connection to Snowflake closed.")
logging.info("customer churn data ingested to snowflake successfully.")