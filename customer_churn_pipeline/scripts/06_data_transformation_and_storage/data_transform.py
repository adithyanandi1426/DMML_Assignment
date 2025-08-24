import pandas as pd
import logging
import os
from dotenv import load_dotenv
from sklearn.preprocessing import StandardScaler
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Load env
load_dotenv()
log_path = os.path.join(os.getenv("LOG_BASE_PATH"), "06_data_transformation.log")


logging.basicConfig(filename=log_path, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# === Dynamically Find Latest Partition Folder ===
base_data_dir = os.getenv("PROCESSED_DATA_PATH_BASE") #base path for processed data

# Get all date folders
partition_folders = [
    f for f in os.listdir(base_data_dir)
    if os.path.isdir(os.path.join(base_data_dir, f)) and f[:4].isdigit()
]

if not partition_folders:
    logging.error("No partitioned date folders found in raw_ingested_data.")
    raise Exception("No partition folders found.")

# Sort and get the latest one
latest_partition = sorted(partition_folders)[-1]
latest_data_path = os.path.join(base_data_dir, latest_partition, "customer_churn_cleaned.csv")

if not os.path.exists(latest_data_path):
    logging.error(f"No CSV file found at expected location: {latest_data_path}")
    raise FileNotFoundError(f"CSV not found: {latest_data_path}")

logging.info("Starting data transformation...")

try:
    df = pd.read_csv(latest_data_path)
    
    # Drop leakage columns (future info)
    leakage_cols = [
        'Customer Status', 'CLTV', 'Total Revenue', 'Total Charges',
        'Churn Category', 'Churn Reason', 'Churn Score', 'Lat Long', 'Customer ID'
    ]
    df.drop(columns=leakage_cols, inplace=True, errors='ignore')
    logging.info(f"Data transformed. New shape: {df.shape}")

    transformed_path = os.getenv("TRANSFORMED_DATA_PATH_BASE")
    output_partition = os.path.join(transformed_path, latest_partition)
    os.makedirs(output_partition, exist_ok=True)
    output_file = os.path.join(output_partition, "customer_churn_transformed.csv")
    df.to_csv(output_file, index=False)
    logging.info(f"Transformed data saved to: {output_file}")
    logging.info("Transformation complete and saved.")
except Exception as e:
    logging.error(f"Transformation failed: {e}")


# === Upload to Snowflake ===
try:
    conn = snowflake.connector.connect(
        user="admin",
        password="Aditya@9705064197",  # Use secrets manager in production
        account="jerzgki-ye33911",
        warehouse="COMPUTE_WH",
        database="CUSTOMER_DB",
        schema="TELECOM"
    )
    logging.info("Connected to Snowflake")

    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name="customer_churn_transformed",
        auto_create_table=True,
        overwrite=True
    )

    if success:
        print(f"Uploaded {nrows} rows to 'customer_churn_transformed' in Snowflake.")
        logging.info(f"Uploaded {nrows} rows to 'customer_churn_transformed' in Snowflake.")
    else:
        print("Upload failed.")
        logging.error("Upload to Snowflake failed.")

    conn.close()
    logging.info("Closed Snowflake connection.")

except Exception as e:
    logging.error(f"Snowflake upload error: {e}")
    print(f"Error uploading to Snowflake: {e}")
