import pandas as pd
import os
import logging
from datetime import datetime
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# === Setup Logging ===
log_dir = r"C:\Users\adity\Mtech\DMML\DMML_Assignment\customer_churn_pipeline\data\logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "05_data_transformation.log")
logging.basicConfig(filename=log_file, level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')
logging.info("Starting data transformation...")

# === Locate Latest Cleaned File ===
processed_base_path = r"C:\Users\adity\Mtech\DMML\DMML_Assignment\customer_churn_pipeline\data\processed"
partition_folders = [f for f in os.listdir(processed_base_path)
                     if os.path.isdir(os.path.join(processed_base_path, f)) and f[:4].isdigit()]

if not partition_folders:
    raise Exception("No partitioned folders found in processed path.")

latest_partition = sorted(partition_folders)[-1]
cleaned_data_path = os.path.join(processed_base_path, latest_partition, "cleaned_customer_churn.csv")

if not os.path.exists(cleaned_data_path):
    raise FileNotFoundError(f"Cleaned data not found at: {cleaned_data_path}")

df = pd.read_csv(cleaned_data_path)
logging.info(f"Loaded cleaned data from {cleaned_data_path} with shape: {df.shape}")
print(f"Loaded cleaned data from: {cleaned_data_path} - Shape: {df.shape}")

# === Feature Engineering ===
if "tenure_months" in df.columns:
    df["tenure_years"] = df["tenure_months"] / 12

if "num_logins" in df.columns and "num_transactions" in df.columns:
    df["activity_score"] = df["num_logins"] * 0.5 + df["num_transactions"] * 0.5

if "monthly_spend" in df.columns and "num_logins" in df.columns:
    df["avg_spend_per_login"] = df["monthly_spend"] / df["num_logins"].replace(0, 1)

logging.info("Engineered features: tenure_years, activity_score, avg_spend_per_login")

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
