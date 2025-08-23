import pandas as pd
import logging
import os
from dotenv import load_dotenv

load_dotenv()
log_path = os.path.join(os.getenv("LOG_BASE_PATH"), "07_feature_store.log")
feature_store_path = os.getenv("FEATURE_STORE_PATH")

logging.basicConfig(filename=log_path, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

logging.info("Storing features...")

# === Dynamically Find Latest Partition Folder ===
base_data_dir = os.getenv("TRANSFORMED_DATA_PATH_BASE") #base path for transformed data

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

try:
    # Use transformed CSV
    df = pd.read_csv(latest_data_path)

    df.to_csv(feature_store_path, index=False)
    logging.info(f"✅ Features stored to: {feature_store_path}")
except Exception as e:
    logging.error(f"❌ Feature store failed: {e}")
