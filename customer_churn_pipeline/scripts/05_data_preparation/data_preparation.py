import pandas as pd
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
log_path = os.path.join(os.getenv("LOG_BASE_PATH"), "05_data_preparation.log")

logging.basicConfig(filename=log_path, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

logging.info("Starting data preparation...")

# === Dynamically Find Latest Partition Folder ===
base_data_dir = os.getenv("OUTPUT_FOLDER_BASE", r"C:\Users\adity\Mtech\DMML\DMML_Assignment\customer_churn_pipeline\data\raw_ingested_data")

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
latest_data_path = os.path.join(base_data_dir, latest_partition, "customer_churn_raw.csv")

if not os.path.exists(latest_data_path):
    logging.error(f"No CSV file found at expected location: {latest_data_path}")
    raise FileNotFoundError(f"CSV not found: {latest_data_path}")

try:
    df = pd.read_csv(latest_data_path)
    logging.info(f"Loaded dataset with {df.shape[0]} rows and {df.shape[1]} columns")
    logging.info("Cleaning data...")
    df.drop_duplicates(inplace=True)
    logging.info(f"Duplicates removed if any. New shape: {df.shape}")
    logging.info(f"Data cleaned. New shape: {df.shape}")
    processed_path = os.getenv("PROCESSED_DATA_PATH_BASE")
    output_partition = os.path.join(processed_path, latest_partition)
    os.makedirs(output_partition, exist_ok=True)
    output_file = os.path.join(output_partition, "customer_churn_cleaned.csv")
    df.to_csv(output_file, index=False)
    logging.info(f"Cleaned data saved to: {output_file}")
except Exception as e:
    logging.error(f"Error in data preparation: {e}")
