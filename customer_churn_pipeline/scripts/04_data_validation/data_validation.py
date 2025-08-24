import pandas as pd
import numpy as np
import logging
import os
from dotenv import load_dotenv

# Load env variables
load_dotenv()

# Logging setup
LOG_BASE_PATH = os.getenv("LOG_BASE_PATH")
FILE_NAME = "04_data_validation.log"
LOG_FILE_NAME = os.path.join(LOG_BASE_PATH, FILE_NAME)

logging.basicConfig(
    filename=LOG_FILE_NAME,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logging.info("Starting custom data validation...")

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

# Load dataset
csv_path = latest_data_path
if not os.path.exists(csv_path):
    logging.error(f"CSV path does not exist: {csv_path}")
    raise FileNotFoundError(f"CSV path not found: {csv_path}")

df = pd.read_csv(csv_path)
logging.info("Data loaded from %s", csv_path)

# Create validation summary
report = []

for col in df.columns:
    data = df[col]
    dtype = data.dtype
    missing_pct = round(data.isnull().mean() * 100, 2)
    n_unique = data.nunique()
    is_constant = n_unique == 1
    is_unique = data.is_unique
    most_frequent = data.mode().iloc[0] if not data.mode().empty else None

    # Simple rule-based status
    if missing_pct > 50:
        status = "High Missing"
    elif is_constant:
        status = "Constant Value"
    elif is_unique and not col.lower().startswith("id"):
        status = "Unexpected Unique"
    else:
        status = "OK"

    report.append({
        "Column": col,
        "Data Type": str(dtype),
        "Missing (%)": missing_pct,
        "Unique Values": n_unique,
        "Is Constant": is_constant,
        "Is Unique": is_unique,
        "Most Frequent Value": most_frequent,
        "Check Status": status
    })

# Convert to DataFrame
report_df = pd.DataFrame(report)

# Save report CSV
report_path = os.path.join(os.getenv('DATA_VALIDATION_REPORT_PATH'), latest_partition, "data_validation_report.csv")
os.makedirs(os.path.dirname(report_path), exist_ok=True)
report_df.to_csv(report_path, index=False)
logging.info("Data quality report saved to: %s", report_path)

logging.info("Data validation completed.")
