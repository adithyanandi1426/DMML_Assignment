import pandas as pd
import os
import logging
from datetime import datetime

# === Setup Logging ===
log_dir = r"C:\Users\adity\Mtech\DMML\DMML_Assignment\customer_churn_pipeline\data\logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "03_data_validation.log")
logging.basicConfig(filename=log_file, level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')
logging.info("Starting data validation...")

# === Dynamically Find Latest Partition Folder ===
base_data_dir = r"C:\Users\adity\Mtech\DMML\DMML_Assignment\customer_churn_pipeline\data\raw_ingested_data"

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

# === Load Data ===
df = pd.read_csv(latest_data_path)
logging.info(f"Loaded data from {latest_data_path} with {len(df)} rows and {len(df.columns)} columns.")
print(f"Loaded {len(df)} rows from {latest_data_path}")

# === Initialize Report ===
report = []

def add_report(issue, column, details):
    report.append({"Check": issue, "Column": column, "Result": details})

# === 1. Missing Values ===
missing = df.isnull().sum()
missing_reported = False
for col, val in missing.items():
    if val > 0:
        add_report("Missing Values", col, f"{val} missing")
        missing_reported = True
if not missing_reported:
    add_report("Missing Values", "-", "No missing values found")

# === 2. Duplicate Rows ===
dup_count = df.duplicated().sum()
if dup_count > 0:
    add_report("Duplicate Rows", "All", f"{dup_count} duplicate rows")
else:
    add_report("Duplicate Rows", "-", "No duplicates found")

# === 3. Data Type Checks ===
expected_dtypes = {
    "customer_id": "int64",
    "age": "int64",
    "gender": "object",
    "tenure_months": "int64",
    "monthly_spend": "float64",
    "num_logins": "int64",
    "num_transactions": "int64",
    "is_premium_member": "int64",
    "last_login_days_ago": "int64",
    "churn": "int64"
}

dtype_issues = 0
for col, expected_dtype in expected_dtypes.items():
    if col in df.columns:
        actual_dtype = df[col].dtype
        if str(actual_dtype) != expected_dtype:
            add_report("Data Type Mismatch", col, f"Expected {expected_dtype}, got {actual_dtype}")
            dtype_issues += 1
if dtype_issues == 0:
    add_report("Data Type Check", "-", "All types matched expectations")

# === 4. Range Checks ===
range_checks_done = 0

if 'age' in df.columns:
    invalid_ages = df[(df['age'] < 18) | (df['age'] > 100)].shape[0]
    if invalid_ages > 0:
        add_report("Invalid Age", "age", f"{invalid_ages} values out of expected range (18-100)")
    else:
        add_report("Invalid Age", "age", "All ages within range")
    range_checks_done += 1

if 'monthly_spend' in df.columns:
    negative_spends = df[df['monthly_spend'] < 0].shape[0]
    if negative_spends > 0:
        add_report("Negative Spend", "monthly_spend", f"{negative_spends} negative values")
    else:
        add_report("Negative Spend", "monthly_spend", "No negative spends")
    range_checks_done += 1

if range_checks_done == 0:
    add_report("Range Checks", "-", "Columns not found")

# === 5. Categorical Checks ===
if 'gender' in df.columns:
    valid_genders = ['Male', 'Female']
    invalid_genders = df[~df['gender'].isin(valid_genders)].shape[0]
    if invalid_genders > 0:
        add_report("Invalid Gender", "gender", f"{invalid_genders} invalid values")
    else:
        add_report("Gender Check", "gender", "All genders valid")
else:
    add_report("Gender Check", "-", "Column 'gender' not found")

# === 6. Outlier Detection ===
if 'monthly_spend' in df.columns:
    high_spends = df[df['monthly_spend'] > 1000].shape[0]
    if high_spends > 0:
        add_report("Outlier - High Spend", "monthly_spend", f"{high_spends} entries above $1000")
    else:
        add_report("Outlier - High Spend", "monthly_spend", "No high spend outliers")
else:
    add_report("Outlier - High Spend", "-", "Column not found")

# === Save Report ===
report_df = pd.DataFrame(report)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
report_output_dir = r"C:\Users\adity\Mtech\DMML\DMML_Assignment\customer_churn_pipeline\docs"
os.makedirs(report_output_dir, exist_ok=True)
report_path = os.path.join(report_output_dir, f"validation_report_{timestamp}.csv")

report_df.to_csv(report_path, index=False)
logging.info("Data validation completed.")
print(f"Validation report saved to: {report_path}")
