import pandas as pd
import os
from datetime import datetime
import logging

# Setup
log_dir = r"C:\Users\adity\Mtech\DMML\DMML_Assignment\customer_churn_pipeline\data\logs"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(filename=os.path.join(log_dir, "06_feature_engineering.log"), level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

# Paths
cleaned_base = r"C:\Users\adity\Mtech\DMML\DMML_Assignment\customer_churn_pipeline\data\processed"
feature_store_dir = r"C:\Users\adity\Mtech\DMML\DMML_Assignment\customer_churn_pipeline\data\feature_store"
metadata_path = os.path.join(feature_store_dir, "metadata", "feature_metadata.csv")
features_dir = os.path.join(feature_store_dir, "features")

# Ensure dirs
os.makedirs(os.path.join(feature_store_dir, "metadata"), exist_ok=True)
os.makedirs(features_dir, exist_ok=True)

# Get latest cleaned partition
partitions = sorted([f for f in os.listdir(cleaned_base) if os.path.isdir(os.path.join(cleaned_base, f))])
if not partitions:
    raise Exception("No processed partitions found")
latest = partitions[-1]

cleaned_file = os.path.join(cleaned_base, latest, "cleaned_customer_churn.csv")
df = pd.read_csv(cleaned_file)

# Create features
df["total_value"] = df["monthly_spend"] * df["tenure_months"]
df["activity_score"] = df["num_logins"] / (df["last_login_days_ago"] + 1)

# Select engineered features + ID
feature_df = df[["customer_id", "total_value", "activity_score"]]
feature_filename = f"{latest}_customer_features.csv"
feature_file_path = os.path.join(features_dir, feature_filename)
feature_df.to_csv(feature_file_path, index=False)

logging.info(f"Saved engineered features to: {feature_file_path}")

# Metadata
metadata = pd.DataFrame([
    {
        "feature_name": "total_value",
        "description": "Total value = monthly_spend * tenure",
        "source_file": feature_filename,
        "version": "v1",
        "created_on": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    },
    {
        "feature_name": "activity_score",
        "description": "Login activity score = logins / (days since last login + 1)",
        "source_file": feature_filename,
        "version": "v1",
        "created_on": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
])

# Append to metadata file
if os.path.exists(metadata_path):
    existing = pd.read_csv(metadata_path)
    metadata = pd.concat([existing, metadata], ignore_index=True)

metadata.to_csv(metadata_path, index=False)
logging.info("Metadata updated.")
print(f"Features and metadata registered in: {feature_store_dir}")
