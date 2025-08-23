import pandas as pd
import os
import json
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
latest_data_path = os.path.join(base_data_dir, latest_partition, "customer_churn_transformed.csv")

if not os.path.exists(latest_data_path):
    logging.error(f"No CSV file found at expected location: {latest_data_path}")
    raise FileNotFoundError(f"CSV not found: {latest_data_path}")



# === File Paths ===
feature_store_path = r"C:\Users\adity\Mtech\DMML\DMML_Assignment\customer_churn_pipeline\data\processed\feature_store.csv"
meta_output_dir = r"C:\Users\adity\Mtech\DMML\DMML_Assignment\customer_churn_pipeline\data\feature_metadata"
os.makedirs(meta_output_dir, exist_ok=True)

feature_with_date_csv = f"{latest_partition}_customer_churn_feature.csv"
feature_with_date_json = f"{latest_partition}_customer_churn_feature.json"
json_output_path = os.path.join(meta_output_dir, feature_with_date_json)
csv_output_path = os.path.join(meta_output_dir, feature_with_date_csv)

# === Descriptions Dictionary (add as needed) ===
feature_descriptions = {
    "Age": "The customer's age in years.",
    "Avg Monthly GB Download": "Average monthly data download in GB.",
    "Avg Monthly Long Distance Charges": "Average monthly long distance call charges.",
    "Churn": "1 if the customer churned, 0 otherwise.",
    "Churn Category": "High-level category for customer's reason for churning.",
    "Churn Reason": "Specific reason why the customer churned.",
    "Churn Score": "Score between 0-100 indicating churn risk.",
    "City": "Customer's city of residence.",
    "CLTV": "Customer Lifetime Value — predicted revenue over customer's lifetime.",
    "Contract": "Contract type (Month-to-Month, One year, Two year).",
    "Country": "Country where the customer resides.",
    "Customer ID": "Unique identifier for each customer.",
    "Customer Status": "Current status (Stayed, Churned, Joined).",
    "Dependents": "1 if the customer has dependents, 0 otherwise.",
    "Device Protection Plan": "1 if the customer has a device protection plan.",
    "Gender": "The customer's gender.",
    "Internet Service": "1 if the customer has internet service, 0 otherwise.",
    "Internet Type": "Type of internet connection (e.g., DSL, Fiber, Cable).",
    "Lat Long": "Combined latitude and longitude of customer's residence.",
    "Latitude": "Geographical latitude of the customer.",
    "Longitude": "Geographical longitude of the customer.",
    "Married": "1 if married, 0 otherwise.",
    "Monthly Charge": "Monthly bill for the customer.",
    "Multiple Lines": "1 if the customer has multiple phone lines.",
    "Number of Dependents": "Total number of dependents for the customer.",
    "Number of Referrals": "Number of people the customer referred.",
    "Offer": "Last marketing offer accepted by the customer.",
    "Online Backup": "1 if the customer has online backup service.",
    "Online Security": "1 if the customer has online security service.",
    "Paperless Billing": "1 if the customer uses paperless billing.",
    "Partner": "1 if the customer has a partner.",
    "Payment Method": "Customer’s chosen method of payment.",
    "Phone Service": "1 if the customer has phone service.",
    "Population": "Estimated population in the customer's ZIP code.",
    "Premium Tech Support": "1 if the customer has premium tech support.",
    "Quarter": "Fiscal quarter (e.g., Q1, Q2, etc.).",
    "Referred a Friend": "1 if the customer referred a friend.",
    "Satisfaction Score": "Customer satisfaction rating from 1 to 5.",
    "Senior Citizen": "1 if customer is aged 65 or older.",
    "State": "State where the customer lives.",
    "Streaming Movies": "1 if the customer subscribes to movie streaming.",
    "Streaming Music": "1 if the customer subscribes to music streaming.",
    "Streaming TV": "1 if the customer subscribes to TV streaming.",
    "Tenure in Months": "Number of months the customer has stayed.",
    "Total Charges": "Total amount charged to the customer.",
    "Total Extra Data Charges": "Total charges incurred for extra data.",
    "Total Long Distance Charges": "Total long distance call charges.",
    "Total Refunds": "Total amount refunded to the customer.",
    "Total Revenue": "Total revenue generated from the customer.",
    "Under 30": "1 if the customer is under 30 years old.",
    "Unlimited Data": "1 if the customer has unlimited data plan.",
    "Zip Code": "ZIP code of the customer’s residence."
}


# === Load Feature Store ===
print("Loading feature store...")
df = pd.read_csv(latest_data_path)

# === Extract Metadata ===
print("Extracting feature metadata...")
meta_store = []

for col in df.columns:
    metadata = {
        "feature_name": col,
        "data_type": str(df[col].dtype),
        "num_unique_values": int(df[col].nunique()),
        "num_missing_values": int(df[col].isnull().sum()),
        "percentage_missing": float((df[col].isnull().mean()) * 100),
        "description": feature_descriptions.get(col, "No description available.")
    }
    meta_store.append(metadata)

# === Save Metadata as JSON ===
print("Saving feature metadata as JSON...")
with open(json_output_path, "w") as f:
    json.dump(meta_store, f, indent=4)

# === Save Metadata as CSV ===
print("Saving feature metadata as CSV...")
logging.info(f"Saving feature metadata to: {csv_output_path}")
pd.DataFrame(meta_store).to_csv(csv_output_path, index=False)

print("Feature metadata successfully stored.")
print(f"JSON file saved to: {json_output_path}")
print(f"CSV file saved to: {csv_output_path}")
logging.info(f"Features stored to: {feature_store_path}")

