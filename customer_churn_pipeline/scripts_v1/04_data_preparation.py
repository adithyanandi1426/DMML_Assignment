import pandas as pd
import os
import logging
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler, LabelEncoder

# === Setup ===
log_dir = r"C:\Users\adity\Mtech\DMML\DMML_Assignment\customer_churn_pipeline\data\logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "04_data_preparation.log")
logging.basicConfig(filename=log_file, level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')
logging.info("Starting data preparation...")

# === Load latest partitioned data from raw_ingested_data ===
base_raw_path = r"C:\Users\adity\Mtech\DMML\DMML_Assignment\customer_churn_pipeline\data\raw_ingested_data"

partition_folders = [f for f in os.listdir(base_raw_path)
                     if os.path.isdir(os.path.join(base_raw_path, f)) and f[:4].isdigit()]

if not partition_folders:
    raise Exception("No date-partitioned folders found.")

latest_partition = sorted(partition_folders)[-1]
input_path = os.path.join(base_raw_path, latest_partition, "customer_churn_raw.csv")

df = pd.read_csv(input_path)
logging.info(f"Loaded data from {input_path} with shape: {df.shape}")
print(f"Loaded raw data from: {input_path} - Shape: {df.shape}")

# === Backup original before changes ===
df_original = df.copy()

# === 1. Handle Missing Values ===
if 'churn' not in df.columns:
    raise Exception("Target column 'churn' is missing from dataset")

# Drop rows with missing churn
df = df.dropna(subset=['churn'])

# Clean target values
if df['churn'].dtype == 'object':
    try:
        # Try to convert strings like "0", "1"
        df['churn'] = df['churn'].astype(int)
    except:
        # If values are 'Yes'/'No', map them
        df['churn'] = df['churn'].map({'No': 0, 'Yes': 1})

# Round floats, ensure int
df['churn'] = df['churn'].round().astype(int)

# Log unique target values
unique_churn = df['churn'].unique()
logging.info(f"Unique values in 'churn': {unique_churn}")
print(f"Unique values in churn column: {unique_churn}")

# === 2. Handle missing values ===
# Numeric
numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns.drop('churn')
df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())

# Categorical
categorical_cols = df.select_dtypes(include=['object']).columns
for col in categorical_cols:
    df[col] = df[col].fillna(df[col].mode()[0])

# === 3. Encode categorical features ===
le = LabelEncoder()
for col in categorical_cols:
    df[col] = le.fit_transform(df[col])

logging.info("Handled missing values and encoded categorical columns")

# === 4. Standardize numeric features ===
scaler = StandardScaler()
df[numeric_cols] = scaler.fit_transform(df[numeric_cols])
logging.info("Standardized numeric columns")

# === 5. Save Cleaned Data ===
processed_path = r"C:\Users\adity\Mtech\DMML\DMML_Assignment\customer_churn_pipeline\data\processed"
output_partition = os.path.join(processed_path, latest_partition)
os.makedirs(output_partition, exist_ok=True)

cleaned_file = os.path.join(output_partition, "cleaned_customer_churn.csv")
df.to_csv(cleaned_file, index=False)
logging.info(f"Saved cleaned data to {cleaned_file}")
print(f"Cleaned data saved to: {cleaned_file}")

# === 6. Generate EDA Visualizations ===
eda_output_dir = os.path.join(output_partition, "eda")
os.makedirs(eda_output_dir, exist_ok=True)

# Histograms
for col in numeric_cols:
    plt.figure(figsize=(6, 4))
    sns.histplot(df[col], kde=True)
    plt.title(f'Distribution of {col}')
    plt.savefig(os.path.join(eda_output_dir, f"{col}_hist.png"))
    plt.close()

# Boxplots
for col in numeric_cols:
    plt.figure(figsize=(6, 4))
    sns.boxplot(x=df[col])
    plt.title(f'Boxplot of {col}')
    plt.savefig(os.path.join(eda_output_dir, f"{col}_boxplot.png"))
    plt.close()

# Summary statistics
summary_stats_file = os.path.join(output_partition, "summary_statistics.csv")
df.describe().transpose().to_csv(summary_stats_file)

logging.info("Generated EDA reports (histograms, boxplots, statistics)")
print("EDA visualizations and summary stats saved.")
# === 7. Log Completion ===
logging.info("Data preparation completed successfully.")