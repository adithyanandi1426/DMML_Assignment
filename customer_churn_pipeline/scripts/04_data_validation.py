import pandas as pd
import json
import os

def validate_data(file_path):
    df = pd.read_csv(file_path)
    report = {
        "missing_values": df.isnull().sum().to_dict(),
        "data_types": df.dtypes.astype(str).to_dict(),
        "duplicates": df.duplicated().sum(),
        "summary_stats": df.describe().to_dict()
    }
    os.makedirs("data/logs", exist_ok=True)
    with open("data/logs/validation_report.json", "w") as f:
        json.dump(report, f, indent=4)
    print(f"Validation report saved for {file_path}")

# Example usage
validate_data("data/raw/csv_ingest_20250810_101500.csv")
