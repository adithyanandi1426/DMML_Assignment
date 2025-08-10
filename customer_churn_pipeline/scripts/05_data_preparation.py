import pandas as pd
import os

def prepare_data(file_path):
    df = pd.read_csv(file_path)
    df.fillna(df.mean(numeric_only=True), inplace=True)
    df.drop_duplicates(inplace=True)
    os.makedirs("data/processed", exist_ok=True)
    df.to_csv("data/processed/clean_data.csv", index=False)
    print("Data prepared and saved to processed folder.")

# Example usage
prepare_data("data/raw/csv_ingest_20250810_101500.csv")
