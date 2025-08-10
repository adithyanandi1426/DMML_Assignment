# scripts/02_data_ingestion.py
import os
import pandas as pd
import requests
from datetime import datetime
import logging

logging.basicConfig(filename="data/logs/ingestion.log", level=logging.INFO)

def ingest_csv(file_path, dest_dir):
    try:
        df = pd.read_csv(file_path)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest_path = os.path.join(dest_dir, f"csv_ingest_{timestamp}.csv")
        df.to_csv(dest_path, index=False)
        logging.info(f"CSV ingestion successful: {dest_path}")
        return dest_path
    except Exception as e:
        logging.error(f"CSV ingestion failed: {e}")

def ingest_api(api_url, dest_dir):
    try:
        response = requests.get(api_url)
        data = response.json()
        df = pd.DataFrame(data)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest_path = os.path.join(dest_dir, f"api_ingest_{timestamp}.csv")
        df.to_csv(dest_path, index=False)
        logging.info(f"API ingestion successful: {dest_path}")
        return dest_path
    except Exception as e:
        logging.error(f"API ingestion failed: {e}")

if __name__ == "__main__":
    os.makedirs("data/raw", exist_ok=True)
    ingest_csv("sample_data/customers.csv", "data/raw")
    ingest_api("https://mocki.io/v1/0e6d8fc6-65f8-4c8c-9c63-d2e8ffb9a67b", "data/raw")
