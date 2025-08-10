import os
import shutil
from datetime import datetime

def store_raw_data(src_file, source_type):
    date_str = datetime.now().strftime("%Y-%m-%d")
    dest_dir = f"data/raw/{source_type}/{date_str}"
    os.makedirs(dest_dir, exist_ok=True)
    shutil.copy(src_file, dest_dir)
    print(f"Stored raw data at: {dest_dir}")

# Example usage
store_raw_data("data/raw/csv_ingest_20250810_101500.csv", "csv")
