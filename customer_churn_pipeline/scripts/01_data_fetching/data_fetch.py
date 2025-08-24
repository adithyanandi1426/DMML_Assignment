import requests
import pandas as pd
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
LOG_BASE_PATH = os.getenv("LOG_BASE_PATH")
FILE_NAME = "01_fetching.log"
LOG_FILE_NAME = os.path.join(LOG_BASE_PATH, FILE_NAME)
logging.basicConfig(
    filename=LOG_FILE_NAME,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Configuration from .env
DATASET_NAME = os.getenv("DATASET_NAME")
CONFIG = os.getenv("CONFIG", "default")
SPLIT = os.getenv("SPLIT", "train")
BASE_URL = os.getenv("BASE_URL", "https://datasets-server.huggingface.co/rows")

# Other constants
BATCH_SIZE = 100
OUTPUT_CSV = os.getenv("CSV_DATA_FROM_API")

# Start ingestion
logging.info("Starting download from Hugging Face for dataset: %s", DATASET_NAME)
all_records = []
offset = 0

while True:
    params = {
        "dataset": DATASET_NAME,
        "config": CONFIG,
        "split": SPLIT,
        "offset": offset,
        "length": BATCH_SIZE
    }

    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error("HTTP error at offset %d: %s", offset, str(e))
        break

    data = response.json()
    rows = data.get("rows", [])

    if not rows:
        logging.info("No more rows to fetch.")
        break

    batch = [row["row"] for row in rows]
    all_records.extend(batch)

    logging.info("Fetched %d rows (Total so far: %d)", len(batch), len(all_records))

    offset += BATCH_SIZE

# Convert to DataFrame
df = pd.DataFrame(all_records)

# Save to CSV
df.to_csv(OUTPUT_CSV, index=False)
logging.info("Saved %d rows to '%s'", len(df), OUTPUT_CSV)
