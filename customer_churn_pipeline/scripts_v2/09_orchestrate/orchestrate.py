import os
import subprocess
import logging
from datetime import datetime
from prefect import flow, get_run_logger, task
from dotenv import load_dotenv

load_dotenv()
 
# === Logging Setup ===
log_dir = os.getenv("LOG_DIR")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "09_orchestration.log")
 
# Create handlers
file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
console_handler = logging.StreamHandler()
 
formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)
 
# Attach to root logger (works even with Prefect)
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)
logging.info("Orchestration script started.")
 
PYTHON_EXECUTABLE = os.getenv("PYTHON_EXECUTABLE")
 
@task(retries=2, retry_delay_seconds=30, name="{script_name}")
def run_script(script_path: str, script_name: str) -> str:
    logger = get_run_logger()
    logging.info(f"Starting {script_name} ({script_path})")
    try:
        result = subprocess.run([PYTHON_EXECUTABLE, script_path], capture_output=True, text=True,encoding="utf-8",
        errors="replace")
        if result.returncode == 0:
            logger.info(f"{script_path} completed successfully.")
            logging.info(f"{script_path} completed successfully.")
        else:
            logger.error(f"{script_path} failed:\n{result.stderr}")
            logging.error(f"{script_path} failed:\n{result.stderr}")
            raise subprocess.CalledProcessError(result.returncode, result.args, result.stdout, result.stderr)
    except Exception as e:
        logger.exception(f"Error running {script_path}: {e}")
        logging.exception(f"Error running {script_path}: {e}")
        raise
 
   
    return script_name
 
 
@flow(name="Churn_Pipeline_Orchestration")
def churn_pipeline_flow():
    base_path = os.getenv("BASE_PATH")  
 
    scripts = [
        ("Data Fetching", os.path.join(base_path, "01_data_fetching", "data_fetch.py")),
        ("Data Ingestion", os.path.join(base_path, "02_data_ingestion", "data_ingest.py")),
        ("Raw Data Storage", os.path.join(base_path, "03_raw_data_storage", "data_storage.py")),
        ("Data Validation", os.path.join(base_path, "04_data_validation", "data_validation.py")),
        ("Data Preparation", os.path.join(base_path, "05_data_preparation", "data_preparation.py")),
        ("Data Transformation", os.path.join(base_path, "06_data_transformation_and_storage", "data_transform.py")),
        ("Feature Store", os.path.join(base_path, "07_feature_store", "feature_store.py")),
        ("Model Building", os.path.join(base_path, "08_model_building", "model_building.py")),
    ]
 
    for script_name, script_path in scripts:
        run_script.with_options(name=script_name)(script_path, script_name=script_name)
 
 
if __name__ == "__main__":
    churn_pipeline_flow()