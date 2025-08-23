import os
import subprocess
import logging
from datetime import datetime
from prefect import flow, get_run_logger, task

# === Logging Setup ===
log_dir = r"C:\Users\adity\Mtech\DMML\DMML_Assignment\customer_churn_pipeline\data\logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "10_orchestration.log")
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logging.info("Started Prefect orchestration for customer churn pipeline")

PYTHON_EXECUTABLE = r"C:\Users\adity\Mtech\DMML\DMML_Assignment\dmml_env\Scripts\python.exe"

# === Prefect Task ===
@task
def run_script(script_path: str):
    logger = get_run_logger()
    logging.info(f"Starting {script_path}")

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

# === Main Flow ===
@flow(name="Churn_Pipeline_Orchestration")
def churn_pipeline_flow():
    base_path = r"C:\Users\adity\Mtech\DMML\DMML_Assignment\customer_churn_pipeline\scripts_v2"

    script_paths = [
        os.path.join(base_path, "01_data_fetching", "data_fetch.py"),
        os.path.join(base_path, "02_data_ingestion", "data_ingest.py"),
        os.path.join(base_path, "03_raw_data_storage", "data_storage.py"),
        os.path.join(base_path, "04_data_validation", "data_validation.py"),
        os.path.join(base_path, "05_data_preparation", "data_preparation.py"),
        os.path.join(base_path, "06_data_transformation_and_storage", "data_transform.py"),
        # os.path.join(base_path, "07_feature_store", "07_feature_store.py"),
        # os.path.join(base_path, "08_data_versioning", "08_data_versioning.py"),
        os.path.join(base_path, "08_model_building", "model_building.py") # This seems to be named differently
    ]

    for script in script_paths:
        run_script(script)

if __name__ == "__main__":
    churn_pipeline_flow()
    logging.shutdown()