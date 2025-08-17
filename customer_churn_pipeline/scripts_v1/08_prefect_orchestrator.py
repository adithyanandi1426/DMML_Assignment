import os
import subprocess
import logging
from datetime import datetime
from prefect import flow, get_run_logger, task

# === Setup Logging ===
log_dir = r"C:\Users\adity\Mtech\DMML\DMML_Assignment\customer_churn_pipeline\data\logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "08_orchestration.log")
logging.basicConfig(filename=log_file, level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

logging.info("Started Prefect orchestration for customer churn pipeline")
PYTHON_EXECUTABLE = r"C:\Users\adity\Mtech\DMML\DMML_Assignment\dmml_env\Scripts\python.exe"
# === Prefect Task to Run Scripts ===
@task
def run_script(script_name: str):
    logger = get_run_logger()
    base_path = r"C:\Users\adity\Mtech\DMML\DMML_Assignment\customer_churn_pipeline\scripts_v1"
    full_path = os.path.join(base_path, script_name)

    logger.info(f"Starting {script_name}")
    logging.info(f"Starting {script_name}")  # File log

    try:
        result = subprocess.run([PYTHON_EXECUTABLE, full_path], capture_output=True, text=True)
        if result.returncode == 0:
            logger.info(f"{script_name} completed successfully.")
            logging.info(f"{script_name} completed successfully.")
        else:
            logger.error(f"{script_name} failed:\n{result.stderr}")
            logging.error(f"{script_name} failed:\n{result.stderr}")
            raise subprocess.CalledProcessError(result.returncode, result.args, result.stdout, result.stderr)
    except Exception as e:
        logger.exception(f"Error running {script_name}: {e}")
        logging.exception(f"Error running {script_name}: {e}")
        raise

# === Main Prefect Flow ===
@flow(name="Churn_Pipeline_Orchestration")
def churn_pipeline_flow():
    script_order = [
        "01_data_ingestion.py",
        "02_data_storgae.py",
        "03_data_validation.py",
        "04_data_preparation.py",
        "05_data_transformation.py",
        "06_feature_store.py",
        "07_Model_tranining.py"
    ]

    for script in script_order:
        run_script(script)

if __name__ == "__main__":
    churn_pipeline_flow()
