import subprocess

pipeline_steps = [
    "scripts/02_data_ingestion.py",
    "scripts/03_raw_data_storage.py",
    "scripts/04_data_validation.py",
    "scripts/05_data_preparation.py",
    "scripts/06_data_transformation.py",
    "scripts/07_feature_store.py",
    "scripts/08_data_versioning.py",
    "scripts/09_model_building.py"
]

for step in pipeline_steps:
    print(f"Running {step}...")
    subprocess.run(["python", step])
