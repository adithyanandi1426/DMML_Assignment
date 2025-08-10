import os
import shutil
from datetime import datetime

def version_data(file_path, version_dir="data_versions"):
    os.makedirs(version_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    shutil.copy(file_path, os.path.join(version_dir, f"{os.path.basename(file_path)}_{timestamp}"))
    print("Data versioned.")

# Example usage
version_data("data/transformed/transformed_data.csv")
