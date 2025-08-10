import pandas as pd
import os

def save_to_feature_store(file_path):
    os.makedirs("data/feature_store", exist_ok=True)
    df = pd.read_csv(file_path)
    df.to_csv("data/feature_store/features_v1.csv", index=False)
    print("Features stored.")

# Example usage
save_to_feature_store("data/transformed/transformed_data.csv")
