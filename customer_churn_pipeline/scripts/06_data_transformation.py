import pandas as pd
import os
from sklearn.preprocessing import StandardScaler

def transform_data(file_path):
    df = pd.read_csv(file_path)
    df["total_spend_per_month"] = df["spend"] / df["tenure"]
    scaler = StandardScaler()
    df[["age", "spend"]] = scaler.fit_transform(df[["age", "spend"]])
    os.makedirs("data/transformed", exist_ok=True)
    df.to_csv("data/transformed/transformed_data.csv", index=False)
    print("Data transformed and saved.")

# Example usage
transform_data("data/processed/clean_data.csv")
