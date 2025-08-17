import os
import pandas as pd
import numpy as np
import uuid
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
import joblib

# === 1. Data Generation ===
def generate_data(n=500):
    df = pd.DataFrame({
        "customer_id": [str(uuid.uuid4()) for _ in range(n)],
        "age": np.random.randint(18, 70, size=n),
        "gender": np.random.choice(["M", "F"], size=n),
        "tenure_months": np.random.randint(1, 60, size=n),
        "monthly_spend": np.round(np.random.uniform(10, 100, size=n), 2),
        "num_logins": np.random.randint(1, 50, size=n),
        "last_login_days": np.random.randint(1, 400, size=n),
        "support_tickets": np.random.randint(0, 5, size=n),
        "churn": np.random.choice([0, 1], size=n, p=[0.7, 0.3])
    })
    os.makedirs("data/raw", exist_ok=True)
    path = f"data/raw/generated_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(path, index=False)
    print(f"Data generated: {path}")
    return path

# === 2. Data Validation ===
def validate_data(path):
    df = pd.read_csv(path)
    print("\n=== Data Validation ===")
    print("Missing values:\n", df.isnull().sum())
    print("Duplicates:", df.duplicated().sum())
    return path

# === 3. Data Preparation ===
def prepare_data(path):
    df = pd.read_csv(path)
    df = df.drop_duplicates()
    for col in df.select_dtypes(include=[np.number]):
        df[col].fillna(df[col].median(), inplace=True)
    for col in df.select_dtypes(exclude=[np.number]):
        df[col].fillna(df[col].mode()[0], inplace=True)
    os.makedirs("data/processed", exist_ok=True)
    out_path = "data/processed/clean_data.csv"
    df.to_csv(out_path, index=False)
    print(f"Data prepared: {out_path}")
    return out_path

# === 4. Feature Engineering ===
def transform_data(path):
    df = pd.read_csv(path)
    df["tenure_years"] = df["tenure_months"] / 12
    df["activity_rate"] = df["num_logins"] / (df["tenure_months"] + 1)
    df["recent_active"] = (df["last_login_days"] < 30).astype(int)
    os.makedirs("data/transformed", exist_ok=True)
    out_path = "data/transformed/features.csv"
    df.to_csv(out_path, index=False)
    print(f"Data transformed: {out_path}")
    return out_path

# === 5. Model Training ===
def train_model(path):
    df = pd.read_csv(path)
    X = df.drop(columns=["customer_id", "churn"])
    y = df["churn"]
    X = pd.get_dummies(X, drop_first=True)  # simple encoding
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    preds = model.predict(X_test)

    print("\n=== Model Performance ===")
    print(classification_report(y_test, preds))

    os.makedirs("data/models", exist_ok=True)
    model_path = "data/models/churn_model.pkl"
    joblib.dump(model, model_path)
    print(f"Model saved: {model_path}")

# === Run Full Pipeline ===
if __name__ == "__main__":
    raw_file = generate_data(500)
    validate_data(raw_file)
    clean_file = prepare_data(raw_file)
    features_file = transform_data(clean_file)
    train_model(features_file)
