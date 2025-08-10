import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import joblib
import os

def train_model(file_path):
    df = pd.read_csv(file_path)
    X = df.drop(columns=["churn"])
    y = df["churn"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    model = RandomForestClassifier(random_state=42)
    model.fit(X_train, y_train)
    
    y_pred = model.predict(X_test)
    metrics = {
        "accuracy": accuracy_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred),
        "recall": recall_score(y_test, y_pred),
        "f1_score": f1_score(y_test, y_pred)
    }
    os.makedirs("models", exist_ok=True)
    joblib.dump(model, "models/churn_model.pkl")
    print(metrics)
    return metrics

# Example usage
train_model("data/feature_store/features_v1.csv")
