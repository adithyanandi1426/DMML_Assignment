import os
import logging
from datetime import datetime
import pandas as pd
import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import snowflake.connector

# === Logging Setup ===
log_dir = r"C:\Users\adity\Mtech\DMML\DMML_Assignment\customer_churn_pipeline\data\logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "07_model_training_mlflow.log")
logging.basicConfig(filename=log_file, level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')
logging.info("Started model training with MLflow")

# === Get today's date ===
run_date = datetime.now().strftime('%Y%m%d')

# === Connect to Snowflake and Fetch Data ===
try:
    conn = snowflake.connector.connect(
        user="admin",
        password="Aditya@9705064197",  # Use secrets manager in production
        account="jerzgki-ye33911",
        warehouse="COMPUTE_WH",
        database="CUSTOMER_DB",
        schema="TELECOM"
    )
    logging.info("Connected to Snowflake")
    query = '''SELECT * FROM TELECOM."customer_churn_transformed";'''
    logging.info("Executing query to fetch transformed customer churn data")
    df = pd.read_sql(query, conn)
    conn.close()
    logging.info(f"Fetched {df.shape[0]} rows from Snowflake")
except Exception as e:
    logging.error(f"Snowflake fetch failed: {e}")
    raise

# === Prepare Features and Target ===
if 'churn' not in df.columns:
    raise ValueError("'churn' column missing!")

# Ensure churn is binary and integer
y = df['churn'].round().astype(int)  # Fix: from float to int

X = df.drop(columns=['churn'])
print("Unique labels in target:", y.unique())


X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, stratify=y, random_state=42
)

# === Model Config ===
models = {
    "LogisticRegression": LogisticRegression(max_iter=1000),
    "RandomForest": RandomForestClassifier(n_estimators=100)
}

# === Start MLflow Experiment ===
mlflow.set_experiment("Customer_Churn_Prediction")

for model_name, model in models.items():
    with mlflow.start_run(run_name=f"{run_date}_{model_name}"):

        # Train model
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        # Evaluation
        acc = accuracy_score(y_test, y_pred)
        pre = precision_score(y_test, y_pred)
        rec = recall_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred)

        # Log metrics
        mlflow.log_metric("accuracy", acc)
        mlflow.log_metric("precision", pre)
        mlflow.log_metric("recall", rec)
        mlflow.log_metric("f1_score", f1)

        # Log parameters
        mlflow.log_param("model_type", model_name)

        # Log model
        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="model",
            registered_model_name=f"{run_date}_{model_name}"
        )

        logging.info(f"{model_name}: Acc={acc:.4f}, Prec={pre:.4f}, Rec={rec:.4f}, F1={f1:.4f}")
        print(f"{model_name} logged to MLflow with F1 Score: {f1:.4f}")

