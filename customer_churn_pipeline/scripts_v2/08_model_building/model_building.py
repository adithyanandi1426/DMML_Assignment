import pandas as pd
import joblib
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
import mlflow
import mlflow.sklearn
import mlflow.models.signature as signature
import logging
import os
from datetime import datetime
from dotenv import load_dotenv
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Load env
load_dotenv()
log_path = os.path.join(os.getenv("LOG_BASE_PATH"), "08_model_training.log")

# === Logging Setup ===
logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.info("Started model training for Logistic Regression and Random Forest")

# === Dynamically Find Latest Partition Folder ===
base_data_dir = os.getenv("TRANSFORMED_DATA_PATH_BASE") #base path for processed data

# Get all date folders
partition_folders = [
    f for f in os.listdir(base_data_dir)
    if os.path.isdir(os.path.join(base_data_dir, f)) and f[:4].isdigit()
]

if not partition_folders:
    logging.error("No partitioned date folders found in raw_ingested_data.")
    raise Exception("No partition folders found.")

# Sort and get the latest one
latest_partition = sorted(partition_folders)[-1]
latest_data_path = os.path.join(base_data_dir, latest_partition, "customer_churn_transformed.csv")

if not os.path.exists(latest_data_path):
    logging.error(f"No CSV file found at expected location: {latest_data_path}")
    raise FileNotFoundError(f"CSV not found: {latest_data_path}")

# === Load data (replace with your Snowflake or CSV loading logic) ===
df = pd.read_csv(latest_data_path)

# === Drop leakage columns ===
# leakage_cols = [
#     'Customer Status', 'CLTV', 'Total Revenue', 'Total Charges',
#     'Churn Category', 'Churn Reason', 'Churn Score', 'Lat Long', 'Customer ID'
# ]
# df.drop(columns=leakage_cols, inplace=True, errors='ignore')

# === Define features and target ===
target = 'Churn'
X = df.drop(columns=[target])
y = df[target]

# Fill missing categorical values
for col in X.select_dtypes(include='object').columns:
    X[col] = X[col].fillna('Missing')

# === Train-test split ===
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# === Preprocessing ===
numeric_features = X_train.select_dtypes(include=['int64', 'float64']).columns.tolist()
categorical_features = X_train.select_dtypes(include=['object']).columns.tolist()

numeric_transformer = Pipeline([
    ('imputer', SimpleImputer(strategy='median')),
    ('scaler', StandardScaler())
])

categorical_transformer = Pipeline([
    ('imputer', SimpleImputer(strategy='constant', fill_value='Missing')),
    ('onehot', OneHotEncoder(handle_unknown='ignore'))
])

preprocessor = ColumnTransformer([
    ('num', numeric_transformer, numeric_features),
    ('cat', categorical_transformer, categorical_features)
])

# === Models to train ===
models = {
    "RandomForest": RandomForestClassifier(n_estimators=100, random_state=42),
    "LogisticRegression": LogisticRegression(max_iter=1000)
}

# === MLflow setup ===
mlflow.set_tracking_uri("http://localhost:5000")
mlflow.set_experiment("CHURN_PREDICTION_EXPERIMENT")

# === Model training and logging ===
for model_name, model in models.items():
    logging.info(f"Training {model_name}...")

    pipeline = Pipeline([
        ('preprocessor', preprocessor),
        ('classifier', model)
    ])

    with mlflow.start_run(run_name=model_name):
        pipeline.fit(X_train, y_train)
        y_pred = pipeline.predict(X_test)

        acc = accuracy_score(y_test, y_pred)
        report = classification_report(y_test, y_pred, output_dict=True)
        conf_matrix = confusion_matrix(y_test, y_pred)

        print(f"Model: {model_name}")
        print("Confusion Matrix:\n", conf_matrix)
        print("Classification Report:\n", classification_report(y_test, y_pred))

        mlflow.log_param("model_type", model_name)
        mlflow.log_metric("accuracy", acc)
        mlflow.log_metric("precision_class_1", report['1']['precision'])
        mlflow.log_metric("recall_class_1", report['1']['recall'])
        mlflow.log_metric("f1_score_class_1", report['1']['f1-score'])

        input_example = X_train.head(3)
        model_signature = signature.infer_signature(input_example, pipeline.predict(input_example))

        mlflow.sklearn.log_model(
            pipeline, "model",
            input_example=input_example,
            signature=model_signature
        )

        # model_path = f"{model_name.lower()}_churn_model.pkl"
        # joblib.dump(pipeline, model_path)
        # logging.info(f"{model_name} model saved to {model_path}")

