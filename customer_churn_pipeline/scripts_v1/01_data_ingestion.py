import pandas as pd
import numpy as np
import logging
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
# This script generates synthetic customer churn data for a machine learning pipeline.

# Set up logging
logging.basicConfig(filename=r"C:\Users\adity\Mtech\DMML\DMML_Assignment\customer_churn_pipeline\data\logs\01_ingestion.log",
                     level=logging.INFO,
                     format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
                     )

# Set seed for reproducibility
np.random.seed(42)

# Simulate 1000 customers
n_customers = 1000

# Generate synthetic customer data
data = pd.DataFrame({
    'customer_id': range(1, n_customers + 1),
    'age': np.random.randint(18, 70, size=n_customers),
    'gender': np.random.choice(['Male', 'Female'], size=n_customers),
    'tenure_months': np.random.randint(1, 60, size=n_customers),
    'monthly_spend': np.round(np.random.uniform(20, 500, size=n_customers), 2),
    'num_logins': np.random.poisson(20, size=n_customers),
    'num_transactions': np.random.poisson(10, size=n_customers),
    'is_premium_member': np.random.choice([0, 1], size=n_customers, p=[0.7, 0.3]),
    'last_login_days_ago': np.random.randint(0, 60, size=n_customers),
})

# Generate churn variable: 1 = churned, 0 = retained
# Probabilistic churn scoring logic
data['score_tenure'] = np.clip((12 - data['tenure_months']) / 12, 0, 1)
data['score_spend'] = np.clip((150 - data['monthly_spend']) / 150, 0, 1)
data['score_inactive'] = np.clip(data['last_login_days_ago'] / 60, 0, 1)
data['score_transactions'] = np.clip((5 - data['num_transactions']) / 5, 0, 1)

data['churn_probability'] = (
    0.35 * data['score_tenure'] +
    0.25 * data['score_spend'] +
    0.25 * data['score_inactive'] +
    0.15 * data['score_transactions']
)

data['churn_probability'] += np.random.normal(0, 0.05, size=n_customers)
data['churn_probability'] = data['churn_probability'].clip(0, 1)

data['churn'] = np.random.binomial(n=1, p=data['churn_probability'])

# Drop helper columns if not needed
data.drop(columns=['score_tenure', 'score_spend', 'score_inactive', 'score_transactions', 'churn_probability'], inplace=True)


conn = snowflake.connector.connect(
    user="admin",
    password="Aditya@9705064197",  # use env var or secret manager in production
    account="jerzgki-ye33911",
    warehouse="COMPUTE_WH",
    database="CUSTOMER_DB",
    schema="TELECOM"
    # role="YOUR_ROLE"  # optional
)

# 3. Upload dataframe using write_pandas (best method!)
success, nchunks, nrows, _ = write_pandas(
    conn=conn,
    df=data,
    table_name="customer_churn",
    auto_create_table=True,    # Will create table if it doesn't exist
    overwrite=True             # Replace existing data
)

# 4. Result
if success:
    print(f"Uploaded {nrows} rows to 'telco_churn_raw' in Snowflake.")
    logging.info(f"Uploaded {nrows} rows to 'customer_churn' in Snowflake.")
else:
    print("Upload failed.")
    logging.error("Upload to Snowflake failed.")

# 5. Close connection
conn.close()

# Preview the dataset
print(data.head())
logging.info("Synthetic customer churn data generated successfully.")