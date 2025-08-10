# scripts/01_problem_formulation.py
import json

problem_definition = {
    "business_problem": "Predict customer churn to reduce loss of customers and revenue.",
    "objectives": [
        "Ingest customer data from multiple sources",
        "Clean, validate, and transform data",
        "Build ML model for churn prediction",
        "Automate pipeline execution"
    ],
    "data_sources": [
        {"type": "csv", "path": "data_source_1.csv", "attributes": ["customer_id", "age", "tenure", "spend", "churn"]},
        {"type": "api", "url": "https://api.mock.com/customers"}
    ],
    "outputs": {
        "clean_data": "EDA-ready dataset",
        "features": "Transformed dataset for ML",
        "model": "Deployed churn prediction model"
    },
    "metrics": ["accuracy", "precision", "recall", "f1_score"]
}

with open("docs/problem_formulation.json", "w") as f:
    json.dump(problem_definition, f, indent=4)

print("Problem formulation saved in docs/problem_formulation.json")
