from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import pandas as pd

def csv():
    with open("/opt/airflow/data/pets-data.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    df = pd.json_normalize(data)
    df.to_csv("/opt/airflow/data/result.csv", index=False, encoding="utf-8")


with DAG(dag_id="json_csv", start_date=datetime(2026, 1, 1)) as dag:
    transform = PythonOperator(task_id="csv", python_callable=csv)
    transform