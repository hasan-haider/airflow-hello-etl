from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path("/opt/airflow")
INPUT_FILE = PROJECT_ROOT / "data" / "input" / "sales.csv"
OUTPUT_DIR = PROJECT_ROOT / "data" / "output"
OUTPUT_FILE = OUTPUT_DIR / "sales_summary.json"


def extract_sales(**context):
    df = pd.read_csv(INPUT_FILE)
    records = df.to_dict(orient="records")
    context["ti"].xcom_push(key="sales_records", value=records)


def transform_sales(**context):
    records = context["ti"].xcom_pull(task_ids="extract_sales", key="sales_records")
    df = pd.DataFrame(records)

    summary = {
        "total_orders": int(len(df)),
        "total_revenue": float(df["amount"].sum()),
        "average_order_value": float(df["amount"].mean()),
        "currency": df["currency"].mode()[0],
        "processed_at": datetime.utcnow().isoformat(),
    }

    context["ti"].xcom_push(key="sales_summary", value=summary)


def load_summary(**context):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    summary = context["ti"].xcom_pull(task_ids="transform_sales", key="sales_summary")
    OUTPUT_FILE.write_text(json.dumps(summary, indent=2), encoding="utf-8")


with DAG(
    dag_id="hello_file_etl",
    description="Beginner Airflow DAG: CSV to JSON summary",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["beginner", "etl", "local"],
) as dag:

    extract = PythonOperator(
        task_id="extract_sales",
        python_callable=extract_sales,
    )

    transform = PythonOperator(
        task_id="transform_sales",
        python_callable=transform_sales,
    )

    load = PythonOperator(
        task_id="load_summary",
        python_callable=load_summary,
    )

    extract >> transform >> load
