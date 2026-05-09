Here is the revised README, following the guidelines:

---

# 01 Airflow Hello ETL

A beginner-friendly Apache Airflow mini project that extracts a small CSV file, transforms it, and writes a clean JSON summary.

## What you learn

* Basic DAG structure
* PythonOperator usage
* Local file ETL
* Airflow task dependencies
* Simple DAG tests

## Project Structure

```text
.
├── dags/
│   └── hello_file_etl_dag.py
├── data/
│   └── input/sales.csv
├── tests/
│   └── test_dag_integrity.py
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Run Locally

```bash
docker compose up airflow-init
docker compose up
```

Open Airflow at:

```text
http://localhost:8080
```

Default login:

```
airflow / airflow
```

Trigger DAG:

```
hello_file_etl
```

## Run Tests

```bash
pip install -r requirements-dev.txt
pytest
```