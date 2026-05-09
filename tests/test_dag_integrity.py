from airflow.models import DagBag


def test_dag_imports_without_errors():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    assert dag_bag.import_errors == {}


def test_hello_file_etl_exists():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    assert "hello_file_etl" in dag_bag.dags


def test_task_count():
    dag = DagBag(dag_folder="dags", include_examples=False).get_dag("hello_file_etl")
    assert len(dag.tasks) == 3
