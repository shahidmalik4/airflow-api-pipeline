from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def hello():
    print("🚀 Airflow is working!")


def add_numbers():
    result = 2 + 3
    print(f"Result: {result}")


with DAG(
    dag_id="test_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["test"],
) as dag:

    task1 = PythonOperator(
        task_id="hello_task",
        python_callable=hello,
    )

    task2 = PythonOperator(
        task_id="add_task",
        python_callable=add_numbers,
    )

    task1 >> task2