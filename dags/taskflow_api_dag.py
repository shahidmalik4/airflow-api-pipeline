from airflow.decorators import dag, task
from datetime import datetime

from operators.api_to_postgres import ApiToPostgresOperator


@dag(
    dag_id="taskflow_todos_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["taskflow", "todos", "api"]
)
def taskflow_todos():

    # -------------------------
    # EXTRACT (just metadata check / optional)
    # -------------------------
    @task
    def log_pipeline():
        return "Starting todos pipeline"

    # -------------------------
    # LOAD (CUSTOM OPERATOR)
    # -------------------------
    load = ApiToPostgresOperator(
        task_id="load_todos",
        api_url="https://jsonplaceholder.typicode.com/todos",
        table_name="raw_todos"
    )

    log_pipeline() >> load


taskflow_todos()