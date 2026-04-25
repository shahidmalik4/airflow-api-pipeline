from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import requests
import json

from config.config_loader import load_pipelines


def create_dag(dag_id, endpoint, table, triggers=None):

    @dag(
        dag_id=dag_id,
        start_date=datetime(2024, 1, 1),
        schedule=None,   # IMPORTANT: event-driven
        catchup=False,
        tags=["dynamic"]
    )
    def dynamic_pipeline():

        # -------------------------
        # EXTRACT
        # -------------------------
        @task(retries=2, retry_delay=timedelta(minutes=1))
        def extract():
            url = f"https://jsonplaceholder.typicode.com/{endpoint}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()[:5]

        # -------------------------
        # LOAD (IDEMPOTENT)
        # -------------------------
        @task
        def load(data):
            hook = PostgresHook(postgres_conn_id="postgres_default")
            conn = hook.get_conn()
            cursor = conn.cursor()

            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    id INT PRIMARY KEY,
                    data JSONB
                )
            """)

            insert_sql = f"""
                INSERT INTO {table} (id, data)
                VALUES (%s, %s)
                ON CONFLICT (id) DO NOTHING
            """

            rows = [(item["id"], json.dumps(item)) for item in data]

            cursor.executemany(insert_sql, rows)

            conn.commit()
            cursor.close()
            conn.close()

        # -------------------------
        # FLOW
        # -------------------------
        data = extract()
        loaded = load(data)

        # -------------------------
        # TRIGGER DOWNSTREAM DAGS
        # -------------------------
        if triggers:
            trigger_tasks = []

            for t in triggers:
                trigger_task = TriggerDagRunOperator(
                    task_id=f"trigger_{t}",
                    trigger_dag_id=t
                )
                trigger_tasks.append(trigger_task)

            loaded >> trigger_tasks

    return dynamic_pipeline()


# -------------------------
# GENERATE ALL DAGS
# -------------------------
PIPELINES = load_pipelines()["pipelines"]

for pipeline in PIPELINES:
    globals()[pipeline["dag_id"]] = create_dag(
        dag_id=pipeline["dag_id"],
        endpoint=pipeline["endpoint"],
        table=pipeline["table"]
    )