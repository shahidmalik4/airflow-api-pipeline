from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

from operators.api_to_postgres import ApiToPostgresOperator
from config.config_loader import load_pipelines


def create_dag(dag_id, endpoint, table, triggers=None):

    @dag(
        dag_id=dag_id,
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
        tags=["custom_operator", "dynamic"]
    )
    def dynamic_pipeline():

        # -------------------------
        # MAIN TASK (CUSTOM OPERATOR)
        # -------------------------
        load_task = ApiToPostgresOperator(
            task_id=f"load_{endpoint}",
            api_url=f"https://jsonplaceholder.typicode.com/{endpoint}",
            table_name=table
        )

        # -------------------------
        # TRIGGERS (optional)
        # -------------------------
        if triggers:
            trigger_tasks = []

            for t in triggers:
                trigger_task = TriggerDagRunOperator(
                    task_id=f"trigger_{t}",
                    trigger_dag_id=t
                )
                trigger_tasks.append(trigger_task)

            load_task >> trigger_tasks

    return dynamic_pipeline()


# -------------------------
# REGISTER ALL DAGS
# -------------------------
PIPELINES = load_pipelines()["custom_pipelines"]

for pipeline in PIPELINES:
    globals()[pipeline["dag_id"]] = create_dag(
        dag_id=pipeline["dag_id"],
        endpoint=pipeline["endpoint"],
        table=pipeline["table"]
    )