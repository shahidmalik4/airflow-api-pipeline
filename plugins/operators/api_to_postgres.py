from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import json
import re


class ApiToPostgresOperator(BaseOperator):

    def __init__(
        self,
        api_url,
        table_name,
        postgres_conn_id="postgres_default",
        primary_key="id",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.api_url = api_url
        self.table_name = table_name
        self.postgres_conn_id = postgres_conn_id
        self.primary_key = primary_key

    def _safe_table_name(self, name):
        # basic safety check
        if not re.match(r"^[a-zA-Z0-9_]+$", name):
            raise ValueError(f"Unsafe table name: {name}")
        return name

    def execute(self, context):

        self.log.info(f"Fetching data from {self.api_url}")

        response = requests.get(self.api_url)
        response.raise_for_status()

        data = response.json()[:10]

        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()

        table = self._safe_table_name(self.table_name)

        # -------------------------
        # CREATE TABLE (generic JSONB)
        # -------------------------
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id TEXT PRIMARY KEY,
                data JSONB
            );
        """)

        # -------------------------
        # INSERT (idempotent)
        # -------------------------
        insert_sql = f"""
            INSERT INTO {table} (id, data)
            VALUES (%s, %s)
            ON CONFLICT (id) DO UPDATE
            SET data = EXCLUDED.data;
        """

        rows = []

        for item in data:

            # flexible ID handling
            record_id = str(item.get(self.primary_key, item.get("id", None)))

            if record_id is None:
                continue

            rows.append((record_id, json.dumps(item)))

        cursor.executemany(insert_sql, rows)
        conn.commit()

        self.log.info(f"Loaded {len(rows)} rows into {table}")