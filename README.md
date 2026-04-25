# Airflow DE Sandbox (API → Postgres Orchestration)

A modular Apache Airflow project simulating real-world Data Engineering pipelines using APIs, dynamic DAG generation, and custom operators.

---

## ⚙️ Tech Stack

- Apache Airflow
- Docker & Docker Compose
- PostgreSQL
- Python (requests, psycopg2)
- JSONPlaceholder API

---

## 📌 Project Overview

This project demonstrates core Data Engineering orchestration concepts using Airflow:

- TaskFlow API DAGs
- Dynamic DAG generation from YAML config
- Custom Airflow operators
- API ingestion pipelines
- Postgres loading with idempotency
- Trigger-based DAG orchestration

---

## 🧱 Architecture

API (JSONPlaceholder)
        ↓
Airflow DAGs
        ↓
Custom Operator (API → Transform → Load)
        ↓
PostgreSQL (raw tables)

---

## 📂 DAG Types

### 1. TaskFlow API DAG
- Simple ETL pipeline using Airflow TaskFlow API

### 2. Dynamic Pipelines
- DAGs generated from YAML configuration

### 3. Custom Operator DAGs
- Reusable operator for API ingestion into Postgres

### 4. Orchestration DAGs
- DAG chaining using TriggerDagRunOperator

---

## 🔁 Features Implemented

- Dynamic DAG creation
- Custom API-to-Postgres operator
- Idempotent inserts (ON CONFLICT DO NOTHING)
- Retry handling (API level)
- Structured logging (JSON logs)
- DAG dependencies and triggers

---

## How to Run

```bash
docker-compose up -d
```

## Airflow UI
```
http://localhost:8080
```

## Login
```
username: admin
password: admin
```