# airflow_dags/monitor_nsf_watcher.py
from __future__ import annotations
import time, json, requests
from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta

WATCHER_URL = "http://nsf-watcher:8000"

default_args = {"owner":"airflow","retries":1,"retry_delay":timedelta(minutes=2)}

with DAG(
    dag_id="monitor_nsf_watcher",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["monitoring","nsf"],
) as dag:

    @task
    def check_health_and_tail():
        h = requests.get(f"{WATCHER_URL}/health", timeout=10).json()
        logs = requests.get(f"{WATCHER_URL}/logs?n=200", timeout=10).text
        print("=== NSF WATCHER HEALTH ===")
        print(json.dumps(h, indent=2))
        print("=== LAST 200 LOG LINES ===")
        print(logs)
        now = time.time()
        stale = (now - float(h.get("last_frame_ts") or 0)) > 60
        if not h.get("ok") or stale:
            raise RuntimeError("nsf-watcher unhealthy or stale frames")

    check_health_and_tail()
