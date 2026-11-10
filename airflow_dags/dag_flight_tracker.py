from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="flight_tracker",
    description="Per-flight thread of official launch signals",
    schedule="7-59/15 * * * *",       # toutes les 15 min (quota Launch Library 2 : 15 requêtes/heure)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"retries": 0},
    max_active_runs=1,
) as dag:
    BashOperator(
        task_id="run_flight_tracker",
        bash_command="python /opt/app/src/FlightTracker.py"
    )
