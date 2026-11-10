from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="weekly",
    description="Weekly Starbase recap (skipped on quiet weeks)",
    schedule="0 18 * * 0",            # dimanche 18:00 UTC = midi/13h à Starbase
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    max_active_runs=1,
) as dag:
    BashOperator(
        task_id="run_weekly",
        bash_command="python /opt/app/src/Weekly.py"
    )
