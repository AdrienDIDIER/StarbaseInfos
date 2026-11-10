from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="digest",
    description="Daily 'Starbase today' digest",
    schedule="0 13 * * *",            # 13:00 UTC = 7h/8h du matin à Starbase
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    max_active_runs=1,
) as dag:
    BashOperator(
        task_id="run_digest",
        bash_command="python /opt/app/src/Digest.py"
    )
