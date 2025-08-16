from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="msib",
    description="OCR PDF & tweet",
    schedule="*/5 * * * *",          # toutes les 5 min
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    max_active_runs=1,
) as dag:
    BashOperator(
        task_id="run_msib",
        bash_command="python /opt/app/src/MSIB.py"
    )
