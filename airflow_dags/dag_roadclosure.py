from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="roadclosure",
    description="Scrape & tweet road closures",
    schedule="*/2 * * * *",           # toutes les 2 min
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    max_active_runs=1,
) as dag:
    BashOperator(
        task_id="run_roadclosure",
        bash_command="python /opt/app/src/Scraper_RC.py"
    )
