from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

from alerting import ALERT_ARGS

with DAG(
    dag_id="navwarnings",
    description="Track & tweet NGA launch hazard notices near Starbase",
    schedule="*/10 * * * *",          # toutes les 10 min
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=3), **ALERT_ARGS},
    max_active_runs=1,
) as dag:
    BashOperator(
        task_id="run_navwarnings",
        bash_command="python /opt/app/src/NavWarnings.py"
    )
