from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

from alerting import ALERT_ARGS

# roadclosure tourne 720 fois par jour : sans ménage, l'historique et les logs grossissent sans fin
RETENTION_DAYS = 7
LOG_DIR = "/opt/airflow/logs"

with DAG(
    dag_id="cleanup",
    description=f"Delete Airflow run history and log files older than {RETENTION_DAYS} days",
    schedule="30 3 * * *",            # tous les jours à 03:30 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=30), **ALERT_ARGS},
    max_active_runs=1,
) as dag:
    # "db clean" garde toujours le dernier run de chaque DAG, même plus vieux que la limite
    BashOperator(
        task_id="run_cleanup",
        bash_command=(
            f'airflow db clean --clean-before-timestamp "{{{{ macros.ds_add(ds, -{RETENTION_DAYS}) }}}}" '
            "--skip-archive --yes && "
            f"find {LOG_DIR} -type f -mtime +{RETENTION_DAYS} -delete && "
            f"find {LOG_DIR} -mindepth 1 -type d -empty -mtime +{RETENTION_DAYS} -delete"
        ),
    )
