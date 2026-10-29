import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from airflow.sdk import dag, task  # noqa: E402
from src.callbacks import notify_failure  # noqa: E402


@dag(
    dag_id="failure_demo",
    schedule=None,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    default_args={"retries": 0, "on_failure_callback": notify_failure},
    tags=["demo"],
)
def failure_demo():

    @task
    def always_fails():
        raise ValueError("Deliberate failure to test the failure callback")

    always_fails()


failure_demo()
