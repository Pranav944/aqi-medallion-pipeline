import sys
from datetime import datetime, timedelta
from pathlib import Path

# Let the DAG import our src/ package
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from airflow.sdk import dag, task  # noqa: E402
from airflow.timetables.interval import CronDataIntervalTimetable  # noqa: E402
from src.callbacks import notify_failure  # noqa: E402
from src.extract import CITIES, extract_to_bronze  # noqa: E402
from src.load_silver import load_silver  # noqa: E402
from src.quality import run_quality_checks  # noqa: E402


@dag(
    dag_id="aqi_medallion",
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=datetime(2026, 9, 29),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": notify_failure,
    },
    tags=["aqi", "medallion"],
)
def aqi_medallion():

    @task
    def extract_city(city: dict, ds=None):
        """Bronze: fetch one city for one date and save raw JSON."""
        return extract_to_bronze(city, ds)

    @task
    def load_to_silver(paths, ds=None):
        """Silver: read all Bronze files for the date into DuckDB."""
        files = list(paths)
        print(f"Received {len(files)} Bronze files for {ds}")
        rows = load_silver(ds)
        print(f"Loaded {rows} rows into silver.aqi_hourly for {ds}")
        return rows

    @task(retries=0)
    def quality_gate(rows_loaded, ds=None):
        """Stop the DAG if the day's data looks wrong (no point retrying)."""
        result = run_quality_checks(ds, expected_cities=len(CITIES))
        print(f"Quality gate passed for {ds}: {result}")
        return result

    bronze_paths = extract_city.expand(city=CITIES)  # dynamic task mapping
    rows = load_to_silver(bronze_paths)
    quality_gate(rows)


aqi_medallion()
 