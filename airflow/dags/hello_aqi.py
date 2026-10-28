from datetime import datetime
from airflow.sdk import dag, task

@dag(
    schedule="@daily",
    start_date=datetime(2026,10,28),
    catchup=False,
    tags=["day1"],
)

def hello_aqi():
    @task
    def pick_city():
        print("picking a city ...")
        return 'Delhi'

    @task
    def announce(city: str):
        print(f"Next,we will fetch airquality for {city}")
    announce(pick_city())
hello_aqi()