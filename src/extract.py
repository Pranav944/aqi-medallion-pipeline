import json
import sys
from pathlib import Path
import requests

API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
BRONZE_DIR = Path("data/bronze")


CITIES = [
    {"name": "Bengaluru", "lat": 12.97, "lon": 77.59},
    {"name": "Delhi", "lat": 28.61, "lon": 77.21},
    {"name": "Mumbai", "lat": 19.07, "lon": 72.88},
    {"name": "Chennai", "lat": 13.08, "lon": 80.27},
    {"name": "Hyderabad", "lat": 17.38, "lon": 78.48},
    {"name": "Kolkata", "lat": 22.57, "lon": 88.36},
    {"name": "Pune", "lat": 18.52, "lon": 73.86},
    {"name": "Ahmedabad", "lat": 23.02, "lon": 72.57},
]


def fetch_city(city: dict,ds: str)->dict:
    """Call the API for one city and one date (ds = 'YYYY-MM-DD')."""
    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "hourly": "pm10,pm2_5",
        "start_date": ds,
        "end_date": ds,
    }
    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()

def save_bronze(city_name: str, ds: str, payload: dict) -> Path:
    """Write the raw JSON to data/bronze/ds=<date>/<city>.json (overwrites)."""
    out = BRONZE_DIR / f"ds={ds}" / f"{city_name}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload))
    return out

def extract_to_bronze(city: dict, ds: str) -> str:
    payload = fetch_city(city, ds)
    path = save_bronze(city["name"], ds, payload)
    return str(path)

if __name__ == "__main__":
    ds = sys.argv[1]
    for city in CITIES:
        print(extract_to_bronze(city, ds))