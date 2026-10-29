import json
import sys
from datetime import date, datetime
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
DB_PATH = str(PROJECT_ROOT / "data" / "warehouse.duckdb")


def read_bronze(ds: str) -> list[tuple]:
    """Read every city file for one date and flatten it to rows."""
    rows = {}  # keyed by (city, ts) so duplicates collapse to one row
    ds_date = date.fromisoformat(ds)
    for path in sorted((BRONZE_DIR / f"ds={ds}").glob("*.json")):
        city = path.stem
        hourly = json.loads(path.read_text())["hourly"]
        for t, pm10, pm25 in zip(hourly["time"], hourly["pm10"], hourly["pm2_5"]):
            ts = datetime.fromisoformat(t)
            rows[(city, ts)] = (city, ts, pm10, pm25, ds_date)
    return list(rows.values())


def load_silver(ds: str, db_path: str = DB_PATH) -> int:
    rows = read_bronze(ds)
    con = duckdb.connect(db_path)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS silver.aqi_hourly (
                city  VARCHAR,
                ts    TIMESTAMP,
                pm10  DOUBLE,
                pm2_5 DOUBLE,
                ds    DATE
            )
            """
        )
        con.execute("BEGIN")
        con.execute("DELETE FROM silver.aqi_hourly WHERE ds = ?", [date.fromisoformat(ds)])
        con.executemany("INSERT INTO silver.aqi_hourly VALUES (?, ?, ?, ?, ?)", rows)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()
    return len(rows)


if __name__ == "__main__":
    ds = sys.argv[1]
    print(f"Loaded {load_silver(ds)} rows into silver.aqi_hourly for {ds}")