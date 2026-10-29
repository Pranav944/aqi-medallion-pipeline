import sys
from datetime import date
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = str(PROJECT_ROOT / "data" / "warehouse.duckdb")


def run_quality_checks(ds: str, expected_cities: int = 8, db_path: str = DB_PATH) -> dict:
    """Validate one day of Silver data. Raises ValueError if anything is wrong."""
    day = date.fromisoformat(ds)
    con = duckdb.connect(db_path, read_only=True)
    try:
        total, cities, null_pm25, min_pm25, max_pm25 = con.execute(
            """
            SELECT count(*), count(DISTINCT city),
                   count(*) FILTER (WHERE pm2_5 IS NULL),
                   min(pm2_5), max(pm2_5)
            FROM silver.aqi_hourly WHERE ds = ?
            """,
            [day],
        ).fetchone()
        dupes = con.execute(
            """
            SELECT count(*) FROM (
                SELECT city, ts FROM silver.aqi_hourly
                WHERE ds = ? GROUP BY city, ts HAVING count(*) > 1
            )
            """,
            [day],
        ).fetchone()[0]
    finally:
        con.close()

    expected_rows = expected_cities * 24
    failures = []
    if total != expected_rows:
        failures.append(f"row count {total}, expected {expected_rows}")
    if cities != expected_cities:
        failures.append(f"{cities} cities, expected {expected_cities}")
    if dupes > 0:
        failures.append(f"{dupes} duplicate city/hour groups")
    if total > 0 and null_pm25 / total > 0.10:
        failures.append(f"{null_pm25} null PM2.5 values out of {total}")
    if min_pm25 is not None and (min_pm25 < 0 or max_pm25 > 1000):
        failures.append(f"PM2.5 out of range ({min_pm25} to {max_pm25})")

    if failures:
        raise ValueError(f"Quality gate FAILED for {ds}: " + "; ".join(failures))

    return {"ds": ds, "rows": total, "cities": cities, "null_pm25": null_pm25,
            "min_pm25": min_pm25, "max_pm25": max_pm25}


if __name__ == "__main__":
    print(run_quality_checks(sys.argv[1]))
