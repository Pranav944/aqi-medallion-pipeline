#!/usr/bin/env bash
source .venv/bin/activate
source airflow_env.sh

active() {
python - << 'PY'
import os, sqlite3
con = sqlite3.connect(os.environ["AIRFLOW_HOME"] + "/airflow.db", timeout=30)
print(con.execute("select count(*) from dag_run where dag_id='aqi_medallion' and state in ('queued','running')").fetchone()[0])
PY
}

for chunk in "2026-10-14 2026-10-16" "2026-10-17 2026-10-19" "2026-10-20 2026-10-22" "2026-10-23 2026-10-25" "2026-10-26 2026-10-28"; do
  set -- $chunk
  created=0
  for attempt in 1 2 3 4 5 6 7 8 9 10; do
    if airflow backfill create --dag-id aqi_medallion --from-date "$1" --to-date "$2" --max-active-runs 1 --reprocess-behavior completed > /tmp/bf.out 2>&1; then
      created=1; break
    fi
    echo "attempt $attempt for $1..$2 failed (lock or active backfill), retrying in 20s"; sleep 20
  done
  [ $created -eq 1 ] || { echo "GAVE UP on $1..$2"; continue; }
  echo "created backfill $1..$2"
  sleep 20
  while [ "$(active)" != "0" ]; do sleep 20; done
  echo "DONE $1..$2"
done
echo "ALL RE-RUNS FINISHED"
