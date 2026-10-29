#!/usr/bin/env bash
cd "$(dirname "$0")/.." || exit 1
source .venv/bin/activate
source airflow_env.sh
source scripts/lib.sh

log "1/6 checking DAG files parse"
python airflow/dags/aqi_medallion.py > /dev/null 2>&1 && python airflow/dags/failure_demo.py > /dev/null 2>&1 \
  || { log "DAG FILE ERROR - stopping"; python airflow/dags/aqi_medallion.py 2>&1 | tail -5; exit 1; }

log "2/6 making sure Airflow is running"
if ! pgrep -f "airflow standalone" > /dev/null; then
  nohup airflow standalone > data/airflow_standalone.log 2>&1 &
  log "started airflow standalone"; sleep 60
fi
for i in $(seq 1 20); do
  airflow dags list 2>/dev/null | grep -q failure_demo && break
  log "waiting for Airflow to see the DAGs ($i/20)"; sleep 15
done

log "3/6 testing the failure callback"
rm -f data/failures.log
retry 10 airflow dags unpause failure_demo
retry 10 airflow dags trigger failure_demo
for i in $(seq 1 18); do grep -qs failure_demo data/failures.log && break; sleep 10; done
if grep -qs failure_demo data/failures.log; then
  log "CALLBACK OK -> $(tail -1 data/failures.log)"
else
  log "CALLBACK NOT FIRED (tell me; everything else continues)"
fi
retry 10 airflow dags pause failure_demo
retry 10 airflow dags unpause aqi_medallion

log "4/6 re-running 15 days through Airflow in chunks of 3"
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
  for attempt in $(seq 1 10); do
    if airflow backfill create --dag-id aqi_medallion --from-date "$1" --to-date "$2" --max-active-runs 1 --reprocess-behavior completed > /tmp/bf.out 2>&1; then
      created=1; break
    fi
    log "chunk $1..$2 attempt $attempt failed (lock or active backfill), retry in 20s"; sleep 20
  done
  [ $created -eq 1 ] || { log "GAVE UP on $1..$2"; continue; }
  log "created backfill $1..$2"; sleep 20
  while [ "$(active)" != "0" ]; do sleep 20; done
  log "DONE $1..$2"
done

log "5/6 your real numbers"
python - << 'PY' | tee data/phase1_summary.txt
import os, sqlite3, duckdb
con = sqlite3.connect(os.environ["AIRFLOW_HOME"] + "/airflow.db", timeout=30)
print("aqi_medallion DAG runs by state:", con.execute("select state, count(*) from dag_run where dag_id='aqi_medallion' group by state").fetchall())
print("quality_gate task successes:", con.execute("select count(*) from task_instance where dag_id='aqi_medallion' and task_id='quality_gate' and state='success'").fetchone()[0])
d = duckdb.connect("data/warehouse.duckdb", read_only=True)
print("Silver days / rows:", d.execute("select count(distinct ds), count(*) from silver.aqi_hourly").fetchone())
print("duplicate city+hour groups:", d.execute("select count(*) from (select city, ts from silver.aqi_hourly group by city, ts having count(*) > 1)").fetchone()[0])
PY

log "6/6 pausing the DAG (no stray runs before the schedule fix) and pushing"
retry 10 airflow dags pause aqi_medallion
git_push "Phase 1: Airflow DAG with mapped extract, quality gate, shared failure callback, failure demo DAG; 15 day re-run through Airflow"
log "PHASE 1 PART 1 FINISHED"
