log() { echo "[$(date +%T)] $*"; }

retry() {
  local n=$1; shift; local i
  for i in $(seq 1 "$n"); do
    if "$@" > /tmp/retry.out 2>&1; then return 0; fi
    log "retry $i/$n: $*"; sleep 15
  done
  tail -5 /tmp/retry.out; return 1
}

git_push() {
  git add -n . > /tmp/gitdry.txt
  if grep -Eq "data/|\.duckdb|\.db|airflow/logs|\.generated|\.venv|airflow\.cfg" /tmp/gitdry.txt; then
    log "UNSAFE files in the commit, NOT pushing:"; cat /tmp/gitdry.txt; return 1
  fi
  git add . && git commit -m "$1" && git push && log "PUSHED TO GITHUB"
}
