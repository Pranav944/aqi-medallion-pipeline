from datetime import datetime, timezone
from pathlib import Path

LOG_PATH = Path(__file__).resolve().parents[1] / "data" / "failures.log"


def notify_failure(context):
    """Failure callback: append one line to data/failures.log."""
    ti = context.get("task_instance") or context.get("ti")
    line = (
        f"{datetime.now(timezone.utc).isoformat()} FAILED "
        f"dag={getattr(ti, 'dag_id', '?')} task={getattr(ti, 'task_id', '?')} "
        f"run={context.get('run_id', '?')}\n"
    )
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_PATH, "a") as f:
        f.write(line)
