"""In-container scheduler for the post-close daily rebuild.

Runs as the ``scheduler`` service in docker-compose so nothing about the job lives
on the host: no launchd agent, no crontab. It wakes once a minute, decides from
the wall clock and the run records whether today's rebuild is still owed, and if
so runs ``scripts/daily_update.py`` as a subprocess.

Why a polling loop rather than cron or one long ``sleep`` until 16:00: when the
host sleeps, Docker Desktop's VM is paused with it, and a single long sleep can
wake hours late or not notice the date changed. Re-reading the wall clock every
minute means a machine that wakes at 19:30 runs the 16:00 job at 19:31.

Why a subprocess: a rebuild peaks at a few GB of pandas frames. Running it in a
child process hands all of that back to the VM when it exits, and a crash inside
the rebuild cannot take the scheduler down with it.

"Owed" is decided from the run records ``daily_update.py`` already writes, never
from state kept in this process, so restarting the container is always safe:

* ``updated`` or ``skipped_non_trading`` today → settled, nothing to do;
* ``failed`` / ``rejected`` today → retried after ``RETRY_MINUTES``, at most
  ``MAX_ATTEMPTS`` times a day;
* a missed day is not replayed. Each rebuild re-pulls full history, so the next
  successful run carries every bar the missed one would have.

Each loop also writes ``logs/scheduler/state.json`` — the one file to read to
answer "is the scheduler alive and what is it waiting for".

Settings (environment):

    AXDATA_UPDATE_AT        HH:MM Beijing, default 16:00
    AXDATA_UPDATE_RETRIES   attempts per day, default 3
    AXDATA_RETRY_MINUTES    gap between attempts, default 30
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

CHINA_TZ = timezone(timedelta(hours=8))
POLL_SECONDS = 60
SETTLED = {"updated", "skipped_non_trading"}
RETRYABLE = {"failed", "rejected"}

APP_ROOT = Path(__file__).resolve().parents[1]
LOG_ROOT = Path(os.getenv("AXDATA_LOG_DIR", APP_ROOT / "logs"))
RECORD_DIR = LOG_ROOT / "daily_update"
STATE_PATH = LOG_ROOT / "scheduler" / "state.json"


def setting_time() -> tuple[int, int]:
    raw = os.getenv("AXDATA_UPDATE_AT", "16:00")
    hour, minute = raw.split(":")
    return int(hour), int(minute)


def setting_int(name: str, default: int) -> int:
    try:
        return max(1, int(os.getenv(name, default)))
    except ValueError:
        return default


def log(message: str) -> None:
    stamp = datetime.now(CHINA_TZ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[scheduler {stamp}] {message}", flush=True)


def todays_records(today: str) -> list[dict[str, object]]:
    """Run records written on ``today`` (YYYY-MM-DD, Beijing), oldest first."""

    records: list[dict[str, object]] = []
    stamp = today.replace("-", "")
    for path in sorted(RECORD_DIR.glob(f"daily_update_{stamp}_*.json")):
        try:
            records.append(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, ValueError):
            continue
    return records


def decide(now: datetime, records: list[dict[str, object]]) -> tuple[bool, str]:
    """Whether a rebuild should start now, and why (or why not).

    Pure: takes the clock and today's records, so it can be exercised without
    running anything.
    """

    if now.weekday() >= 5:
        return False, "周末"
    hour, minute = setting_time()
    due_at = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if now < due_at:
        return False, f"等待 {hour:02d}:{minute:02d}"

    outcomes = [str(record.get("outcome")) for record in records]
    settled = [outcome for outcome in outcomes if outcome in SETTLED]
    if settled:
        return False, f"今日已完成（{settled[-1]}）"

    attempts = [record for record in records if record.get("outcome") in RETRYABLE]
    max_attempts = setting_int("AXDATA_UPDATE_RETRIES", 3)
    if len(attempts) >= max_attempts:
        return False, f"今日已失败 {len(attempts)} 次，停止重试"
    if attempts:
        last = datetime.fromisoformat(str(attempts[-1].get("ranAt")))
        retry_after = last + timedelta(minutes=setting_int("AXDATA_RETRY_MINUTES", 30))
        if now < retry_after:
            return False, f"第 {len(attempts)} 次失败，{retry_after:%H:%M} 后重试"
        return True, f"重试（第 {len(attempts) + 1} 次）"
    return True, "到点执行"


def write_state(state: dict[str, object]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE_PATH)


def run_update() -> int:
    command = [sys.executable, str(APP_ROOT / "scripts" / "daily_update.py")]
    log(f"starting: {' '.join(command)}")
    completed = subprocess.run(command, cwd=APP_ROOT, check=False)
    log(f"finished with exit code {completed.returncode}")
    return completed.returncode


def main() -> int:
    stopping = False

    def stop(signum, _frame):  # noqa: ANN001 - signal handler signature
        nonlocal stopping
        stopping = True
        log(f"received signal {signum}, exiting after the current step")

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    hour, minute = setting_time()
    log(f"started; daily rebuild at {hour:02d}:{minute:02d} Beijing, Mon-Fri")
    started_at = datetime.now(CHINA_TZ).isoformat()
    last_exit: int | None = None
    last_run_at: str | None = None

    while not stopping:
        now = datetime.now(CHINA_TZ)
        today = now.strftime("%Y-%m-%d")
        due, reason = decide(now, todays_records(today))
        write_state(
            {
                "startedAt": started_at,
                "checkedAt": now.isoformat(),
                "updateAt": f"{hour:02d}:{minute:02d}",
                "due": due,
                "reason": reason,
                "lastRunAt": last_run_at,
                "lastExitCode": last_exit,
                "running": due,
            }
        )
        if due:
            log(reason)
            last_run_at = now.isoformat()
            last_exit = run_update()
            continue

        # Short sleeps so SIGTERM (docker stop) is honoured within a second.
        for _ in range(POLL_SECONDS):
            if stopping:
                break
            time.sleep(1)

    return 0


if __name__ == "__main__":
    sys.exit(main())
