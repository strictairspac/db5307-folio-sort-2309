"""Worker: admit queued runs under the shared segment quota and execute them.

Several runs may merge at once as long as their reserved peak footprints fit
the shared quota.  Each admitted run holds a lease, refreshed while it works,
and the lease is returned exactly once when the run ends or is reclaimed after
a crash.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from threading import Event, Lock, Thread

from src.config.settings import ARTIFACTS, cache, engine
from src.services import scheduler
from src.services.domain import solve
from src.services.workflow import finish

HEARTBEAT = Path(ARTIFACTS) / 'worker-heartbeat'
POLL_SECONDS = 1
IDLE_RECOVERY_SECONDS = 5
CONCURRENCY = max(1, int(os.getenv('WORKER_CONCURRENCY', '2')))


def _watch(stop: Event, run_id: int | None = None) -> None:
    while not stop.is_set():
        HEARTBEAT.touch()
        if run_id is not None:
            try:
                scheduler.heartbeat(run_id)
            except Exception:
                logging.warning('Lease heartbeat failed for run %s', run_id, exc_info=True)
        stop.wait(2)


def run_one(run_id: int, token: str) -> None:
    """Execute one admitted run and always return its reservation."""
    stop = Event()
    Thread(target=_watch, args=(stop, run_id), daemon=True).start()
    try:
        payload = scheduler.payload_for(run_id, token)
        if payload is None:
            return
        try:
            result = solve(payload, engine, work_root=ARTIFACTS, run_id=run_id)
        except (ValueError, KeyError, TypeError, RuntimeError) as exc:
            # Validation and temp-disk failures carry a message meant for the
            # operator; the run fails without publishing an order sheet.
            finish(run_id, token, error=str(exc)[:500])
        except Exception:
            logging.exception('Domain run failed: %s', run_id)
            finish(run_id, token, error='后台执行失败，请检查输入或查看 worker 日志。')
        else:
            finish(run_id, token, result=result)
    finally:
        stop.set()
        # The run's own temp dir is gone by now (sorted: removed in a finally,
        # failed: swept by this cleanup); only then is the reservation returned.
        scheduler.release_after_run(run_id)


class Runner:
    """Owns the set of runs this worker is executing right now."""

    def __init__(self, concurrency: int = CONCURRENCY):
        self.concurrency = concurrency
        self._active: set[int] = set()
        self._lock = Lock()

    def free_slot(self) -> bool:
        with self._lock:
            return len(self._active) < self.concurrency

    def start(self, run_id: int, token: str) -> None:
        with self._lock:
            self._active.add(run_id)
        Thread(target=self._execute, args=(run_id, token), daemon=True).start()

    def _execute(self, run_id: int, token: str) -> None:
        try:
            run_one(run_id, token)
        except Exception:
            logging.exception('Run %s crashed outside its error path', run_id)
        finally:
            with self._lock:
                self._active.discard(run_id)

    def busy(self) -> bool:
        with self._lock:
            return bool(self._active)


def work() -> None:
    HEARTBEAT.unlink(missing_ok=True)
    Thread(target=_watch, args=(Event(),), daemon=True).start()
    runner = Runner()
    last_recovery = 0.0
    while True:
        try:
            while runner.free_slot():
                admitted = scheduler.admit_one()
                if admitted is None:
                    break
                run_id, token, _reserved = admitted
                runner.start(run_id, token)
            if runner.busy():
                time.sleep(0.05)
                continue

            woke = cache.brpop('worker:wake', timeout=POLL_SECONDS)
            now = time.monotonic()
            if woke is None and now - last_recovery >= IDLE_RECOVERY_SECONDS:
                last_recovery = now
                scheduler.recover()
                scheduler.retry_stuck()
        except Exception:
            logging.exception('Worker loop failure')
            time.sleep(2)


if __name__ == '__main__':
    work()
