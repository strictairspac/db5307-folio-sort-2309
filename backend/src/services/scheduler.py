"""Capacity admission and crash recovery used by the worker loop.

Admission: a queued run only starts after its predicted peak segment footprint
(old and new runs that can coexist, not just the merged output) has been
reserved against the shared quota.

Recovery: a lease whose worker stopped heartbeating is reclaimed by removing
that worker's leftover segment directories, then returning the reservation.  A
retried cleanup never refunds the same bytes twice: the lease flips to
``released`` under the capacity row lock in the same transaction as the refund.
Only directories whose live-lock can be taken are deleted, and a directory is
locked while it is deleted so two recoverers cannot clean it concurrently.
"""

from __future__ import annotations

import logging

from sqlalchemy import select, update

from src.config.settings import ARTIFACTS, engine
from src.constants.workflow import Status
from src.models.tables import audit, leases, runs
from src.services import capacity, residue
from src.utils.time import utc_time

LEASE_TIMEOUT_SECONDS = 120


def admit_one() -> tuple[int, str, int] | None:
    """Reserve capacity for the next eligible queued run.

    ``LeaseConflict`` means a lease from an earlier attempt of the same run has
    not been returned; the caller retries on the next loop rather than starting
    a second copy of that run.
    """
    _backfill_plans()
    try:
        with engine.begin() as conn:
            return capacity.reserve_next(conn)
    except capacity.LeaseConflict:
        logging.warning('Admission hit an unreturned lease; retrying later', exc_info=True)
        return None


def _backfill_plans() -> None:
    """Record disk plans for queued runs submitted before plans were tracked."""
    from src.services import domain

    with engine.connect() as conn:
        pending = capacity.unplanned(conn)
    for row in pending:
        try:
            plan = domain.measure(row['payload'])
        except (ValueError, TypeError) as exc:
            # An unplannable legacy queue entry would wait forever; fail it with
            # the same validation message a new submission would receive.
            with engine.begin() as conn:
                conn.execute(update(runs).where(
                    runs.c.id == row['id'], runs.c.status == Status.QUEUED).values(
                    status=Status.FAILED, finished_at=utc_time(), error=str(exc)[:500]))
            continue
        with engine.begin() as conn:
            capacity.record_plan(conn, row['id'], plan.peak_bytes, plan.initial_runs)


def payload_for(run_id: int, token: str):
    return capacity.payload_for(run_id, token)


def heartbeat(run_id: int) -> None:
    """Refresh a lease while its run is merging."""
    with engine.begin() as conn:
        capacity.touch(conn, run_id)


def release_after_run(run_id: int) -> None:
    """Sweep leftovers, then return capacity once this worker's run finishes.

    Must run for every admitted run, including one whose payload could not be
    loaded: capacity is only returned together with a clean temp dir.
    """
    try:
        clean_run_dirs(run_id)
    except OSError:
        logging.warning('Leftovers for run %s remain; keeping them reserved', run_id, exc_info=True)
        with engine.begin() as conn:
            capacity.lock_quota(conn)
            capacity.note_cleanup_attempt(conn, run_id)
        return
    try:
        with engine.begin() as conn:
            # Lock the shared capacity row before the lease row: admission uses
            # the same order, so cleanup and admission cannot deadlock.
            capacity.lock_quota(conn)
            capacity.note_cleanup_attempt(conn, run_id)
            capacity.release(conn, run_id)
    except Exception:
        # The lease stays active; the orphan pass retries the return later.
        logging.warning('Lease return failed for run %s; will retry', run_id, exc_info=True)


def retry_stuck(limit: int = 20) -> int:
    """Retry cleanup+return for this worker's leases whose run already ended."""
    with engine.connect() as conn:
        stuck = capacity.orphan_leases(conn)[:limit]
    for lease in stuck:
        release_after_run(lease['run_id'])
    return len(stuck)


def clean_run_dirs(run_id: int) -> None:
    """Remove this run's segment dirs, refusing any that a live process holds."""
    for path in residue.run_dirs(ARTIFACTS, run_id):
        if not path.exists():
            continue
        with residue.claimed(ARTIFACTS, path) as owned:
            if not owned:
                raise OSError(f'执行 {run_id} 的工作目录仍被占用: {path}')
            residue.remove(ARTIFACTS, path)


def reclaim_stale() -> int:
    """Clean and return leases left by a worker that stopped heartbeating.

    A run still marked running is marked failed so the operator can see why it
    stopped and resubmit; a run that had already finished is only released.
    Either way the refund happens in the same transaction as the lease flip.
    """
    with engine.connect() as conn:
        stale = capacity.stale_leases(conn, utc_time(-LEASE_TIMEOUT_SECONDS))
    reclaimed = 0
    for lease in stale:
        run_id = lease['run_id']
        try:
            clean_run_dirs(run_id)
        except OSError:
            logging.warning('Residue cleanup for run %s failed; will retry', run_id, exc_info=True)
            with engine.begin() as conn:
                capacity.lock_quota(conn)
                capacity.note_cleanup_attempt(conn, run_id)
            continue
        with engine.begin() as conn:
            capacity.lock_quota(conn)
            row = conn.execute(select(leases).where(leases.c.run_id == run_id)
                               .with_for_update()).mappings().first()
            capacity.note_cleanup_attempt(conn, run_id)
            if row is None or row['status'] != capacity.LEASE_ACTIVE:
                continue
            if row['heartbeat_at'] is not None and (
                    row['heartbeat_at'] > utc_time(-LEASE_TIMEOUT_SECONDS)):
                continue
            conn.execute(update(runs).where(
                runs.c.id == run_id, runs.c.status == Status.RUNNING,
                runs.c.attempt_token == lease['attempt_token']).values(
                status=Status.FAILED, finished_at=utc_time(),
                error='后台执行中断，已清理残段并释放容量'))
            if lease['run_status'] == Status.RUNNING:
                conn.execute(audit.insert().values(actor='worker', action='reclaimed',
                                                   reference=str(run_id), created_at=utc_time()))
            if capacity.release(conn, run_id):
                reclaimed += 1
    return reclaimed


def recover_orphans() -> int:
    """Return leases whose run already finished before its cleanup ran."""
    returned = 0
    with engine.connect() as conn:
        orphans = capacity.orphan_leases(conn)
    for lease in orphans:
        run_id = lease['run_id']
        try:
            clean_run_dirs(run_id)
        except OSError:
            logging.warning('Orphan cleanup for run %s failed; will retry', run_id, exc_info=True)
            with engine.begin() as conn:
                capacity.lock_quota(conn)
                capacity.note_cleanup_attempt(conn, run_id)
            continue
        with engine.begin() as conn:
            capacity.lock_quota(conn)
            capacity.note_cleanup_attempt(conn, run_id)
            if capacity.release(conn, run_id):
                returned += 1
    return returned


def reclaim_unleased() -> int:
    """Fail runs that were marked running but no longer hold any lease.

    The directory and the capacity were already handled when the lease was
    returned; marking the run failed stops it from showing as active forever.
    """
    with engine.connect() as conn:
        stranded = capacity.running_without_lease(conn, utc_time(-LEASE_TIMEOUT_SECONDS))
    failed = 0
    for run in stranded:
        with engine.begin() as conn:
            changed = conn.execute(update(runs).where(
                runs.c.id == run['id'], runs.c.status == Status.RUNNING,
                runs.c.attempt_token == run['attempt_token']).values(
                status=Status.FAILED, finished_at=utc_time(),
                error='后台执行未持有容量租约，已标记失败，请重新提交')).rowcount
            if changed:
                conn.execute(audit.insert().values(
                    actor='worker', action='reclaimed', reference=str(run['id']),
                    created_at=utc_time()))
                failed += 1
    return failed


def recover() -> tuple[int, int, int]:
    """All recovery passes; safe to call on every idle worker wakeup."""
    return reclaim_stale(), recover_orphans(), reclaim_unleased()
