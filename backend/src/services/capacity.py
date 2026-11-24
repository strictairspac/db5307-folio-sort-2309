"""Shared temporary-disk quota, leases and FIFO admission for merge runs.

A run reserves its predicted peak segment footprint before the worker starts
merging.  Reservations are leases stored in the database so that a crashed
worker's hold can be cleaned and returned exactly once.

Lock order is always the shared capacity row first, then the lease row; every
admission, refund and cleanup path follows it so they cannot deadlock.
"""

from __future__ import annotations

import os
import socket
import uuid

from sqlalchemy import case, exists, insert, select, update
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.exc import IntegrityError

from src.config.settings import engine
from src.constants.workflow import Status
from src.models.tables import audit, capacity, cases, leases, run_plans, runs
from src.services.admission import MAX_OVERTAKES, Quota, choose
from src.utils.time import utc_time

DEFAULT_LIMIT_BYTES = int(os.getenv('SORT_CAPACITY_BYTES', 8 * 1024 * 1024))
LEASE_ACTIVE = 'active'
LEASE_RELEASED = 'released'
WORKER_ID = f'{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}'


class LeaseConflict(RuntimeError):
    """The run already holds capacity that has not been returned."""


def _insert_ignore(conn, table, values: dict) -> None:
    dialect = conn.dialect.name
    if dialect == 'postgresql':
        conn.execute(postgres_insert(table).values(**values).on_conflict_do_nothing(
            index_elements=['id']))
    elif dialect in ('mysql', 'mariadb'):
        conn.execute(mysql_insert(table).prefix_with('IGNORE').values(**values))
    else:
        try:
            with conn.begin_nested():
                conn.execute(insert(table).values(**values))
        except IntegrityError:
            pass


def ensure(conn) -> None:
    _insert_ignore(conn, capacity, {
        'id': 1, 'limit_bytes': DEFAULT_LIMIT_BYTES, 'used_bytes': 0,
        'updated_at': utc_time(),
    })


def read_quota(conn) -> Quota:
    """Read the quota without writing; a missing row means the default quota."""
    row = conn.execute(select(capacity).where(capacity.c.id == 1)).mappings().first()
    if row is None:
        return Quota(DEFAULT_LIMIT_BYTES, 0)
    return Quota(row['limit_bytes'], row['used_bytes'])


def lock_quota(conn) -> Quota:
    """Create-if-needed and lock the singleton row; admission decisions serialize here."""
    ensure(conn)
    row = conn.execute(select(capacity).where(capacity.c.id == 1).with_for_update()
                       ).mappings().one()
    return Quota(row['limit_bytes'], row['used_bytes'])


def set_limit(limit_bytes: int, actor: str) -> Quota:
    """Change the quota.

    Lowering it never touches running runs, which keep their leases and finish
    normally; queued runs are re-checked against the new figure when the next
    admission decision is made.  The counter can therefore exceed a lowered
    quota until the running leases return.
    """
    with engine.begin() as conn:
        lock_quota(conn)
        conn.execute(update(capacity).where(capacity.c.id == 1).values(
            limit_bytes=limit_bytes, updated_at=utc_time()))
        conn.execute(audit.insert().values(actor=actor, action='set_capacity',
                                           reference=str(limit_bytes), created_at=utc_time()))
        return read_quota(conn)


def grant(conn, run_id: int, reserved_bytes: int, worker_id: str = WORKER_ID) -> None:
    """Reserve bytes for one run.  Caller must hold the capacity row lock."""
    existing = conn.execute(select(leases).where(leases.c.run_id == run_id)
                            .with_for_update()).mappings().first()
    if existing is not None and existing['status'] == LEASE_ACTIVE:
        raise LeaseConflict(f'执行 {run_id} 仍持有未归还的容量')
    if existing is not None:
        conn.execute(update(leases).where(leases.c.id == existing['id']).values(
            bytes=reserved_bytes, status=LEASE_ACTIVE, worker_id=worker_id,
            created_at=utc_time(), heartbeat_at=utc_time(), released_at=None,
            cleanup_attempts=0))
    else:
        conn.execute(insert(leases).values(run_id=run_id, bytes=reserved_bytes,
                                           status=LEASE_ACTIVE, worker_id=worker_id,
                                           created_at=utc_time(), heartbeat_at=utc_time()))
    conn.execute(update(capacity).where(capacity.c.id == 1).values(
        used_bytes=capacity.c.used_bytes + reserved_bytes, updated_at=utc_time()))


def release(conn, run_id: int) -> bool:
    """Return a lease's bytes exactly once.

    The capacity row is locked first (the same order admission uses, so the two
    cannot deadlock), then the lease flips to ``released`` before the counter
    moves -- a retried cleanup after a crash cannot refund the same bytes twice.
    """
    ensure(conn)
    conn.execute(select(capacity.c.id).where(capacity.c.id == 1).with_for_update()).first()
    row = conn.execute(select(leases).where(leases.c.run_id == run_id).with_for_update()
                       ).mappings().first()
    if row is None or row['status'] != LEASE_ACTIVE:
        return False
    conn.execute(update(leases).where(leases.c.id == row['id']).values(
        status=LEASE_RELEASED, released_at=utc_time()))
    conn.execute(update(capacity).where(capacity.c.id == 1).values(
        used_bytes=case((capacity.c.used_bytes - row['bytes'] < 0, 0),
                        else_=capacity.c.used_bytes - row['bytes']),
        updated_at=utc_time()))
    return True


def touch(conn, run_id: int) -> None:
    conn.execute(update(leases).where(leases.c.run_id == run_id,
                                      leases.c.status == LEASE_ACTIVE).values(
        heartbeat_at=utc_time()))


def stale_leases(conn, cutoff: str, worker_id: str = WORKER_ID) -> list[dict]:
    """Active leases owned by another worker that stopped heartbeating."""
    rows = conn.execute(select(leases, runs.c.status.label('run_status'),
                               runs.c.attempt_token)
                        .join(runs, runs.c.id == leases.c.run_id)
                        .where(leases.c.status == LEASE_ACTIVE,
                               leases.c.worker_id != worker_id,
                               leases.c.heartbeat_at < cutoff)).mappings().all()
    return [dict(row) for row in rows]


def orphan_leases(conn) -> list[dict]:
    """Active leases this worker owns whose run is already terminal."""
    rows = conn.execute(select(leases).join(runs, runs.c.id == leases.c.run_id)
                        .where(leases.c.status == LEASE_ACTIVE,
                               leases.c.worker_id == WORKER_ID,
                               runs.c.status.in_([Status.COMPLETE, Status.FAILED]))).mappings().all()
    return [dict(row) for row in rows]


def running_without_lease(conn, cutoff: str) -> list[dict]:
    """Runs marked running that hold no active lease.

    Admission grants the lease in the same transaction that marks the run
    running, so a running run with no lease is a stranded record (for example a
    failed finish followed by a successful refund), not a live batch.
    """
    held = exists().where(leases.c.run_id == runs.c.id,
                          leases.c.status == LEASE_ACTIVE)
    rows = conn.execute(select(runs.c.id, runs.c.attempt_token).where(
        runs.c.status == Status.RUNNING, runs.c.started_at < cutoff, ~held
    ).order_by(runs.c.id)).mappings().all()
    return [dict(row) for row in rows]


def note_cleanup_attempt(conn, run_id: int) -> None:
    conn.execute(update(leases).where(leases.c.run_id == run_id).values(
        cleanup_attempts=leases.c.cleanup_attempts + 1))


def active_leases(conn) -> list[dict]:
    rows = conn.execute(select(leases).where(leases.c.status == LEASE_ACTIVE)
                        .order_by(leases.c.run_id)).mappings().all()
    return [dict(row) for row in rows]


def queue(conn) -> list[dict]:
    rows = conn.execute(select(runs.c.id, run_plans.c.peak_bytes, run_plans.c.overtakes,
                               runs.c.case_id, runs.c.created_at)
                        .join(run_plans, run_plans.c.run_id == runs.c.id)
                        .where(runs.c.status == Status.QUEUED).order_by(runs.c.id)).mappings().all()
    return [dict(row) for row in rows]


def unplanned(conn) -> list[dict]:
    """Queued runs created before disk plans were recorded (upgrade backfill)."""
    rows = conn.execute(select(runs.c.id, runs.c.case_id, cases.c.payload)
                        .join(cases, cases.c.id == runs.c.case_id)
                        .outerjoin(run_plans, run_plans.c.run_id == runs.c.id)
                        .where(runs.c.status == Status.QUEUED,
                               run_plans.c.run_id.is_(None))).mappings().all()
    return [dict(row) for row in rows]


def record_plan(conn, run_id: int, peak_bytes: int, initial_runs: int) -> None:
    existing = conn.execute(select(run_plans.c.run_id).where(
        run_plans.c.run_id == run_id)).first()
    if existing is None:
        conn.execute(insert(run_plans).values(run_id=run_id, peak_bytes=peak_bytes,
                                             initial_runs=initial_runs, overtakes=0,
                                             created_at=utc_time()))


def reserve_next(conn, worker_id: str = WORKER_ID) -> tuple[int, str, int] | None:
    """Reserve capacity and mark one queued run running, atomically."""
    quota = lock_quota(conn)
    chosen, overtaken = choose(queue(conn), quota.limit_bytes, quota.used_bytes)
    if chosen is None:
        return None
    if overtaken is not None:
        conn.execute(update(run_plans).where(run_plans.c.run_id == overtaken).values(
            overtakes=run_plans.c.overtakes + 1))
    token = str(uuid.uuid4())
    changed = conn.execute(update(runs).where(
        runs.c.id == chosen['id'], runs.c.status == Status.QUEUED).values(
        status=Status.RUNNING, started_at=utc_time(), attempt_token=token,
        finished_at=None, error=None)).rowcount
    if not changed:
        return None
    grant(conn, chosen['id'], chosen['peak_bytes'], worker_id)
    conn.execute(audit.insert().values(actor='worker', action='admit',
                                       reference=str(chosen['id']), created_at=utc_time()))
    return chosen['id'], token, chosen['peak_bytes']


def payload_for(run_id: int, token: str) -> object | None:
    """Load the payload for a reserved run, or ``None`` if it is no longer ours."""
    with engine.connect() as conn:
        row = conn.execute(select(runs.c.case_id).where(
            runs.c.id == run_id, runs.c.status == Status.RUNNING,
            runs.c.attempt_token == token)).first()
        if row is None:
            return None
        case = conn.execute(select(cases.c.payload).where(cases.c.id == row.case_id)).first()
    return case.payload if case is not None else None


def state() -> dict:
    with engine.connect() as conn:
        quota = read_quota(conn)
        queued = queue(conn)
        active = active_leases(conn)
    waiting = []
    for position, row in enumerate(queued):
        if row['peak_bytes'] > quota.limit_bytes:
            reason = '超出共享容量上限，等待调高额度'
        elif row['peak_bytes'] > quota.available_bytes:
            reason = '等待余量'
        elif position == 0 and row['overtakes'] >= MAX_OVERTAKES:
            reason = '排在队首，等待余量或调高额度'
        else:
            reason = '等待排位'
        waiting.append({**row, 'reason': reason})
    return {
        **quota.as_dict(),
        'worker_id': WORKER_ID,
        'max_overtakes': MAX_OVERTAKES,
        'queued': waiting,
        'leases': [{'run_id': row['run_id'], 'bytes': row['bytes'],
                    'created_at': row['created_at']} for row in active],
    }
