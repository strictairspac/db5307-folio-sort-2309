"""Detect and remove segment files left behind by an interrupted worker."""

from __future__ import annotations

import fcntl
import shutil
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Iterator

PREFIX = 'folio-sort-'
LIVE_MARKER = 'live.lock'


def run_dirs(root: str | Path, run_id: int) -> list[Path]:
    """All temp dirs whose name belongs to ``run_id``, live or not."""
    base = Path(root)
    try:
        return [path for path in sorted(base.glob(f'{PREFIX}{run_id}-*')) if path.is_dir()]
    except OSError:
        return []


def hold_live(path: Path) -> IO[str]:
    """Lock this process's temp dir; keep the handle alive while sorting."""
    handle = (path / LIVE_MARKER).open('a+')
    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    return handle


@contextmanager
def claimed(root: str | Path, path: Path) -> Iterator[bool]:
    """Become the exclusive owner of ``path`` for the duration of the block.

    Yields ``True`` only when the directory is inside ``root``, named like a
    work dir, still present, and no live process holds its lock.  The lock is
    held while the caller deletes, so two recovering workers cannot clean the
    same directory concurrently.
    """
    handle = None
    try:
        try:
            handle = (path / LIVE_MARKER).open('a+')
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            yield False
            return
        yield _confined(root, path) and path.is_dir()
    finally:
        if handle is not None:
            handle.close()


def _confined(root: str | Path, path: Path) -> bool:
    base = Path(root).resolve()
    try:
        target = path.resolve()
    except OSError:
        return False
    return target.parent == base and target.name.startswith(PREFIX)


def remove(root: str | Path, path: Path) -> None:
    """Delete one work dir.  Caller must hold its lock via :func:`claimed`."""
    if not _confined(root, path):
        raise OSError(f'拒绝清理工作目录之外的路径: {path}')
    shutil.rmtree(path)


