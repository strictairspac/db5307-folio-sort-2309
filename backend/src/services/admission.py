"""Pure admission policy for the shared merge-segment capacity.

Kept free of database imports so the fairness rules can be exercised and
reviewed on their own.
"""

from __future__ import annotations

MAX_OVERTAKES = 2


class Quota:
    def __init__(self, limit_bytes: int, used_bytes: int):
        self.limit_bytes = max(0, int(limit_bytes))
        self.used_bytes = max(0, int(used_bytes))

    @property
    def available_bytes(self) -> int:
        return max(0, self.limit_bytes - self.used_bytes)

    def as_dict(self) -> dict[str, int]:
        return {'limit_bytes': self.limit_bytes, 'used_bytes': self.used_bytes,
                'available_bytes': self.available_bytes}


def choose(queue_rows: list[dict], limit_bytes: int, used_bytes: int) -> tuple[dict | None, int | None]:
    """Pick the next queued run, FIFO with bounded overtaking.

    ``queue_rows`` is ordered by submission; each row carries ``id``,
    ``peak_bytes`` (the old and new segments that coexist at the run's peak) and
    ``overtakes`` (how many times it has already been passed over).

    * The head runs whenever its whole peak fits the free space.
    * If the head does not fit, a later batch that fits may borrow the leftover
      space.  Each such borrow increments the head's overtake count.
    * After ``MAX_OVERTAKES`` borrows the head is not passed over again, so one
      large batch is overtaken at most twice; then it holds the queue until
      capacity returns or the quota is raised.  A batch larger than the whole
      quota therefore waits for a raise but cannot be starved by an endless
      stream of small batches.
    """
    if not queue_rows:
        return None, None
    available = max(0, limit_bytes - used_bytes)
    head = queue_rows[0]
    if head['peak_bytes'] <= available:
        return head, None
    if head['overtakes'] >= MAX_OVERTAKES:
        return None, None
    for candidate in queue_rows[1:]:
        if candidate['peak_bytes'] <= available:
            return candidate, head['id']
    return None, None
