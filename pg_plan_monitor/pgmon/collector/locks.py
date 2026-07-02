"""История блокировок: кто кого блокирует. Пишется только при наличии блокировок."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from .. import pg_client, storage
from .base import Collector

log = logging.getLogger(__name__)

LOCK_COLUMNS = [
    "ts", "cluster", "datname",
    "blocked_pid", "blocked_user", "blocked_query", "blocked_duration_s",
    "blocked_mode", "blocking_pid", "blocking_user", "blocking_query",
    "blocking_state", "blocking_mode", "lock_type", "relation",
]


class LocksCollector(Collector):
    interval_key = "locks"

    def collect(self) -> None:
        rows = self.conn.query(pg_client.SQL_LOCKS)
        if not rows:
            return
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        seen: set[tuple[int, int, str]] = set()
        out: list[list] = []
        for r in rows:
            # LEFT JOIN к pg_locks может дать дубли пары pid'ов — схлопываем
            key = (r["blocked_pid"], r["blocking_pid"], r["lock_type"])
            if key in seen:
                continue
            seen.add(key)
            out.append([
                now, self.cluster_name, r["datname"],
                r["blocked_pid"], r["blocked_user"], r["blocked_query"][:4000],
                float(r["blocked_duration_s"]), r["blocked_mode"],
                r["blocking_pid"], r["blocking_user"], r["blocking_query"][:4000],
                r["blocking_state"], r["blocking_mode"], r["lock_type"],
                r["relation"],
            ])
        storage.insert_rows(self.cfg, "locks", LOCK_COLUMNS, out)
        log.info("locks: %d блокирующих пар", len(out))
