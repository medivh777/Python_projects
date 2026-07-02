"""Active Session History: семплирование pg_stat_activity.

Аналог pg_activity / ASH: раз в ash_sample секунд снимаются все не-idle
клиентские сессии. Выборка из pg_stat_activity читает разделяемую память —
нагрузки на диск и таблицы не создаёт. Записи буферизуются и сбрасываются
в ClickHouse пачками, чтобы не делать вставку на каждый семпл.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from .. import pg_client, storage
from .base import Collector

log = logging.getLogger(__name__)

ASH_COLUMNS = [
    "ts", "cluster", "datname", "pid", "usename", "application_name",
    "client_addr", "backend_type", "state", "wait_event_type", "wait_event",
    "queryid", "query", "query_start", "xact_start", "blocked_by",
]

FLUSH_EVERY = 10          # семплов
MAX_BUFFER_ROWS = 5000

_EPOCH = datetime(1970, 1, 1)


def _dt(value) -> datetime:
    if value is None:
        return _EPOCH
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


class AshCollector(Collector):
    interval_key = "ash_sample"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._buffer: list[list] = []
        self._samples_since_flush = 0

    def collect(self) -> None:
        rows = self.conn.query(pg_client.SQL_ACTIVITY)
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        for r in rows:
            self._buffer.append([
                now, self.cluster_name, r["datname"] or "", r["pid"],
                r["usename"] or "", r["application_name"], r["client_addr"],
                r["backend_type"], r["state"], r["wait_event_type"],
                r["wait_event"], int(r["queryid"]), r["query"][:4000],
                _dt(r["query_start"]), _dt(r["xact_start"]),
                list(r["blocked_by"] or []),
            ])

        self._samples_since_flush += 1
        if (self._samples_since_flush >= FLUSH_EVERY
                or len(self._buffer) >= MAX_BUFFER_ROWS):
            self.flush()

    def flush(self) -> None:
        if self._buffer:
            storage.insert_rows(self.cfg, "ash", ASH_COLUMNS, self._buffer)
            log.debug("ash: записано %d строк", len(self._buffer))
            self._buffer = []
        self._samples_since_flush = 0
