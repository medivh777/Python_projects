"""Снапшоты pg_stat_statements → дельты → ClickHouse.

pg_stat_statements хранит счётчики накопительным итогом; для графиков нужны
приросты за интервал. Первый снапшот после старта только запоминается.
Сброс статистики (pg_stat_statements_reset) определяется по уменьшению calls.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from .. import pg_client, storage
from .base import Collector

log = logging.getLogger(__name__)

METRIC_COLUMNS = [
    "ts", "cluster", "datname", "usename", "queryid",
    "calls", "total_exec_time", "mean_exec_time", "min_exec_time",
    "max_exec_time", "stddev_exec_time", "rows",
    "shared_blks_hit", "shared_blks_read", "shared_blks_dirtied",
    "shared_blks_written", "local_blks_read", "local_blks_written",
    "temp_blks_read", "temp_blks_written",
    "blk_read_time", "blk_write_time", "wal_bytes",
]

COUNTERS = [
    "calls", "total_exec_time", "rows",
    "shared_blks_hit", "shared_blks_read", "shared_blks_dirtied",
    "shared_blks_written", "local_blks_read", "local_blks_written",
    "temp_blks_read", "temp_blks_written",
    "blk_read_time", "blk_write_time", "wal_bytes",
]


class StatementsCollector(Collector):
    interval_key = "statements"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._prev: dict[tuple, dict] = {}

    def collect(self) -> None:
        rows = self.conn.query(pg_client.SQL_STATEMENTS)
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        metric_rows: list[list] = []
        query_rows: list[list] = []

        for r in rows:
            key = (r["datname"], r["usename"], r["queryid"])
            prev = self._prev.get(key)
            self._prev[key] = r

            query_rows.append([
                self.cluster_name, r["queryid"], r["datname"], r["usename"],
                r["query"], now,
            ])

            if prev is None:
                continue
            d_calls = r["calls"] - prev["calls"]
            if d_calls < 0:
                # был reset — текущий снапшот становится базой
                continue
            if d_calls == 0:
                continue

            delta = {c: max(0, (r[c] or 0) - (prev[c] or 0)) for c in COUNTERS}
            mean = delta["total_exec_time"] / d_calls if d_calls else 0.0
            metric_rows.append([
                now, self.cluster_name, r["datname"], r["usename"], r["queryid"],
                int(delta["calls"]), float(delta["total_exec_time"]), float(mean),
                float(r["min_exec_time"] or 0), float(r["max_exec_time"] or 0),
                float(r["stddev_exec_time"] or 0), int(delta["rows"]),
                int(delta["shared_blks_hit"]), int(delta["shared_blks_read"]),
                int(delta["shared_blks_dirtied"]), int(delta["shared_blks_written"]),
                int(delta["local_blks_read"]), int(delta["local_blks_written"]),
                int(delta["temp_blks_read"]), int(delta["temp_blks_written"]),
                float(delta["blk_read_time"]), float(delta["blk_write_time"]),
                int(delta["wal_bytes"]),
            ])

        storage.insert_rows(self.cfg, "statements_metrics", METRIC_COLUMNS, metric_rows)
        storage.insert_rows(
            self.cfg, "queries",
            ["cluster", "queryid", "datname", "usename", "query", "last_seen"],
            query_rows,
        )
        log.debug("statements: %d дельт, %d запросов", len(metric_rows), len(query_rows))
