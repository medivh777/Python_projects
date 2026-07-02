"""Снапшоты статистики таблиц и индексов по всем наблюдаемым БД кластера.

Помимо размеров и счётчиков сканирований снимаются toast-размер и
vacuum-информация (когда и сколько раз проходил vacuum/analyze).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from .. import pg_client, storage
from .base import Collector

log = logging.getLogger(__name__)

TABLE_COLUMNS = [
    "ts", "cluster", "datname", "schemaname", "relname",
    "seq_scan", "idx_scan", "n_live_tup", "n_dead_tup", "n_mod_since_analyze",
    "total_bytes", "toast_bytes",
    "last_vacuum", "last_autovacuum", "last_analyze", "last_autoanalyze",
    "vacuum_count", "autovacuum_count", "analyze_count", "autoanalyze_count",
]
INDEX_COLUMNS = [
    "ts", "cluster", "datname", "schemaname", "relname", "indexrelname",
    "idx_scan", "size_bytes", "is_unique", "is_primary", "definition",
]

_EPOCH = datetime(1970, 1, 1)


def _dt(value) -> datetime:
    if value is None:
        return _EPOCH
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


class TableStatsCollector(Collector):
    interval_key = "table_stats"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._db_conns: dict[str, pg_client.ReconnectingConn] = {}

    def _databases(self) -> list[str]:
        wanted = self.cluster.get("databases") or []
        if wanted:
            return wanted
        return [r["datname"] for r in self.conn.query(pg_client.SQL_DATABASES)]

    def _db_conn(self, datname: str) -> pg_client.ReconnectingConn:
        if datname not in self._db_conns:
            self._db_conns[datname] = pg_client.ReconnectingConn(
                self.cluster["dsn"], dbname=datname
            )
        return self._db_conns[datname]

    def collect(self) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        table_rows: list[list] = []
        index_rows: list[list] = []

        for datname in self._databases():
            try:
                conn = self._db_conn(datname)
                for r in conn.query(pg_client.SQL_TABLE_STATS):
                    table_rows.append([
                        now, self.cluster_name, datname, r["schemaname"],
                        r["relname"], int(r["seq_scan"] or 0),
                        int(r["idx_scan"] or 0), int(r["n_live_tup"] or 0),
                        int(r["n_dead_tup"] or 0),
                        int(r["n_mod_since_analyze"] or 0),
                        int(r["total_bytes"] or 0), int(r["toast_bytes"] or 0),
                        _dt(r["last_vacuum"]), _dt(r["last_autovacuum"]),
                        _dt(r["last_analyze"]), _dt(r["last_autoanalyze"]),
                        int(r["vacuum_count"] or 0), int(r["autovacuum_count"] or 0),
                        int(r["analyze_count"] or 0), int(r["autoanalyze_count"] or 0),
                    ])
                for r in conn.query(pg_client.SQL_INDEX_STATS):
                    index_rows.append([
                        now, self.cluster_name, datname, r["schemaname"],
                        r["relname"], r["indexrelname"],
                        int(r["idx_scan"] or 0), int(r["size_bytes"] or 0),
                        1 if r["is_unique"] else 0, 1 if r["is_primary"] else 0,
                        r["definition"] or "",
                    ])
            except Exception:
                log.exception("table_stats: ошибка для БД %s", datname)

        storage.insert_rows(self.cfg, "table_stats", TABLE_COLUMNS, table_rows)
        storage.insert_rows(self.cfg, "index_stats", INDEX_COLUMNS, index_rows)
        log.debug("table_stats: %d таблиц, %d индексов", len(table_rows), len(index_rows))
