"""Оценка bloat таблиц и btree-индексов.

Использует оценочные запросы по статистике планировщика (ioguix) —
данные таблиц не читаются, поэтому запросы дёшевы, но результат
приблизительный. Интервал по умолчанию — час: bloat меняется медленно.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from .. import pg_client, storage
from .base import Collector

log = logging.getLogger(__name__)

COLUMNS = [
    "ts", "cluster", "datname", "schemaname", "relname", "indexrelname",
    "kind", "real_bytes", "bloat_bytes", "bloat_pct",
]


class BloatCollector(Collector):
    interval_key = "bloat"

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
        rows: list[list] = []

        for datname in self._databases():
            try:
                conn = self._db_conn(datname)
                for r in conn.query(pg_client.SQL_TABLE_BLOAT):
                    rows.append([
                        now, self.cluster_name, datname, r["schemaname"],
                        r["relname"], "", "table",
                        int(r["real_bytes"] or 0), int(r["bloat_bytes"] or 0),
                        float(r["bloat_pct"] or 0),
                    ])
                for r in conn.query(pg_client.SQL_INDEX_BLOAT):
                    rows.append([
                        now, self.cluster_name, datname, r["schemaname"],
                        r["relname"], r["indexrelname"], "index",
                        int(r["real_bytes"] or 0), int(r["bloat_bytes"] or 0),
                        float(r["bloat_pct"] or 0),
                    ])
            except Exception:
                log.exception("bloat: ошибка для БД %s", datname)

        storage.insert_rows(self.cfg, "bloat_stats", COLUMNS, rows)
        log.debug("bloat: %d объектов", len(rows))
