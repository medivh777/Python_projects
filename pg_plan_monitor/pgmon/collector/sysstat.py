"""Нагрузка: дельты pg_stat_database + счётчики сессий + метрики хоста.

Метрики хоста (CPU/RAM/диск через psutil) включаются host_metrics: true и имеют
смысл, только когда коллектор работает на том же сервере, что и PostgreSQL.
Без них нагрузка оценивается по данным самой БД: blk_read_time/blk_write_time
(диск), active backends (CPU-прокси), temp_bytes (spill на диск).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from .. import pg_client, storage
from .base import Collector

log = logging.getLogger(__name__)

COLUMNS = [
    "ts", "cluster", "active_backends", "idle_in_xact", "waiting_backends",
    "total_backends", "xact_commit", "xact_rollback", "blks_read", "blks_hit",
    "tup_returned", "tup_fetched", "tup_inserted", "tup_updated", "tup_deleted",
    "temp_bytes", "deadlocks", "blk_read_time", "blk_write_time",
    "host_cpu_percent", "host_mem_percent", "host_read_bytes",
    "host_write_bytes", "host_load1",
]

DB_COUNTERS = [
    "xact_commit", "xact_rollback", "blks_read", "blks_hit",
    "tup_returned", "tup_fetched", "tup_inserted", "tup_updated",
    "tup_deleted", "temp_bytes", "deadlocks", "blk_read_time", "blk_write_time",
]


class SysstatCollector(Collector):
    interval_key = "sysstat"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._prev_db: dict | None = None
        self._prev_io: tuple[int, int] | None = None
        self._psutil = None
        if self.cfg.get("host_metrics", default=False):
            try:
                import psutil
                self._psutil = psutil
                psutil.cpu_percent(interval=None)  # инициализация счётчика
            except ImportError:
                log.warning("host_metrics включён, но psutil не установлен")

    def _host_metrics(self) -> tuple[float, float, int, int, float]:
        if self._psutil is None:
            return 0.0, 0.0, 0, 0, 0.0
        ps = self._psutil
        cpu = ps.cpu_percent(interval=None)
        mem = ps.virtual_memory().percent
        io = ps.disk_io_counters()
        read_b, write_b = (io.read_bytes, io.write_bytes) if io else (0, 0)
        d_read, d_write = 0, 0
        if self._prev_io is not None:
            d_read = max(0, read_b - self._prev_io[0])
            d_write = max(0, write_b - self._prev_io[1])
        self._prev_io = (read_b, write_b)
        try:
            load1 = ps.getloadavg()[0]
        except (OSError, AttributeError):
            load1 = 0.0
        return float(cpu), float(mem), d_read, d_write, float(load1)

    def collect(self) -> None:
        db = self.conn.query(pg_client.SQL_DB_STATS)[0]
        counts = self.conn.query(pg_client.SQL_BACKEND_COUNTS)[0]
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        prev = self._prev_db
        self._prev_db = db
        if prev is None:
            return

        delta = {c: max(0, float(db[c] or 0) - float(prev[c] or 0)) for c in DB_COUNTERS}
        cpu, mem, d_read, d_write, load1 = self._host_metrics()

        storage.insert_rows(self.cfg, "sysstat", COLUMNS, [[
            now, self.cluster_name,
            int(counts["active"]), int(counts["idle_in_xact"]),
            int(counts["waiting"]), int(counts["total"]),
            int(delta["xact_commit"]), int(delta["xact_rollback"]),
            int(delta["blks_read"]), int(delta["blks_hit"]),
            int(delta["tup_returned"]), int(delta["tup_fetched"]),
            int(delta["tup_inserted"]), int(delta["tup_updated"]),
            int(delta["tup_deleted"]), int(delta["temp_bytes"]),
            int(delta["deadlocks"]),
            float(delta["blk_read_time"]), float(delta["blk_write_time"]),
            cpu, mem, d_read, d_write, load1,
        ]])
