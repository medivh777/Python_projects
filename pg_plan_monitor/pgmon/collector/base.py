"""Базовый класс коллектора: поток с интервалом и защитой от падений."""

from __future__ import annotations

import logging
import threading
import time

from ..config import Config
from ..pg_client import ReconnectingConn

log = logging.getLogger(__name__)


class Collector(threading.Thread):
    """Периодически вызывает collect(); ошибки логируются, поток живёт дальше."""

    interval_key = ""   # ключ в config.intervals

    def __init__(self, cfg: Config, cluster: dict, stop_event: threading.Event):
        super().__init__(daemon=True, name=f"{type(self).__name__}:{cluster['name']}")
        self.cfg = cfg
        self.cluster = cluster
        self.cluster_name = cluster["name"]
        self.stop_event = stop_event
        self.conn = ReconnectingConn(cluster["dsn"])

    @property
    def interval(self) -> float:
        return float(self.cfg.intervals[self.interval_key])

    def collect(self) -> None:  # pragma: no cover - переопределяется
        raise NotImplementedError

    def run(self) -> None:
        log.info("%s: старт, интервал %.0fс", self.name, self.interval)
        while not self.stop_event.is_set():
            started = time.monotonic()
            try:
                self.collect()
            except Exception:
                log.exception("%s: ошибка итерации", self.name)
            elapsed = time.monotonic() - started
            self.stop_event.wait(max(0.5, self.interval - elapsed))
        self.conn.close()
