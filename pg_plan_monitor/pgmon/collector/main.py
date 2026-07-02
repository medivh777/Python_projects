"""Точка входа коллектора: python -m pgmon.collector.main [config.yaml]"""

from __future__ import annotations

import logging
import signal
import sys
import threading
import time

from .. import storage
from ..config import load_config
from .activity import AshCollector
from .bloat import BloatCollector
from .locks import LocksCollector
from .plans import PlansCollector
from .recommender import RecommenderCollector
from .statements import StatementsCollector
from .sysstat import SysstatCollector
from .tables import TableStatsCollector

log = logging.getLogger(__name__)

COLLECTORS = [
    AshCollector,
    StatementsCollector,
    LocksCollector,
    SysstatCollector,
    PlansCollector,
    TableStatsCollector,
    BloatCollector,
    RecommenderCollector,
]


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    cfg = load_config(sys.argv[1] if len(sys.argv) > 1 else None)

    # ClickHouse может стартовать дольше коллектора (docker compose) — ждём.
    for attempt in range(30):
        try:
            storage.ensure_schema(cfg)
            break
        except Exception as exc:
            log.warning("ClickHouse недоступен (%s), попытка %d/30", exc, attempt + 1)
            time.sleep(2)
    else:
        log.error("ClickHouse так и не поднялся — выходим")
        sys.exit(1)

    stop = threading.Event()

    def _sigterm(_sig, _frm):
        log.info("Останавливаемся...")
        stop.set()

    signal.signal(signal.SIGTERM, _sigterm)
    signal.signal(signal.SIGINT, _sigterm)

    threads: list[threading.Thread] = []
    for cluster in cfg.clusters:
        for cls in COLLECTORS:
            t = cls(cfg, cluster, stop)
            t.start()
            threads.append(t)

    log.info("Запущено %d коллекторов для %d кластера(ов)",
             len(threads), len(cfg.clusters))
    while not stop.is_set():
        stop.wait(1.0)
    for t in threads:
        t.join(timeout=10)


if __name__ == "__main__":
    main()
