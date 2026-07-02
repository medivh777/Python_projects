"""Периодический пересчёт рекомендаций по индексам.

Работает целиком по данным ClickHouse — PostgreSQL не трогает:
  * Seq Scan с фильтрами из свежих планов (query_plans.plan_json);
  * размеры таблиц из table_stats;
  * использование индексов из index_stats (первый и последний снапшот).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from .. import storage
from ..analysis.plandiff import analyze_plan
from ..analysis.recommend import (
    Recommendation,
    index_columns,
    recommend_create,
    recommend_drop,
    recommend_duplicates,
)
from .base import Collector

log = logging.getLogger(__name__)

REC_COLUMNS = [
    "ts", "cluster", "kind", "datname", "tablename", "indexname",
    "columns", "reason", "ddl", "queryids",
]


class RecommenderCollector(Collector):
    interval_key = "recommend"

    def collect(self) -> None:
        cli = storage.thread_client(self.cfg)
        cluster = self.cluster_name

        # 1. Последний план каждого запроса за 7 дней → Seq Scan с фильтрами.
        plans = cli.query(
            """
            SELECT datname, queryid, argMax(plan_json, ts) AS plan_json
            FROM query_plans
            WHERE cluster = %(cluster)s AND ts > now() - INTERVAL 7 DAY
            GROUP BY datname, queryid
            """,
            parameters={"cluster": cluster},
        )
        seq_scans: list[dict] = []
        for datname, queryid, plan_json in plans.result_rows:
            try:
                info = analyze_plan(json.loads(plan_json))
            except (json.JSONDecodeError, KeyError, TypeError):
                continue
            for s in info.seq_scan_filters:
                if s.get("filter"):
                    seq_scans.append({
                        "datname": datname,
                        "table": s["table"],
                        "filter": s["filter"],
                        "queryid": int(queryid),
                    })

        # 2. Актуальные размеры таблиц.
        tbl = cli.query(
            """
            SELECT datname, relname, argMax(total_bytes, ts) AS bytes
            FROM table_stats
            WHERE cluster = %(cluster)s AND ts > now() - INTERVAL 2 DAY
            GROUP BY datname, relname
            """,
            parameters={"cluster": cluster},
        )
        table_sizes = {(d, r): int(b) for d, r, b in tbl.result_rows}

        # 3. Индексы: прирост idx_scan за весь период наблюдения + определение.
        idx = cli.query(
            """
            SELECT datname, schemaname, relname, indexrelname,
                   max(idx_scan) - min(idx_scan) AS idx_scan_delta,
                   argMax(size_bytes, ts)        AS size_bytes,
                   argMax(is_unique, ts)         AS is_unique,
                   argMax(is_primary, ts)        AS is_primary,
                   argMax(definition, ts)        AS definition,
                   count()                       AS snapshots
            FROM index_stats
            WHERE cluster = %(cluster)s
            GROUP BY datname, schemaname, relname, indexrelname
            """,
            parameters={"cluster": cluster},
        )
        index_rows: list[dict] = []
        existing: dict[tuple[str, str], list[list[str]]] = {}
        for (datname, schema, rel, idxname, delta, size, uniq, prim,
             definition, snapshots) in idx.result_rows:
            row = {
                "datname": datname, "schemaname": schema, "relname": rel,
                "indexrelname": idxname, "idx_scan_delta": int(delta),
                "size_bytes": int(size), "is_unique": int(uniq),
                "is_primary": int(prim), "definition": definition,
            }
            existing.setdefault((datname, rel), []).append(index_columns(definition))
            # для drop-рекомендаций нужно хотя бы 2 снапшота (иначе дельта всегда 0)
            if int(snapshots) >= 2:
                index_rows.append(row)

        recs: list[Recommendation] = []
        recs += recommend_create(seq_scans, table_sizes, existing)
        recs += recommend_drop(index_rows)
        recs += recommend_duplicates(index_rows)

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        rows = [[
            now, cluster, r.kind, r.datname, r.tablename, r.indexname,
            r.columns, r.reason, r.ddl, r.queryids,
        ] for r in recs]
        # ReplacingMergeTree(ts): повторный расчёт обновляет ts той же рекомендации
        storage.insert_rows(self.cfg, "recommendations", REC_COLUMNS, rows)
        log.info("recommend: %d рекомендаций (create=%d, drop=%d, dup=%d)",
                 len(recs),
                 sum(r.kind == "create_index" for r in recs),
                 sum(r.kind == "drop_index" for r in recs),
                 sum(r.kind == "duplicate_index" for r in recs))
