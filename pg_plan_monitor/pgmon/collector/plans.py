"""Снятие планов выполнения топ-N запросов и алертинг при смене плана.

Механика:
  * из pg_stat_statements берутся top_n самых дорогих запросов;
  * для каждого выполняется EXPLAIN (FORMAT JSON) в БД запроса,
    в транзакции с ROLLBACK и statement_timeout;
  * параметризованные запросы ($1, $2 …) планируются через
    EXPLAIN (GENERIC_PLAN) — доступно с PostgreSQL 16;
  * план сохраняется в ClickHouse только если его фингерпринт изменился
    (плюс первый снятый план запроса);
  * при изменении фингерпринта создаются алерты (index → seqscan и т.д.).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import psycopg

from .. import pg_client, storage
from ..analysis.plandiff import PlanInfo, analyze_plan, compare_plans
from .base import Collector

log = logging.getLogger(__name__)

PLAN_COLUMNS = [
    "ts", "cluster", "queryid", "datname", "fingerprint", "plan_json",
    "total_cost", "startup_cost", "tables", "indexes", "seq_scan_tables",
    "node_types", "source",
]
ALERT_COLUMNS = [
    "ts", "cluster", "queryid", "datname", "severity", "kind", "description",
    "old_fingerprint", "new_fingerprint", "old_cost", "new_cost",
]

PARAM_MARKER = "$1"


class PlansCollector(Collector):
    interval_key = "plans"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # (datname, queryid) -> последний PlanInfo
        self._last: dict[tuple[str, int], PlanInfo] = {}
        self._db_conns: dict[str, pg_client.ReconnectingConn] = {}
        self._server_version: int | None = None
        self._warmed_up = False

    # -- вспомогательное --------------------------------------------------

    def _version(self) -> int:
        if self._server_version is None:
            self._server_version = int(
                self.conn.query(pg_client.SQL_SERVER_VERSION)[0]["v"]
            )
        return self._server_version

    def _db_conn(self, datname: str) -> pg_client.ReconnectingConn:
        if datname not in self._db_conns:
            self._db_conns[datname] = pg_client.ReconnectingConn(
                self.cluster["dsn"], dbname=datname
            )
        return self._db_conns[datname]

    def _allowed_db(self, datname: str) -> bool:
        wanted = self.cluster.get("databases") or []
        return not wanted or datname in wanted

    def _warm_up(self) -> None:
        """После рестарта коллектора поднимаем последние фингерпринты из
        ClickHouse, чтобы не заспамить алертами «первый план»."""
        cli = storage.thread_client(self.cfg)
        res = cli.query(
            """
            SELECT datname, queryid,
                   argMax(fingerprint, ts) AS fp,
                   argMax(plan_json, ts)   AS plan_json
            FROM query_plans
            WHERE cluster = %(cluster)s
            GROUP BY datname, queryid
            """,
            parameters={"cluster": self.cluster_name},
        )
        for datname, queryid, _fp, plan_json in res.result_rows:
            try:
                self._last[(datname, int(queryid))] = analyze_plan(json.loads(plan_json))
            except (json.JSONDecodeError, KeyError, TypeError):
                continue
        log.info("plans: восстановлено %d фингерпринтов из ClickHouse", len(self._last))

    def _explain(self, datname: str, query: str) -> tuple[dict | None, str]:
        """Возвращает (plan_json, source) или (None, причина)."""
        parametrized = PARAM_MARKER in query
        if parametrized:
            if not self.cfg.get("plans", "use_generic_plan", default=True):
                return None, "parametrized"
            if self._version() < 160000:
                return None, "parametrized_old_pg"
            explain_sql = f"EXPLAIN (GENERIC_PLAN, FORMAT JSON) {query}"
            source = "generic_plan"
        else:
            explain_sql = f"EXPLAIN (FORMAT JSON) {query}"
            source = "explain"

        timeout = int(self.cfg.get("plans", "statement_timeout_ms", default=3000))
        conn = self._db_conn(datname).get()
        try:
            with pg_client.explain_tx(conn, timeout) as cur:
                cur.execute(explain_sql)
                row = cur.fetchone()
            payload = row["QUERY PLAN"] if isinstance(row, dict) else row[0]
            if isinstance(payload, str):
                payload = json.loads(payload)
            return payload, source
        except psycopg.Error as exc:
            return None, f"error:{type(exc).__name__}"

    # -- основной цикл -----------------------------------------------------

    def collect(self) -> None:
        if not self._warmed_up:
            try:
                self._warm_up()
            except Exception:
                log.exception("plans: не удалось восстановить фингерпринты")
            self._warmed_up = True

        top = self.conn.query(pg_client.SQL_TOP_QUERIES, {
            "min_calls": int(self.cfg.get("plans", "min_calls", default=5)),
            "top_n": int(self.cfg.get("plans", "top_n", default=50)),
        })
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        cost_ratio = float(self.cfg.get("alerts", "cost_ratio_warning", default=5.0))

        plan_rows: list[list] = []
        alert_rows: list[list] = []
        skipped: dict[str, int] = {}

        for q in top:
            datname, queryid, query = q["datname"], int(q["queryid"]), q["query"]
            if not self._allowed_db(datname):
                continue
            plan_json, source = self._explain(datname, query)
            if plan_json is None:
                skipped[source] = skipped.get(source, 0) + 1
                continue

            info = analyze_plan(plan_json)
            key = (datname, queryid)
            prev = self._last.get(key)

            if prev is not None and prev.fingerprint == info.fingerprint:
                continue  # план не менялся — не храним дубликат

            plan_rows.append([
                now, self.cluster_name, queryid, datname, info.fingerprint,
                json.dumps(plan_json, ensure_ascii=False), info.total_cost,
                info.startup_cost, info.tables, info.indexes,
                info.seq_scan_tables, info.node_types, source,
            ])

            if prev is not None:
                for ch in compare_plans(prev, info, cost_ratio):
                    alert_rows.append([
                        now, self.cluster_name, queryid, datname,
                        ch.severity, ch.kind, ch.description,
                        prev.fingerprint, info.fingerprint,
                        prev.total_cost, info.total_cost,
                    ])

            self._last[key] = info

        storage.insert_rows(self.cfg, "query_plans", PLAN_COLUMNS, plan_rows)
        storage.insert_rows(self.cfg, "plan_alerts", ALERT_COLUMNS, alert_rows)
        if plan_rows or alert_rows:
            log.info("plans: %d новых планов, %d алертов (пропущено: %s)",
                     len(plan_rows), len(alert_rows), skipped or "—")
