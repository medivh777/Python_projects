"""JSON API. Все данные читаются из ClickHouse — PostgreSQL не нагружается."""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from .. import storage
from ..analysis.plandiff import analyze_plan, compare_plans
from ..config import load_config

router = APIRouter(prefix="/api")

_cfg = None


def cfg():
    global _cfg
    if _cfg is None:
        _cfg = load_config()
    return _cfg


def q(sql: str, params: dict | None = None) -> list[dict[str, Any]]:
    cli = storage.thread_client(cfg())
    res = cli.query(sql, parameters=params or {})
    cols = res.column_names
    return [dict(zip(cols, row)) for row in res.result_rows]


def str_qids(rows: list[dict]) -> list[dict]:
    """queryid — Int64 и не помещается в Number JS (2^53): отдаём строкой."""
    for r in rows:
        if "queryid" in r:
            r["queryid"] = str(r["queryid"])
        if "queryids" in r:
            r["queryids"] = [str(x) for x in r["queryids"]]
    return rows


def bucket_minutes(hours: int) -> int:
    if hours <= 6:
        return 5
    if hours <= 24:
        return 10
    if hours <= 72:
        return 30
    if hours <= 168:
        return 60
    return 360


# ---------------------------------------------------------------------------
# Обзор / графики statements (как в temboard)
# ---------------------------------------------------------------------------

@router.get("/overview")
def overview(hours: int = Query(6, ge=1, le=720)) -> dict:
    step = bucket_minutes(hours)
    series = q(f"""
        SELECT toStartOfInterval(ts, INTERVAL {step} MINUTE) AS t,
               avg(active_backends)   AS active,
               avg(idle_in_xact)      AS idle_in_xact,
               avg(waiting_backends)  AS waiting,
               sum(xact_commit)       AS commits,
               sum(xact_rollback)     AS rollbacks,
               sum(blks_read)         AS blks_read,
               sum(blks_hit)          AS blks_hit,
               sum(temp_bytes)        AS temp_bytes,
               sum(blk_read_time)     AS blk_read_time,
               sum(blk_write_time)    AS blk_write_time,
               avg(host_cpu_percent)  AS cpu,
               avg(host_mem_percent)  AS mem,
               sum(host_read_bytes)   AS host_read_bytes,
               sum(host_write_bytes)  AS host_write_bytes
        FROM sysstat
        WHERE ts > now() - INTERVAL %(hours)s HOUR
        GROUP BY t ORDER BY t
    """, {"hours": hours})

    stmt = q(f"""
        SELECT toStartOfInterval(ts, INTERVAL {step} MINUTE) AS t,
               sum(calls)                     AS calls,
               sum(total_exec_time)           AS total_time,
               total_time / greatest(calls,1) AS mean_time
        FROM statements_metrics
        WHERE ts > now() - INTERVAL %(hours)s HOUR
        GROUP BY t ORDER BY t
    """, {"hours": hours})

    totals = q("""
        SELECT count(DISTINCT queryid)        AS queries,
               sum(calls)                     AS calls,
               sum(total_exec_time)           AS total_time,
               total_time / greatest(calls,1) AS mean_time
        FROM statements_metrics
        WHERE ts > now() - INTERVAL %(hours)s HOUR
    """, {"hours": hours})

    alerts = q("""
        SELECT severity, count() AS cnt
        FROM plan_alerts
        WHERE ts > now() - INTERVAL 24 HOUR
        GROUP BY severity
    """)

    host_metrics = bool(cfg().get("host_metrics", default=False))
    return {
        "series": series, "statements": stmt,
        "totals": totals[0] if totals else {},
        "alerts_24h": {a["severity"]: a["cnt"] for a in alerts},
        "host_metrics": host_metrics,
    }


# ---------------------------------------------------------------------------
# Запросы
# ---------------------------------------------------------------------------

@router.get("/queries")
def queries(hours: int = Query(24, ge=1, le=720),
            search: str = "",
            order: str = Query("total_time", pattern="^(total_time|calls|mean_time|max_time|blks_read)$"),
            limit: int = Query(100, ge=1, le=1000)) -> list[dict]:
    # серверный поиск по телу запроса: сначала находим подходящие queryid
    search_filter = ""
    params: dict[str, Any] = {"hours": hours, "limit": limit}
    if search.strip():
        found = q("""
            SELECT DISTINCT queryid FROM queries
            WHERE positionCaseInsensitive(query, %(s)s) > 0
            LIMIT 2000
        """, {"s": search.strip()})
        if not found:
            return []
        search_filter = "AND m.queryid IN %(qids)s"
        params["qids"] = [r["queryid"] for r in found]

    rows = q(f"""
        SELECT m.queryid                                       AS queryid,
               any(m.datname)                                  AS datname,
               any(m.usename)                                  AS usename,
               sum(m.calls)                   AS calls,
               sum(m.total_exec_time)         AS total_time,
               total_time / greatest(calls,1) AS mean_time,
               max(m.max_exec_time)                            AS max_time,
               sum(m.rows)                                     AS rows,
               sum(m.shared_blks_read)                         AS blks_read,
               sum(m.temp_blks_written)                        AS temp_blks
        FROM statements_metrics m
        WHERE m.ts > now() - INTERVAL %(hours)s HOUR {search_filter}
        GROUP BY m.queryid
        ORDER BY {order} DESC
        LIMIT %(limit)s
    """, params)

    if not rows:
        return []
    ids = [r["queryid"] for r in rows]
    texts = q("""
        SELECT queryid, argMax(query, last_seen) AS query
        FROM queries
        WHERE queryid IN %(ids)s
        GROUP BY queryid
    """, {"ids": ids})
    text_by_id = {t["queryid"]: t["query"] for t in texts}

    plans = q("""
        SELECT queryid, uniqExact(fingerprint) AS plan_count,
               argMax(fingerprint, ts) AS last_fingerprint
        FROM query_plans
        WHERE queryid IN %(ids)s
        GROUP BY queryid
    """, {"ids": ids})
    plan_by_id = {p["queryid"]: p for p in plans}

    out = []
    for r in rows:
        p = plan_by_id.get(r["queryid"], {})
        r["query"] = text_by_id.get(r["queryid"], "")
        r["plan_count"] = p.get("plan_count", 0)
        r["last_fingerprint"] = p.get("last_fingerprint", "")
        out.append(r)
    return str_qids(out)


@router.get("/query/{queryid}")
def query_detail(queryid: int, hours: int = Query(24, ge=1, le=720)) -> dict:
    step = bucket_minutes(hours)

    meta = q("""
        SELECT argMax(qq.query, qq.last_seen)   AS query,
               argMax(qq.datname, qq.last_seen) AS datname,
               argMax(qq.usename, qq.last_seen) AS usename,
               max(qq.last_seen)                AS last_seen
        FROM queries qq WHERE qq.queryid = %(qid)s GROUP BY qq.queryid
    """, {"qid": queryid})
    if not meta:
        raise HTTPException(404, "queryid не найден")

    series = q(f"""
        SELECT toStartOfInterval(ts, INTERVAL {step} MINUTE) AS t,
               sum(calls)                                    AS calls,
               sum(total_exec_time)                          AS total_time,
               total_time / greatest(calls,1)                AS mean_time,
               max(max_exec_time)                            AS max_time,
               sum(rows)                                     AS rows,
               sum(shared_blks_read)                         AS blks_read,
               sum(shared_blks_hit)                          AS blks_hit,
               sum(temp_blks_written)                        AS temp_blks_written,
               sum(blk_read_time)                            AS blk_read_time,
               sum(wal_bytes)                                AS wal_bytes
        FROM statements_metrics
        WHERE queryid = %(qid)s AND ts > now() - INTERVAL %(hours)s HOUR
        GROUP BY t ORDER BY t
    """, {"qid": queryid, "hours": hours})

    totals = q("""
        SELECT sum(calls) AS calls,
               sum(total_exec_time) AS total_time,
               total_time / greatest(calls,1) AS mean_time,
               max(max_exec_time) AS max_time,
               sum(shared_blks_read) AS blks_read,
               sum(shared_blks_hit)  AS blks_hit
        FROM statements_metrics
        WHERE queryid = %(qid)s AND ts > now() - INTERVAL %(hours)s HOUR
    """, {"qid": queryid, "hours": hours})

    plans = q("""
        SELECT ts, fingerprint, total_cost, tables, indexes,
               seq_scan_tables, node_types, source
        FROM query_plans
        WHERE queryid = %(qid)s
        ORDER BY ts DESC LIMIT 50
    """, {"qid": queryid})

    alerts = q("""
        SELECT ts, severity, kind, description, old_fingerprint, new_fingerprint
        FROM plan_alerts
        WHERE queryid = %(qid)s
        ORDER BY ts DESC LIMIT 50
    """, {"qid": queryid})

    # запросы, работающие с теми же таблицами
    tables: list[str] = plans[0]["tables"] if plans else []
    related: list[dict] = []
    if tables:
        related = q("""
            SELECT DISTINCT p.queryid AS queryid,
                   argMax(q.query, q.last_seen) AS query
            FROM query_plans p
            LEFT JOIN queries q ON q.queryid = p.queryid
            WHERE hasAny(p.tables, %(tables)s) AND p.queryid != %(qid)s
            GROUP BY p.queryid
            LIMIT 30
        """, {"tables": tables, "qid": queryid})

    return {
        "queryid": str(queryid),
        "meta": meta[0],
        "totals": totals[0] if totals else {},
        "series": series,
        "plans": plans,
        "alerts": alerts,
        "tables": tables,
        "indexes": plans[0]["indexes"] if plans else [],
        "related": str_qids(related),
    }


@router.get("/query/{queryid}/plan/{fingerprint}")
def plan_json(queryid: int, fingerprint: str) -> dict:
    rows = q("""
        SELECT ts, plan_json, total_cost, source
        FROM query_plans
        WHERE queryid = %(qid)s AND fingerprint = %(fp)s
        ORDER BY ts DESC LIMIT 1
    """, {"qid": queryid, "fp": fingerprint})
    if not rows:
        raise HTTPException(404, "план не найден")
    r = rows[0]
    return {"ts": r["ts"], "total_cost": r["total_cost"], "source": r["source"],
            "plan": json.loads(r["plan_json"])}


@router.get("/query/{queryid}/diff")
def plan_diff(queryid: int, old: str, new: str) -> dict:
    """Сравнение двух планов запроса по фингерпринтам."""
    def load(fp: str) -> dict:
        rows = q("""
            SELECT plan_json FROM query_plans
            WHERE queryid = %(qid)s AND fingerprint = %(fp)s
            ORDER BY ts DESC LIMIT 1
        """, {"qid": queryid, "fp": fp})
        if not rows:
            raise HTTPException(404, f"план {fp} не найден")
        return json.loads(rows[0]["plan_json"])

    old_json, new_json = load(old), load(new)
    old_info, new_info = analyze_plan(old_json), analyze_plan(new_json)
    changes = compare_plans(old_info, new_info,
                            float(cfg().get("alerts", "cost_ratio_warning", default=5.0)))
    return {
        "old": {"fingerprint": old, "plan": old_json, "cost": old_info.total_cost},
        "new": {"fingerprint": new, "plan": new_json, "cost": new_info.total_cost},
        "changes": [c.__dict__ for c in changes],
    }


# ---------------------------------------------------------------------------
# Поиск планов по телу запроса
# ---------------------------------------------------------------------------

@router.get("/plans/search")
def plans_search(text: str = Query("", alias="q"),
                 days: int = Query(7, ge=1, le=90),
                 limit: int = Query(20, ge=1, le=100)) -> list[dict]:
    """Ищет запросы по подстроке текста и возвращает версии их планов
    за период (последний план — первым). Если в периоде смен не было,
    возвращается последний известный план запроса."""
    if not text.strip():
        return []
    hits = q("""
        SELECT qq.queryid AS queryid,
               argMax(qq.query, qq.last_seen)   AS query,
               argMax(qq.datname, qq.last_seen) AS datname,
               max(qq.last_seen)                AS last_seen
        FROM queries qq
        WHERE positionCaseInsensitive(qq.query, %(s)s) > 0
        GROUP BY qq.queryid
        ORDER BY last_seen DESC
        LIMIT %(lim)s
    """, {"s": text.strip(), "lim": limit})

    out = []
    for h in hits:
        plans = q("""
            SELECT ts, fingerprint, total_cost, tables, indexes,
                   seq_scan_tables, source
            FROM query_plans
            WHERE queryid = %(qid)s AND ts > now() - INTERVAL %(days)s DAY
            ORDER BY ts DESC LIMIT 50
        """, {"qid": h["queryid"], "days": days})
        if not plans:
            # смен плана в периоде не было — показываем последний известный
            plans = q("""
                SELECT ts, fingerprint, total_cost, tables, indexes,
                       seq_scan_tables, source
                FROM query_plans
                WHERE queryid = %(qid)s
                ORDER BY ts DESC LIMIT 1
            """, {"qid": h["queryid"]})
        h["plans"] = plans
        out.append(h)
    return str_qids(out)


# ---------------------------------------------------------------------------
# Statements: графики как в temboard
# ---------------------------------------------------------------------------

@router.get("/statements")
def statements(hours: int = Query(24, ge=1, le=720),
               top: int = Query(5, ge=1, le=8)) -> dict:
    step = bucket_minutes(hours)

    series = q(f"""
        SELECT toStartOfInterval(ts, INTERVAL {step} MINUTE) AS t,
               sum(calls)                     AS calls,
               sum(total_exec_time)           AS total_time,
               total_time / greatest(calls,1) AS mean_time,
               sum(rows)                      AS rows,
               sum(shared_blks_read)          AS blks_read,
               sum(shared_blks_hit)           AS blks_hit,
               sum(temp_blks_written)         AS temp_blks_written,
               sum(blk_read_time)             AS blk_read_time,
               sum(blk_write_time)            AS blk_write_time,
               sum(wal_bytes)                 AS wal_bytes
        FROM statements_metrics
        WHERE ts > now() - INTERVAL %(hours)s HOUR
        GROUP BY t ORDER BY t
    """, {"hours": hours})

    top_rows = q("""
        SELECT queryid, sum(total_exec_time) AS total_time
        FROM statements_metrics
        WHERE ts > now() - INTERVAL %(hours)s HOUR
        GROUP BY queryid ORDER BY total_time DESC LIMIT %(top)s
    """, {"hours": hours, "top": top})
    top_ids = [r["queryid"] for r in top_rows]

    per_query: list[dict] = []
    if top_ids:
        texts = q("""
            SELECT queryid, argMax(query, last_seen) AS query
            FROM queries WHERE queryid IN %(ids)s GROUP BY queryid
        """, {"ids": top_ids})
        text_by_id = {t["queryid"]: t["query"] for t in texts}

        buckets = q(f"""
            SELECT queryid,
                   toStartOfInterval(ts, INTERVAL {step} MINUTE) AS t,
                   sum(calls)                     AS calls,
                   sum(total_exec_time)           AS total_time,
                   total_time / greatest(calls,1) AS mean_time
            FROM statements_metrics
            WHERE ts > now() - INTERVAL %(hours)s HOUR AND queryid IN %(ids)s
            GROUP BY queryid, t ORDER BY t
        """, {"hours": hours, "ids": top_ids})
        by_id: dict = {}
        for b in buckets:
            by_id.setdefault(b["queryid"], []).append(
                {"t": b["t"], "calls": b["calls"],
                 "total_time": b["total_time"], "mean_time": b["mean_time"]})
        for r in top_rows:
            per_query.append({
                "queryid": str(r["queryid"]),
                "query": text_by_id.get(r["queryid"], ""),
                "total_time": r["total_time"],
                "series": by_id.get(r["queryid"], []),
            })

    return {"series": series, "top_queries": per_query}


# ---------------------------------------------------------------------------
# Активность (pg_activity / ASH)
# ---------------------------------------------------------------------------

@router.get("/activity")
def activity(hours: int = Query(1, ge=1, le=336)) -> dict:
    step = bucket_minutes(hours)

    # средние активные сессии по типам ожиданий (Average Active Sessions)
    wait_series = q(f"""
        SELECT toStartOfInterval(ts, INTERVAL {step} MINUTE) AS t,
               if(wait_event_type = '', 'CPU', wait_event_type) AS wait_type,
               count() / greatest(uniqExact(ts), 1) AS sessions
        FROM ash
        WHERE ts > now() - INTERVAL %(hours)s HOUR AND state = 'active'
        GROUP BY t, wait_type
        ORDER BY t
    """, {"hours": hours})

    current = q("""
        SELECT *
        FROM ash
        WHERE ts = (SELECT max(ts) FROM ash)
        ORDER BY query_start
        LIMIT 1 BY pid
    """)

    top_waits = q("""
        SELECT if(wait_event = '', 'CPU', concat(wait_event_type, ':', wait_event)) AS wait,
               count() AS samples
        FROM ash
        WHERE ts > now() - INTERVAL %(hours)s HOUR AND state = 'active'
        GROUP BY wait ORDER BY samples DESC LIMIT 10
    """, {"hours": hours})

    top_queries = q("""
        SELECT a.queryid                     AS queryid,
               any(a.query)                  AS query,
               count()                       AS samples
        FROM ash a
        WHERE a.ts > now() - INTERVAL %(hours)s HOUR AND a.state = 'active'
        GROUP BY a.queryid ORDER BY samples DESC LIMIT 10
    """, {"hours": hours})

    return {"wait_series": wait_series, "current": str_qids(current),
            "top_waits": top_waits, "top_queries": str_qids(top_queries)}


# ---------------------------------------------------------------------------
# Блокировки
# ---------------------------------------------------------------------------

@router.get("/locks")
def locks(days: int = Query(7, ge=1, le=7), limit: int = Query(300, ge=1, le=2000)) -> dict:
    current = q("""
        SELECT * FROM locks
        WHERE ts = (SELECT max(ts) FROM locks WHERE ts > now() - INTERVAL 2 MINUTE)
        ORDER BY blocked_duration_s DESC
    """)
    history = q("""
        SELECT * FROM locks
        WHERE ts > now() - INTERVAL %(days)s DAY
        ORDER BY ts DESC LIMIT %(limit)s
    """, {"days": days, "limit": limit})
    series = q("""
        SELECT toStartOfInterval(ts, INTERVAL 10 MINUTE) AS t,
               uniqExact(blocked_pid) AS blocked_sessions
        FROM locks
        WHERE ts > now() - INTERVAL %(days)s DAY
        GROUP BY t ORDER BY t
    """, {"days": days})
    return {"current": current, "history": history, "series": series}


# ---------------------------------------------------------------------------
# Алерты и рекомендации
# ---------------------------------------------------------------------------

@router.get("/alerts")
def alerts(days: int = Query(7, ge=1, le=90), severity: str = "") -> list[dict]:
    where = "ts > now() - INTERVAL %(days)s DAY"
    params: dict[str, Any] = {"days": days}
    if severity:
        where += " AND severity = %(sev)s"
        params["sev"] = severity
    rows = q(f"""
        SELECT a.*, argMax(q.query, q.last_seen) AS query
        FROM plan_alerts a
        LEFT JOIN queries q ON q.queryid = a.queryid
        WHERE {where}
        GROUP BY a.ts, a.cluster, a.queryid, a.datname, a.severity, a.kind,
                 a.description, a.old_fingerprint, a.new_fingerprint,
                 a.old_cost, a.new_cost
        ORDER BY a.ts DESC
        LIMIT 500
    """, params)
    return str_qids(rows)


@router.get("/alerts/badge")
def alerts_badge() -> dict:
    rows = q("""
        SELECT severity, count() AS cnt FROM plan_alerts
        WHERE ts > now() - INTERVAL 24 HOUR
        GROUP BY severity
    """)
    return {r["severity"]: r["cnt"] for r in rows}


@router.get("/recommendations")
def recommendations() -> list[dict]:
    return str_qids(q("""
        SELECT kind, datname, tablename, indexname, columns, reason, ddl,
               queryids, max(ts) AS ts
        FROM recommendations
        GROUP BY kind, datname, tablename, indexname, columns, reason, ddl, queryids
        ORDER BY kind, tablename
    """))


# ---------------------------------------------------------------------------
# Таблицы
# ---------------------------------------------------------------------------

@router.get("/tables")
def tables_list(search: str = "") -> list[dict]:
    where = ""
    params: dict[str, Any] = {}
    if search.strip():
        where = "AND positionCaseInsensitive(relname, %(s)s) > 0"
        params["s"] = search.strip()
    rows = q(f"""
        SELECT datname, schemaname, relname,
               argMax(total_bytes, ts)         AS total_bytes,
               argMax(toast_bytes, ts)         AS toast_bytes,
               argMax(n_live_tup, ts)          AS n_live_tup,
               argMax(n_dead_tup, ts)          AS n_dead_tup,
               argMax(n_mod_since_analyze, ts) AS n_mod_since_analyze,
               argMax(seq_scan, ts)            AS seq_scan,
               argMax(idx_scan, ts)            AS idx_scan,
               argMax(last_vacuum, ts)         AS last_vacuum,
               argMax(last_autovacuum, ts)     AS last_autovacuum,
               argMax(last_analyze, ts)        AS last_analyze,
               argMax(last_autoanalyze, ts)    AS last_autoanalyze,
               argMax(vacuum_count, ts)        AS vacuum_count,
               argMax(autovacuum_count, ts)    AS autovacuum_count
        FROM table_stats
        WHERE ts > now() - INTERVAL 2 DAY {where}
        GROUP BY datname, schemaname, relname
        ORDER BY total_bytes DESC
        LIMIT 500
    """, params)

    bloat = q("""
        SELECT datname, schemaname, relname, kind,
               argMax(bloat_bytes, ts) AS bloat_bytes,
               argMax(bloat_pct, ts)   AS bloat_pct,
               argMax(real_bytes, ts)  AS real_bytes
        FROM bloat_stats
        WHERE ts > now() - INTERVAL 2 DAY AND kind = 'table'
        GROUP BY datname, schemaname, relname, kind
    """)
    bloat_by_tbl = {(b["datname"], b["schemaname"], b["relname"]): b for b in bloat}

    idx_bloat = q("""
        SELECT datname, schemaname, relname,
               sum(b) AS idx_bloat_bytes
        FROM (
            SELECT datname, schemaname, relname, indexrelname,
                   argMax(bloat_bytes, ts) AS b
            FROM bloat_stats
            WHERE ts > now() - INTERVAL 2 DAY AND kind = 'index'
            GROUP BY datname, schemaname, relname, indexrelname
        )
        GROUP BY datname, schemaname, relname
    """)
    ib_by_tbl = {(b["datname"], b["schemaname"], b["relname"]): b["idx_bloat_bytes"]
                 for b in idx_bloat}

    for r in rows:
        key = (r["datname"], r["schemaname"], r["relname"])
        b = bloat_by_tbl.get(key, {})
        r["bloat_bytes"] = b.get("bloat_bytes", 0)
        r["bloat_pct"] = b.get("bloat_pct", 0)
        r["idx_bloat_bytes"] = ib_by_tbl.get(key, 0)
    return rows


@router.get("/table/{datname}/{table}")
def table_detail(datname: str, table: str) -> dict:
    stats = q("""
        SELECT relname, schemaname,
               argMax(seq_scan, ts) AS seq_scan,
               argMax(idx_scan, ts) AS idx_scan,
               argMax(n_live_tup, ts) AS n_live_tup,
               argMax(n_dead_tup, ts) AS n_dead_tup,
               argMax(n_mod_since_analyze, ts) AS n_mod_since_analyze,
               argMax(total_bytes, ts) AS total_bytes,
               argMax(toast_bytes, ts) AS toast_bytes,
               argMax(last_vacuum, ts)      AS last_vacuum,
               argMax(last_autovacuum, ts)  AS last_autovacuum,
               argMax(last_analyze, ts)     AS last_analyze,
               argMax(last_autoanalyze, ts) AS last_autoanalyze,
               argMax(vacuum_count, ts)     AS vacuum_count,
               argMax(autovacuum_count, ts) AS autovacuum_count,
               argMax(analyze_count, ts)    AS analyze_count,
               argMax(autoanalyze_count, ts) AS autoanalyze_count
        FROM table_stats
        WHERE datname = %(d)s AND relname = %(t)s
        GROUP BY relname, schemaname
    """, {"d": datname, "t": table})

    indexes = q("""
        SELECT indexrelname,
               max(idx_scan) - min(idx_scan) AS idx_scan_delta,
               argMax(size_bytes, ts) AS size_bytes,
               argMax(definition, ts) AS definition,
               argMax(is_unique, ts) AS is_unique,
               argMax(is_primary, ts) AS is_primary
        FROM index_stats
        WHERE datname = %(d)s AND relname = %(t)s
        GROUP BY indexrelname
        ORDER BY size_bytes DESC
    """, {"d": datname, "t": table})

    queries_rows = q("""
        SELECT p.queryid AS queryid,
               argMax(q.query, q.last_seen) AS query,
               argMax(p.indexes, p.ts) AS indexes,
               max(p.ts) AS last_plan
        FROM query_plans p
        LEFT JOIN queries q ON q.queryid = p.queryid
        WHERE has(p.tables, %(t)s) AND p.datname = %(d)s
        GROUP BY p.queryid
        LIMIT 100
    """, {"d": datname, "t": table})

    bloat = q("""
        SELECT indexrelname, kind,
               argMax(bloat_bytes, ts) AS bloat_bytes,
               argMax(bloat_pct, ts)   AS bloat_pct
        FROM bloat_stats
        WHERE datname = %(d)s AND relname = %(t)s
          AND ts > now() - INTERVAL 2 DAY
        GROUP BY indexrelname, kind
    """, {"d": datname, "t": table})
    table_bloat = next((b for b in bloat if b["kind"] == "table"), {})
    idx_bloat = {b["indexrelname"]: b for b in bloat if b["kind"] == "index"}
    for i in indexes:
        b = idx_bloat.get(i["indexrelname"], {})
        i["bloat_bytes"] = b.get("bloat_bytes", 0)
        i["bloat_pct"] = b.get("bloat_pct", 0)

    return {"stats": stats[0] if stats else {}, "indexes": indexes,
            "table_bloat": table_bloat, "queries": str_qids(queries_rows)}
