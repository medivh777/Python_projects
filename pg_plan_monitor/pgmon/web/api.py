"""JSON API. Все данные читаются из ClickHouse — PostgreSQL не нагружается.

Каждый эндпоинт принимает необязательный параметр ?cluster=<имя> —
фильтр по кластеру PostgreSQL (см. секцию clusters конфига).
Без параметра возвращаются данные всех кластеров.
"""

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


def cluster_cond(cluster: str, params: dict, col: str = "cluster") -> str:
    """'AND <col> = %(cluster)s', если фильтр по кластеру задан."""
    if cluster:
        params["cluster"] = cluster
        return f"AND {col} = %(cluster)s"
    return ""


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


@router.get("/clusters")
def clusters_list() -> list[str]:
    """Имена кластеров из конфигурации — для селектора в шапке."""
    return [c["name"] for c in cfg().clusters]


# ---------------------------------------------------------------------------
# Обзор
# ---------------------------------------------------------------------------

@router.get("/overview")
def overview(hours: int = Query(6, ge=1, le=720), cluster: str = "") -> dict:
    step = bucket_minutes(hours)
    params: dict[str, Any] = {"hours": hours}
    cc = cluster_cond(cluster, params)

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
        WHERE ts > now() - INTERVAL %(hours)s HOUR {cc}
        GROUP BY t ORDER BY t
    """, params)

    stmt = q(f"""
        SELECT toStartOfInterval(ts, INTERVAL {step} MINUTE) AS t,
               sum(calls)                     AS calls,
               sum(total_exec_time)           AS total_time,
               total_time / greatest(calls,1) AS mean_time
        FROM statements_metrics
        WHERE ts > now() - INTERVAL %(hours)s HOUR {cc}
        GROUP BY t ORDER BY t
    """, params)

    totals = q(f"""
        SELECT count(DISTINCT queryid)        AS queries,
               sum(calls)                     AS calls,
               sum(total_exec_time)           AS total_time,
               total_time / greatest(calls,1) AS mean_time
        FROM statements_metrics
        WHERE ts > now() - INTERVAL %(hours)s HOUR {cc}
    """, params)

    aparams: dict[str, Any] = {}
    acc = cluster_cond(cluster, aparams)
    alerts = q(f"""
        SELECT severity, count() AS cnt
        FROM plan_alerts
        WHERE ts > now() - INTERVAL 24 HOUR {acc}
        GROUP BY severity
    """, aparams)

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
            limit: int = Query(100, ge=1, le=1000),
            cluster: str = "") -> list[dict]:
    params: dict[str, Any] = {"hours": hours, "limit": limit}
    cc = cluster_cond(cluster, params, "m.cluster")

    # серверный поиск по телу запроса: сначала находим подходящие queryid
    search_filter = ""
    if search.strip():
        sparams: dict[str, Any] = {"s": search.strip()}
        scc = cluster_cond(cluster, sparams)
        found = q(f"""
            SELECT DISTINCT queryid FROM queries
            WHERE positionCaseInsensitive(query, %(s)s) > 0 {scc}
            LIMIT 2000
        """, sparams)
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
        WHERE m.ts > now() - INTERVAL %(hours)s HOUR {cc} {search_filter}
        GROUP BY m.queryid
        ORDER BY {order} DESC
        LIMIT %(limit)s
    """, params)

    if not rows:
        return []
    ids = [r["queryid"] for r in rows]

    tparams: dict[str, Any] = {"ids": ids}
    tcc = cluster_cond(cluster, tparams)
    texts = q(f"""
        SELECT queryid, argMax(query, last_seen) AS query
        FROM queries
        WHERE queryid IN %(ids)s {tcc}
        GROUP BY queryid
    """, tparams)
    text_by_id = {t["queryid"]: t["query"] for t in texts}

    plans = q(f"""
        SELECT queryid, uniqExact(fingerprint) AS plan_count,
               argMax(fingerprint, ts) AS last_fingerprint
        FROM query_plans
        WHERE queryid IN %(ids)s {tcc}
        GROUP BY queryid
    """, tparams)
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
def query_detail(queryid: int, hours: int = Query(24, ge=1, le=720),
                 cluster: str = "") -> dict:
    step = bucket_minutes(hours)
    params: dict[str, Any] = {"qid": queryid, "hours": hours}
    cc = cluster_cond(cluster, params)

    mparams: dict[str, Any] = {"qid": queryid}
    mcc = cluster_cond(cluster, mparams, "qq.cluster")
    meta = q(f"""
        SELECT argMax(qq.query, qq.last_seen)   AS query,
               argMax(qq.datname, qq.last_seen) AS datname,
               argMax(qq.usename, qq.last_seen) AS usename,
               max(qq.last_seen)                AS last_seen
        FROM queries qq WHERE qq.queryid = %(qid)s {mcc} GROUP BY qq.queryid
    """, mparams)
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
        WHERE queryid = %(qid)s AND ts > now() - INTERVAL %(hours)s HOUR {cc}
        GROUP BY t ORDER BY t
    """, params)

    totals = q(f"""
        SELECT sum(calls) AS calls,
               sum(total_exec_time) AS total_time,
               total_time / greatest(calls,1) AS mean_time,
               max(max_exec_time) AS max_time,
               sum(shared_blks_read) AS blks_read,
               sum(shared_blks_hit)  AS blks_hit
        FROM statements_metrics
        WHERE queryid = %(qid)s AND ts > now() - INTERVAL %(hours)s HOUR {cc}
    """, params)

    plans = q(f"""
        SELECT ts, fingerprint, total_cost, tables, indexes,
               seq_scan_tables, node_types, source
        FROM query_plans
        WHERE queryid = %(qid)s {mcc.replace('qq.', '')}
        ORDER BY ts DESC LIMIT 50
    """, mparams)

    alerts = q(f"""
        SELECT ts, severity, kind, description, old_fingerprint, new_fingerprint
        FROM plan_alerts
        WHERE queryid = %(qid)s {mcc.replace('qq.', '')}
        ORDER BY ts DESC LIMIT 50
    """, mparams)

    # запросы, работающие с теми же таблицами
    tables: list[str] = plans[0]["tables"] if plans else []
    related: list[dict] = []
    if tables:
        rparams: dict[str, Any] = {"tables": tables, "qid": queryid}
        rcc = cluster_cond(cluster, rparams, "p.cluster")
        related = q(f"""
            SELECT DISTINCT p.queryid AS queryid,
                   argMax(q.query, q.last_seen) AS query
            FROM query_plans p
            LEFT JOIN queries q ON q.queryid = p.queryid
            WHERE hasAny(p.tables, %(tables)s) AND p.queryid != %(qid)s {rcc}
            GROUP BY p.queryid
            LIMIT 30
        """, rparams)

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
def plan_json(queryid: int, fingerprint: str, cluster: str = "") -> dict:
    params: dict[str, Any] = {"qid": queryid, "fp": fingerprint}
    cc = cluster_cond(cluster, params)
    rows = q(f"""
        SELECT ts, plan_json, total_cost, source
        FROM query_plans
        WHERE queryid = %(qid)s AND fingerprint = %(fp)s {cc}
        ORDER BY ts DESC LIMIT 1
    """, params)
    if not rows:
        raise HTTPException(404, "план не найден")
    r = rows[0]
    return {"ts": r["ts"], "total_cost": r["total_cost"], "source": r["source"],
            "plan": json.loads(r["plan_json"])}


@router.get("/query/{queryid}/diff")
def plan_diff(queryid: int, old: str, new: str, cluster: str = "") -> dict:
    """Сравнение двух планов запроса по фингерпринтам."""
    def load(fp: str) -> dict:
        params: dict[str, Any] = {"qid": queryid, "fp": fp}
        cc = cluster_cond(cluster, params)
        rows = q(f"""
            SELECT plan_json FROM query_plans
            WHERE queryid = %(qid)s AND fingerprint = %(fp)s {cc}
            ORDER BY ts DESC LIMIT 1
        """, params)
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
                 limit: int = Query(20, ge=1, le=100),
                 cluster: str = "") -> list[dict]:
    """Ищет запросы по подстроке текста и возвращает версии их планов
    за период (последний план — первым). Если в периоде смен не было,
    возвращается последний известный план запроса."""
    if not text.strip():
        return []
    hparams: dict[str, Any] = {"s": text.strip(), "lim": limit}
    hcc = cluster_cond(cluster, hparams, "qq.cluster")
    hits = q(f"""
        SELECT qq.queryid AS queryid,
               argMax(qq.query, qq.last_seen)   AS query,
               argMax(qq.datname, qq.last_seen) AS datname,
               max(qq.last_seen)                AS last_seen
        FROM queries qq
        WHERE positionCaseInsensitive(qq.query, %(s)s) > 0 {hcc}
        GROUP BY qq.queryid
        ORDER BY last_seen DESC
        LIMIT %(lim)s
    """, hparams)

    out = []
    for h in hits:
        pparams: dict[str, Any] = {"qid": h["queryid"], "days": days}
        pcc = cluster_cond(cluster, pparams)
        plans = q(f"""
            SELECT ts, fingerprint, total_cost, tables, indexes,
                   seq_scan_tables, source
            FROM query_plans
            WHERE queryid = %(qid)s AND ts > now() - INTERVAL %(days)s DAY {pcc}
            ORDER BY ts DESC LIMIT 50
        """, pparams)
        if not plans:
            # смен плана в периоде не было — показываем последний известный
            plans = q(f"""
                SELECT ts, fingerprint, total_cost, tables, indexes,
                       seq_scan_tables, source
                FROM query_plans
                WHERE queryid = %(qid)s {pcc}
                ORDER BY ts DESC LIMIT 1
            """, pparams)
        h["plans"] = plans
        out.append(h)
    return str_qids(out)


# ---------------------------------------------------------------------------
# Statements: графики как в temboard
# ---------------------------------------------------------------------------

@router.get("/statements")
def statements(hours: int = Query(24, ge=1, le=720),
               top: int = Query(5, ge=1, le=8),
               cluster: str = "") -> dict:
    step = bucket_minutes(hours)
    params: dict[str, Any] = {"hours": hours}
    cc = cluster_cond(cluster, params)

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
        WHERE ts > now() - INTERVAL %(hours)s HOUR {cc}
        GROUP BY t ORDER BY t
    """, params)

    tparams: dict[str, Any] = {"hours": hours, "top": top}
    tcc = cluster_cond(cluster, tparams)
    top_rows = q(f"""
        SELECT queryid, sum(total_exec_time) AS total_time
        FROM statements_metrics
        WHERE ts > now() - INTERVAL %(hours)s HOUR {tcc}
        GROUP BY queryid ORDER BY total_time DESC LIMIT %(top)s
    """, tparams)
    top_ids = [r["queryid"] for r in top_rows]

    per_query: list[dict] = []
    if top_ids:
        xparams: dict[str, Any] = {"ids": top_ids}
        xcc = cluster_cond(cluster, xparams)
        texts = q(f"""
            SELECT queryid, argMax(query, last_seen) AS query
            FROM queries WHERE queryid IN %(ids)s {xcc} GROUP BY queryid
        """, xparams)
        text_by_id = {t["queryid"]: t["query"] for t in texts}

        bparams: dict[str, Any] = {"hours": hours, "ids": top_ids}
        bcc = cluster_cond(cluster, bparams)
        buckets = q(f"""
            SELECT queryid,
                   toStartOfInterval(ts, INTERVAL {step} MINUTE) AS t,
                   sum(calls)                     AS calls,
                   sum(total_exec_time)           AS total_time,
                   total_time / greatest(calls,1) AS mean_time
            FROM statements_metrics
            WHERE ts > now() - INTERVAL %(hours)s HOUR AND queryid IN %(ids)s {bcc}
            GROUP BY queryid, t ORDER BY t
        """, bparams)
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
def activity(hours: int = Query(1, ge=1, le=336), cluster: str = "") -> dict:
    step = bucket_minutes(hours)
    params: dict[str, Any] = {"hours": hours}
    cc = cluster_cond(cluster, params)

    # средние активные сессии по типам ожиданий (Average Active Sessions)
    wait_series = q(f"""
        SELECT toStartOfInterval(ts, INTERVAL {step} MINUTE) AS t,
               if(wait_event_type = '', 'CPU', wait_event_type) AS wait_type,
               count() / greatest(uniqExact(ts), 1) AS sessions
        FROM ash
        WHERE ts > now() - INTERVAL %(hours)s HOUR AND state = 'active' {cc}
        GROUP BY t, wait_type
        ORDER BY t
    """, params)

    cparams: dict[str, Any] = {}
    ccc = cluster_cond(cluster, cparams)
    current = q(f"""
        SELECT *
        FROM ash
        WHERE ts = (SELECT max(ts) FROM ash WHERE 1=1 {ccc}) {ccc}
        ORDER BY query_start
        LIMIT 1 BY pid
    """, cparams)

    top_waits = q(f"""
        SELECT if(wait_event = '', 'CPU', concat(wait_event_type, ':', wait_event)) AS wait,
               count() AS samples
        FROM ash
        WHERE ts > now() - INTERVAL %(hours)s HOUR AND state = 'active' {cc}
        GROUP BY wait ORDER BY samples DESC LIMIT 10
    """, params)

    aparams: dict[str, Any] = {"hours": hours}
    acc = cluster_cond(cluster, aparams, "a.cluster")
    top_queries = q(f"""
        SELECT a.queryid                     AS queryid,
               any(a.query)                  AS query,
               count()                       AS samples
        FROM ash a
        WHERE a.ts > now() - INTERVAL %(hours)s HOUR AND a.state = 'active' {acc}
        GROUP BY a.queryid ORDER BY samples DESC LIMIT 10
    """, aparams)

    return {"wait_series": wait_series, "current": str_qids(current),
            "top_waits": top_waits, "top_queries": str_qids(top_queries)}


# ---------------------------------------------------------------------------
# Блокировки
# ---------------------------------------------------------------------------

@router.get("/locks")
def locks(days: int = Query(7, ge=1, le=7), limit: int = Query(300, ge=1, le=2000),
          cluster: str = "") -> dict:
    cparams: dict[str, Any] = {}
    ccc = cluster_cond(cluster, cparams)
    current = q(f"""
        SELECT * FROM locks
        WHERE ts = (SELECT max(ts) FROM locks
                    WHERE ts > now() - INTERVAL 2 MINUTE {ccc}) {ccc}
        ORDER BY blocked_duration_s DESC
    """, cparams)

    params: dict[str, Any] = {"days": days, "limit": limit}
    cc = cluster_cond(cluster, params)
    history = q(f"""
        SELECT * FROM locks
        WHERE ts > now() - INTERVAL %(days)s DAY {cc}
        ORDER BY ts DESC LIMIT %(limit)s
    """, params)
    series = q(f"""
        SELECT toStartOfInterval(ts, INTERVAL 10 MINUTE) AS t,
               uniqExact(blocked_pid) AS blocked_sessions
        FROM locks
        WHERE ts > now() - INTERVAL %(days)s DAY {cc}
        GROUP BY t ORDER BY t
    """, params)
    return {"current": current, "history": history, "series": series}


# ---------------------------------------------------------------------------
# Алерты и рекомендации
# ---------------------------------------------------------------------------

@router.get("/alerts")
def alerts(days: int = Query(7, ge=1, le=90), severity: str = "",
           cluster: str = "") -> list[dict]:
    where = "a.ts > now() - INTERVAL %(days)s DAY"
    params: dict[str, Any] = {"days": days}
    if severity:
        where += " AND a.severity = %(sev)s"
        params["sev"] = severity
    where += " " + cluster_cond(cluster, params, "a.cluster")
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
def alerts_badge(cluster: str = "") -> dict:
    params: dict[str, Any] = {}
    cc = cluster_cond(cluster, params)
    rows = q(f"""
        SELECT severity, count() AS cnt FROM plan_alerts
        WHERE ts > now() - INTERVAL 24 HOUR {cc}
        GROUP BY severity
    """, params)
    return {r["severity"]: r["cnt"] for r in rows}


@router.get("/recommendations")
def recommendations(cluster: str = "") -> list[dict]:
    params: dict[str, Any] = {}
    cc = cluster_cond(cluster, params)
    return str_qids(q(f"""
        SELECT kind, datname, tablename, indexname, columns, reason, ddl,
               queryids, max(ts) AS ts
        FROM recommendations
        WHERE 1=1 {cc}
        GROUP BY kind, datname, tablename, indexname, columns, reason, ddl, queryids
        ORDER BY kind, tablename
    """, params))


# ---------------------------------------------------------------------------
# Таблицы
# ---------------------------------------------------------------------------

@router.get("/tables")
def tables_list(search: str = "", cluster: str = "") -> list[dict]:
    params: dict[str, Any] = {}
    where = cluster_cond(cluster, params)
    if search.strip():
        where += " AND positionCaseInsensitive(relname, %(s)s) > 0"
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

    bparams: dict[str, Any] = {}
    bcc = cluster_cond(cluster, bparams)
    bloat = q(f"""
        SELECT datname, schemaname, relname, kind,
               argMax(bloat_bytes, ts) AS bloat_bytes,
               argMax(bloat_pct, ts)   AS bloat_pct,
               argMax(real_bytes, ts)  AS real_bytes
        FROM bloat_stats
        WHERE ts > now() - INTERVAL 2 DAY AND kind = 'table' {bcc}
        GROUP BY datname, schemaname, relname, kind
    """, bparams)
    bloat_by_tbl = {(b["datname"], b["schemaname"], b["relname"]): b for b in bloat}

    idx_bloat = q(f"""
        SELECT datname, schemaname, relname,
               sum(b) AS idx_bloat_bytes
        FROM (
            SELECT datname, schemaname, relname, indexrelname,
                   argMax(bloat_bytes, ts) AS b
            FROM bloat_stats
            WHERE ts > now() - INTERVAL 2 DAY AND kind = 'index' {bcc}
            GROUP BY datname, schemaname, relname, indexrelname
        )
        GROUP BY datname, schemaname, relname
    """, bparams)
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
def table_detail(datname: str, table: str, cluster: str = "") -> dict:
    params: dict[str, Any] = {"d": datname, "t": table}
    cc = cluster_cond(cluster, params)

    stats = q(f"""
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
        WHERE datname = %(d)s AND relname = %(t)s {cc}
        GROUP BY relname, schemaname
    """, params)

    indexes = q(f"""
        SELECT indexrelname,
               max(idx_scan) - min(idx_scan) AS idx_scan_delta,
               argMax(size_bytes, ts) AS size_bytes,
               argMax(definition, ts) AS definition,
               argMax(is_unique, ts) AS is_unique,
               argMax(is_primary, ts) AS is_primary
        FROM index_stats
        WHERE datname = %(d)s AND relname = %(t)s {cc}
        GROUP BY indexrelname
        ORDER BY size_bytes DESC
    """, params)

    qparams: dict[str, Any] = {"d": datname, "t": table}
    qcc = cluster_cond(cluster, qparams, "p.cluster")
    queries_rows = q(f"""
        SELECT p.queryid AS queryid,
               argMax(q.query, q.last_seen) AS query,
               argMax(p.indexes, p.ts) AS indexes,
               max(p.ts) AS last_plan
        FROM query_plans p
        LEFT JOIN queries q ON q.queryid = p.queryid
        WHERE has(p.tables, %(t)s) AND p.datname = %(d)s {qcc}
        GROUP BY p.queryid
        LIMIT 100
    """, qparams)

    bloat = q(f"""
        SELECT indexrelname, kind,
               argMax(bloat_bytes, ts) AS bloat_bytes,
               argMax(bloat_pct, ts)   AS bloat_pct
        FROM bloat_stats
        WHERE datname = %(d)s AND relname = %(t)s {cc}
          AND ts > now() - INTERVAL 2 DAY
        GROUP BY indexrelname, kind
    """, params)
    table_bloat = next((b for b in bloat if b["kind"] == "table"), {})
    idx_bloat = {b["indexrelname"]: b for b in bloat if b["kind"] == "index"}
    for i in indexes:
        b = idx_bloat.get(i["indexrelname"], {})
        i["bloat_bytes"] = b.get("bloat_bytes", 0)
        i["bloat_pct"] = b.get("bloat_pct", 0)

    return {"stats": stats[0] if stats else {}, "indexes": indexes,
            "table_bloat": table_bloat, "queries": str_qids(queries_rows)}
