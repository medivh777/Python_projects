"""Подключения к PostgreSQL и SQL-запросы коллектора.

Принципы, чтобы не грузить наблюдаемую БД:
  * все выборки — из статистических представлений (память, без обращения к данным);
  * на каждый кластер — одно постоянное соединение на поток коллектора;
  * EXPLAIN выполняется в транзакции с ROLLBACK и statement_timeout;
  * размер таблиц/индексов берётся редко (интервал table_stats).
"""

from __future__ import annotations

import logging
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row

log = logging.getLogger(__name__)


def connect(dsn: str, dbname: str | None = None) -> psycopg.Connection:
    conn = psycopg.connect(
        dsn,
        dbname=dbname,
        autocommit=True,
        row_factory=dict_row,
        application_name="pg_plan_monitor",
        connect_timeout=10,
    )
    return conn


class ReconnectingConn:
    """Обёртка над соединением с автоматическим переподключением."""

    def __init__(self, dsn: str, dbname: str | None = None):
        self.dsn = dsn
        self.dbname = dbname
        self._conn: psycopg.Connection | None = None

    def get(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            self._conn = connect(self.dsn, self.dbname)
        return self._conn

    def query(self, sql: str, params=None) -> list[dict]:
        try:
            with self.get().cursor() as cur:
                cur.execute(sql, params)
                if cur.description is None:
                    return []
                return cur.fetchall()
        except psycopg.OperationalError:
            # соединение умерло — пробуем один раз переподключиться
            self.close()
            with self.get().cursor() as cur:
                cur.execute(sql, params)
                if cur.description is None:
                    return []
                return cur.fetchall()

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

SQL_SERVER_VERSION = "SELECT current_setting('server_version_num')::int AS v"

SQL_DATABASES = """
SELECT datname FROM pg_database
WHERE NOT datistemplate AND datallowconn
ORDER BY datname
"""

# pg_stat_statements: PG13+ (total_exec_time). Снимаем всё разом, дельты считаем сами.
SQL_STATEMENTS = """
SELECT
    d.datname,
    r.rolname                            AS usename,
    s.queryid,
    s.query,
    s.calls,
    s.total_exec_time,
    s.min_exec_time,
    s.max_exec_time,
    s.mean_exec_time,
    s.stddev_exec_time,
    s.rows,
    s.shared_blks_hit,
    s.shared_blks_read,
    s.shared_blks_dirtied,
    s.shared_blks_written,
    s.local_blks_read,
    s.local_blks_written,
    s.temp_blks_read,
    s.temp_blks_written,
    s.blk_read_time,
    s.blk_write_time,
    s.wal_bytes
FROM pg_stat_statements s
JOIN pg_database d ON d.oid = s.dbid
JOIN pg_roles    r ON r.oid = s.userid
WHERE s.queryid IS NOT NULL
"""

# Активные сессии для ASH. pg_blocking_pids дёшев и вызывается
# только для ожидающих на локах сессий.
SQL_ACTIVITY = """
SELECT
    a.datname,
    a.pid,
    a.usename,
    coalesce(a.application_name, '')          AS application_name,
    coalesce(host(a.client_addr), '')         AS client_addr,
    a.backend_type,
    coalesce(a.state, '')                     AS state,
    coalesce(a.wait_event_type, '')           AS wait_event_type,
    coalesce(a.wait_event, '')                AS wait_event,
    coalesce(a.query_id, 0)                   AS queryid,
    coalesce(a.query, '')                     AS query,
    a.query_start,
    a.xact_start,
    CASE WHEN a.wait_event_type = 'Lock'
         THEN pg_blocking_pids(a.pid)
         ELSE '{}'::int[]
    END                                       AS blocked_by
FROM pg_stat_activity a
WHERE a.state IS DISTINCT FROM 'idle'
  AND a.pid <> pg_backend_pid()
  AND a.backend_type = 'client backend'
"""

# Активность прямо сейчас (для страницы «Активность», включая idle in transaction).
SQL_ACTIVITY_NOW = """
SELECT
    a.datname,
    a.pid,
    a.usename,
    coalesce(a.application_name, '')  AS application_name,
    coalesce(host(a.client_addr), '') AS client_addr,
    a.backend_type,
    coalesce(a.state, '')             AS state,
    coalesce(a.wait_event_type, '')   AS wait_event_type,
    coalesce(a.wait_event, '')        AS wait_event,
    coalesce(a.query_id, 0)           AS queryid,
    coalesce(a.query, '')             AS query,
    extract(epoch FROM clock_timestamp() - a.query_start) AS query_age_s,
    extract(epoch FROM clock_timestamp() - a.xact_start)  AS xact_age_s
FROM pg_stat_activity a
WHERE a.pid <> pg_backend_pid()
  AND a.backend_type = 'client backend'
ORDER BY a.query_start NULLS LAST
"""

# Кто кого блокирует: пары blocked/blocking через pg_blocking_pids.
SQL_LOCKS = """
WITH blocked AS (
    SELECT a.pid,
           a.datname,
           a.usename,
           a.query,
           extract(epoch FROM clock_timestamp() - a.query_start) AS wait_s,
           unnest(pg_blocking_pids(a.pid)) AS blocking_pid
    FROM pg_stat_activity a
    WHERE cardinality(pg_blocking_pids(a.pid)) > 0
)
SELECT
    b.pid                                   AS blocked_pid,
    coalesce(b.datname, '')                 AS datname,
    coalesce(b.usename, '')                 AS blocked_user,
    coalesce(b.query, '')                   AS blocked_query,
    coalesce(b.wait_s, 0)                   AS blocked_duration_s,
    coalesce(wl.mode, '')                   AS blocked_mode,
    b.blocking_pid,
    coalesce(ba.usename, '')                AS blocking_user,
    coalesce(ba.query, '')                  AS blocking_query,
    coalesce(ba.state, '')                  AS blocking_state,
    coalesce(hl.mode, '')                   AS blocking_mode,
    coalesce(wl.locktype, '')               AS lock_type,
    coalesce(wl.relation::regclass::text, '') AS relation
FROM blocked b
JOIN pg_stat_activity ba ON ba.pid = b.blocking_pid
LEFT JOIN pg_locks wl ON wl.pid = b.pid AND NOT wl.granted
LEFT JOIN pg_locks hl ON hl.pid = b.blocking_pid AND hl.granted
      AND hl.locktype = wl.locktype
      AND hl.relation IS NOT DISTINCT FROM wl.relation
      AND hl.transactionid IS NOT DISTINCT FROM wl.transactionid
"""

SQL_DB_STATS = """
SELECT
    sum(xact_commit)::bigint    AS xact_commit,
    sum(xact_rollback)::bigint  AS xact_rollback,
    sum(blks_read)::bigint      AS blks_read,
    sum(blks_hit)::bigint       AS blks_hit,
    sum(tup_returned)::bigint   AS tup_returned,
    sum(tup_fetched)::bigint    AS tup_fetched,
    sum(tup_inserted)::bigint   AS tup_inserted,
    sum(tup_updated)::bigint    AS tup_updated,
    sum(tup_deleted)::bigint    AS tup_deleted,
    sum(temp_bytes)::bigint     AS temp_bytes,
    sum(deadlocks)::bigint      AS deadlocks,
    sum(blk_read_time)          AS blk_read_time,
    sum(blk_write_time)         AS blk_write_time
FROM pg_stat_database
WHERE datname IS NOT NULL
"""

SQL_BACKEND_COUNTS = """
SELECT
    count(*) FILTER (WHERE state = 'active')                    AS active,
    count(*) FILTER (WHERE state LIKE 'idle in transaction%%')  AS idle_in_xact,
    count(*) FILTER (WHERE wait_event_type = 'Lock')            AS waiting,
    count(*)                                                    AS total
FROM pg_stat_activity
WHERE backend_type = 'client backend'
"""

SQL_TABLE_STATS = """
SELECT
    current_database()                       AS datname,
    s.schemaname,
    s.relname,
    s.seq_scan,
    coalesce(s.idx_scan, 0)                  AS idx_scan,
    s.n_live_tup,
    s.n_dead_tup,
    s.n_mod_since_analyze,
    pg_total_relation_size(s.relid)::bigint  AS total_bytes,
    CASE WHEN c.reltoastrelid <> 0
         THEN pg_total_relation_size(c.reltoastrelid)::bigint
         ELSE 0 END                          AS toast_bytes,
    coalesce(s.last_vacuum,      'epoch'::timestamptz) AS last_vacuum,
    coalesce(s.last_autovacuum,  'epoch'::timestamptz) AS last_autovacuum,
    coalesce(s.last_analyze,     'epoch'::timestamptz) AS last_analyze,
    coalesce(s.last_autoanalyze, 'epoch'::timestamptz) AS last_autoanalyze,
    s.vacuum_count,
    s.autovacuum_count,
    s.analyze_count,
    s.autoanalyze_count
FROM pg_stat_user_tables s
JOIN pg_class c ON c.oid = s.relid
"""

# Оценка bloat таблиц (по мотивам ioguix/pgsql-bloat-estimation).
# Работает по статистике планировщика, данные таблиц не читает.
SQL_TABLE_BLOAT = """
SELECT current_database() AS datname, schemaname, tblname AS relname,
       (tblpages * bs)::bigint AS real_bytes,
       CASE WHEN tblpages - est_tblpages_ff > 0
            THEN ((tblpages - est_tblpages_ff) * bs)::bigint ELSE 0 END AS bloat_bytes,
       CASE WHEN tblpages > 0 AND tblpages - est_tblpages_ff > 0
            THEN round((100 * (tblpages - est_tblpages_ff) / tblpages::float)::numeric, 1)::float
            ELSE 0 END AS bloat_pct
FROM (
  SELECT ceil( greatest(reltuples, 0) / ( (bs - page_hdr) * fillfactor / (tpl_size * 100) ) )
           + ceil( toasttuples / 4 ) AS est_tblpages_ff,
         tblpages, bs, schemaname, tblname
  FROM (
    SELECT ( 4 + tpl_hdr_size + tpl_data_size + (2 * ma)
             - CASE WHEN tpl_hdr_size % ma = 0 THEN ma ELSE tpl_hdr_size % ma END
             - CASE WHEN ceil(tpl_data_size)::int % ma = 0 THEN ma
                    ELSE ceil(tpl_data_size)::int % ma END
           ) AS tpl_size,
           heappages + toastpages AS tblpages,
           reltuples, toasttuples, bs, page_hdr, fillfactor, schemaname, tblname
    FROM (
      SELECT ns.nspname AS schemaname, tbl.relname AS tblname, tbl.reltuples,
             tbl.relpages AS heappages, coalesce(toast.relpages, 0) AS toastpages,
             coalesce(toast.reltuples, 0) AS toasttuples,
             coalesce(substring(array_to_string(tbl.reloptions, ' ')
                      FROM 'fillfactor=([0-9]+)')::smallint, 100) AS fillfactor,
             current_setting('block_size')::numeric AS bs,
             CASE WHEN version() ~ 'mingw32|64-bit|x86_64|ppc64|ia64|amd64'
                  THEN 8 ELSE 4 END AS ma,
             24 AS page_hdr,
             23 + CASE WHEN max(coalesce(st.null_frac, 0)) > 0
                       THEN (7 + count(st.attname)) / 8 ELSE 0::int END AS tpl_hdr_size,
             sum((1 - coalesce(st.null_frac, 0)) * coalesce(st.avg_width, 0)) AS tpl_data_size
      FROM pg_attribute att
      JOIN pg_class tbl ON att.attrelid = tbl.oid
      JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
      LEFT JOIN pg_stats st ON st.schemaname = ns.nspname
           AND st.tablename = tbl.relname AND st.attname = att.attname
      LEFT JOIN pg_class toast ON tbl.reltoastrelid = toast.oid
      WHERE att.attnum > 0 AND NOT att.attisdropped
        AND tbl.relkind = 'r' AND tbl.relpages > 0
        AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ) s
  ) s2
  WHERE tpl_size > 0
) s3
"""

# Оценка bloat btree-индексов (по мотивам ioguix/pgsql-bloat-estimation).
# Индексы по выражениям пропускаются (нет статистики по колонке).
SQL_INDEX_BLOAT = """
SELECT current_database() AS datname, nspname AS schemaname,
       tblname AS relname, idxname AS indexrelname,
       (relpages * bs)::bigint AS real_bytes,
       CASE WHEN relpages > est_pages_ff
            THEN ((relpages - est_pages_ff) * bs)::bigint ELSE 0 END AS bloat_bytes,
       CASE WHEN relpages > 0 AND relpages > est_pages_ff
            THEN round((100 * (relpages - est_pages_ff)::float / relpages)::numeric, 1)::float
            ELSE 0 END AS bloat_pct
FROM (
  SELECT coalesce(1 + ceil(greatest(reltuples, 0) /
             floor((bs - pageopqdata - pagehdr) * fillfactor
                   / (100 * (4 + nulldatahdrwidth)::float))), 0) AS est_pages_ff,
         bs, nspname, tblname, idxname, relpages
  FROM (
    SELECT maxalign, bs, nspname, tblname, idxname, reltuples, relpages, fillfactor,
           ( index_tuple_hdr_bm + maxalign
             - CASE WHEN index_tuple_hdr_bm % maxalign = 0
                    THEN maxalign ELSE index_tuple_hdr_bm % maxalign END
             + nulldatawidth + maxalign
             - CASE WHEN nulldatawidth = 0 THEN 0
                    WHEN nulldatawidth::integer % maxalign = 0 THEN maxalign
                    ELSE nulldatawidth::integer % maxalign END
           )::numeric AS nulldatahdrwidth, pagehdr, pageopqdata
    FROM (
      SELECT n.nspname, i.tblname, i.idxname, i.reltuples, i.relpages, i.fillfactor,
             current_setting('block_size')::numeric AS bs,
             CASE WHEN version() ~ 'mingw32|64-bit|x86_64|ppc64|ia64|amd64'
                  THEN 8 ELSE 4 END AS maxalign,
             24 AS pagehdr, 16 AS pageopqdata,
             CASE WHEN max(coalesce(s.null_frac, 0)) = 0 THEN 8
                  ELSE 8 + ((32 + 8 - 1) / 8) END AS index_tuple_hdr_bm,
             sum((1 - coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 1024)) AS nulldatawidth
      FROM (
        SELECT ct.relname AS tblname, ct.relnamespace, ic.idxname, ic.attpos,
               ic.indkey[ic.attpos] AS indattnum,
               ic.reltuples, ic.relpages, ic.tbloid, ic.fillfactor
        FROM (
          SELECT idxname, reltuples, relpages, tbloid, fillfactor, indkey,
                 generate_series(1, indnatts) AS attpos
          FROM (
            SELECT ci.relname AS idxname, ci.reltuples, ci.relpages,
                   i.indrelid AS tbloid,
                   coalesce(substring(array_to_string(ci.reloptions, ' ')
                            FROM 'fillfactor=([0-9]+)')::smallint, 90) AS fillfactor,
                   i.indnatts,
                   string_to_array(textin(int2vectorout(i.indkey)), ' ')::int[] AS indkey
            FROM pg_index i
            JOIN pg_class ci ON ci.oid = i.indexrelid
            WHERE ci.relam = (SELECT oid FROM pg_am WHERE amname = 'btree')
              AND ci.relpages > 0
          ) idx_data
        ) ic
        JOIN pg_class ct ON ct.oid = ic.tbloid
      ) i
      JOIN pg_attribute a ON a.attrelid = i.tbloid AND a.attnum = i.indattnum
      JOIN pg_namespace n ON n.oid = i.relnamespace
      JOIN pg_stats s ON s.schemaname = n.nspname
           AND s.tablename = i.tblname AND s.attname = a.attname
      WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
        AND i.indattnum > 0
      GROUP BY 1, 2, 3, 4, 5, 6
    ) rows_data_stats
  ) rows_hdr_pdg_stats
) relation_stats
"""

SQL_INDEX_STATS = """
SELECT
    current_database()                          AS datname,
    s.schemaname,
    s.relname,
    s.indexrelname,
    s.idx_scan,
    pg_relation_size(s.indexrelid)::bigint      AS size_bytes,
    i.indisunique                               AS is_unique,
    i.indisprimary                              AS is_primary,
    pg_get_indexdef(s.indexrelid)               AS definition
FROM pg_stat_user_indexes s
JOIN pg_index i ON i.indexrelid = s.indexrelid
"""

# Топ запросов для снятия планов: самые дорогие по суммарному времени.
SQL_TOP_QUERIES = """
SELECT d.datname, s.queryid, s.query, s.calls
FROM pg_stat_statements s
JOIN pg_database d ON d.oid = s.dbid
WHERE s.queryid IS NOT NULL
  AND s.calls >= %(min_calls)s
  AND s.query !~* '^\\s*(begin|commit|rollback|set|show|explain|deallocate|fetch|close|vacuum|analyze|create|alter|drop|copy)'
ORDER BY s.total_exec_time DESC
LIMIT %(top_n)s
"""


@contextmanager
def explain_tx(conn: psycopg.Connection, timeout_ms: int):
    """Транзакция для EXPLAIN: всегда ROLLBACK, всегда с таймаутом."""
    old_autocommit = conn.autocommit
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            # SET не поддерживает bind-параметры — подставляем проверенное число
            cur.execute(f"SET LOCAL statement_timeout = {int(timeout_ms)}")
            yield cur
    finally:
        try:
            conn.rollback()
        finally:
            conn.autocommit = old_autocommit
