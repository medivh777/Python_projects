"""Слой хранения ClickHouse: DDL схемы и батчевые вставки.

Каждый поток коллектора создаёт собственный клиент (clickhouse-connect
не потокобезопасен). TTL таблиц берётся из конфигурации retention.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Sequence

import clickhouse_connect

from .config import Config

log = logging.getLogger(__name__)

_local = threading.local()


def make_client(cfg: Config):
    ch = cfg.clickhouse
    return clickhouse_connect.get_client(
        host=ch["host"],
        port=int(ch["port"]),
        username=ch["user"],
        password=ch["password"],
        database=ch["database"],
        secure=bool(ch.get("secure", False)),
    )


def thread_client(cfg: Config):
    """Клиент ClickHouse, привязанный к текущему потоку."""
    cli = getattr(_local, "client", None)
    if cli is None:
        cli = make_client(cfg)
        _local.client = cli
    return cli


# ---------------------------------------------------------------------------
# Схема
# ---------------------------------------------------------------------------

def schema_ddl(cfg: Config) -> list[str]:
    r = cfg.retention
    ash_days = int(r["ash_days"])
    locks_days = int(r["locks_days"])
    metrics_days = int(r["metrics_days"])
    plans_days = int(r["plans_days"])

    return [
        # Дельты pg_stat_statements между снапшотами: основа графиков statements.
        f"""
        CREATE TABLE IF NOT EXISTS statements_metrics (
            ts                  DateTime,
            cluster             LowCardinality(String),
            datname             LowCardinality(String),
            usename             LowCardinality(String),
            queryid             Int64,
            calls               UInt64,
            total_exec_time     Float64,
            mean_exec_time      Float64,
            min_exec_time       Float64,
            max_exec_time       Float64,
            stddev_exec_time    Float64,
            rows                UInt64,
            shared_blks_hit     UInt64,
            shared_blks_read    UInt64,
            shared_blks_dirtied UInt64,
            shared_blks_written UInt64,
            local_blks_read     UInt64,
            local_blks_written  UInt64,
            temp_blks_read      UInt64,
            temp_blks_written   UInt64,
            blk_read_time       Float64,
            blk_write_time      Float64,
            wal_bytes           UInt64
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMMDD(ts)
        ORDER BY (cluster, queryid, ts)
        TTL ts + INTERVAL {metrics_days} DAY
        """,
        # Справочник запросов: текст, БД, последнее появление.
        """
        CREATE TABLE IF NOT EXISTS queries (
            cluster    LowCardinality(String),
            queryid    Int64,
            datname    LowCardinality(String),
            usename    LowCardinality(String),
            query      String,
            last_seen  DateTime
        ) ENGINE = ReplacingMergeTree(last_seen)
        ORDER BY (cluster, queryid, datname)
        """,
        # История планов выполнения.
        f"""
        CREATE TABLE IF NOT EXISTS query_plans (
            ts               DateTime,
            cluster          LowCardinality(String),
            queryid          Int64,
            datname          LowCardinality(String),
            fingerprint      String,
            plan_json        String,
            total_cost       Float64,
            startup_cost     Float64,
            tables           Array(String),
            indexes          Array(String),
            seq_scan_tables  Array(String),
            node_types       Array(String),
            source           LowCardinality(String)
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(ts)
        ORDER BY (cluster, queryid, ts)
        TTL ts + INTERVAL {plans_days} DAY
        """,
        # Алерты о смене плана.
        f"""
        CREATE TABLE IF NOT EXISTS plan_alerts (
            ts              DateTime,
            cluster         LowCardinality(String),
            queryid         Int64,
            datname         LowCardinality(String),
            severity        LowCardinality(String),
            kind            LowCardinality(String),
            description     String,
            old_fingerprint String,
            new_fingerprint String,
            old_cost        Float64,
            new_cost        Float64
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(ts)
        ORDER BY (cluster, ts)
        TTL ts + INTERVAL {metrics_days} DAY
        """,
        # Active Session History: семпл pg_stat_activity раз в N секунд.
        f"""
        CREATE TABLE IF NOT EXISTS ash (
            ts               DateTime,
            cluster          LowCardinality(String),
            datname          LowCardinality(String),
            pid              Int32,
            usename          LowCardinality(String),
            application_name LowCardinality(String),
            client_addr      String,
            backend_type     LowCardinality(String),
            state            LowCardinality(String),
            wait_event_type  LowCardinality(String),
            wait_event       LowCardinality(String),
            queryid          Int64,
            query            String,
            query_start      DateTime64(3),
            xact_start       DateTime64(3),
            blocked_by       Array(Int32)
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMMDD(ts)
        ORDER BY (cluster, ts)
        TTL ts + INTERVAL {ash_days} DAY
        """,
        # История блокировок: кто кого блокирует. Хранится неделю.
        f"""
        CREATE TABLE IF NOT EXISTS locks (
            ts                 DateTime,
            cluster            LowCardinality(String),
            datname            LowCardinality(String),
            blocked_pid        Int32,
            blocked_user      LowCardinality(String),
            blocked_query      String,
            blocked_duration_s Float64,
            blocked_mode       LowCardinality(String),
            blocking_pid       Int32,
            blocking_user      LowCardinality(String),
            blocking_query     String,
            blocking_state     LowCardinality(String),
            blocking_mode      LowCardinality(String),
            lock_type          LowCardinality(String),
            relation           String
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMMDD(ts)
        ORDER BY (cluster, ts)
        TTL ts + INTERVAL {locks_days} DAY
        """,
        # Нагрузка: дельты pg_stat_database + опционально метрики хоста (psutil).
        f"""
        CREATE TABLE IF NOT EXISTS sysstat (
            ts                DateTime,
            cluster           LowCardinality(String),
            active_backends   UInt32,
            idle_in_xact      UInt32,
            waiting_backends  UInt32,
            total_backends    UInt32,
            xact_commit       UInt64,
            xact_rollback     UInt64,
            blks_read         UInt64,
            blks_hit          UInt64,
            tup_returned      UInt64,
            tup_fetched       UInt64,
            tup_inserted      UInt64,
            tup_updated       UInt64,
            tup_deleted       UInt64,
            temp_bytes        UInt64,
            deadlocks         UInt64,
            blk_read_time     Float64,
            blk_write_time    Float64,
            host_cpu_percent  Float32,
            host_mem_percent  Float32,
            host_read_bytes   UInt64,
            host_write_bytes  UInt64,
            host_load1        Float32
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMMDD(ts)
        ORDER BY (cluster, ts)
        TTL ts + INTERVAL {metrics_days} DAY
        """,
        # Снапшоты статистики таблиц.
        f"""
        CREATE TABLE IF NOT EXISTS table_stats (
            ts             DateTime,
            cluster        LowCardinality(String),
            datname        LowCardinality(String),
            schemaname     LowCardinality(String),
            relname        String,
            seq_scan       UInt64,
            idx_scan       UInt64,
            n_live_tup     UInt64,
            n_dead_tup     UInt64,
            total_bytes    UInt64
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(ts)
        ORDER BY (cluster, datname, schemaname, relname, ts)
        TTL ts + INTERVAL {metrics_days} DAY
        """,
        # Снапшоты статистики индексов (для рекомендаций «удалить неиспользуемый»).
        f"""
        CREATE TABLE IF NOT EXISTS index_stats (
            ts            DateTime,
            cluster       LowCardinality(String),
            datname       LowCardinality(String),
            schemaname    LowCardinality(String),
            relname       String,
            indexrelname  String,
            idx_scan      UInt64,
            size_bytes    UInt64,
            is_unique     UInt8,
            is_primary    UInt8,
            definition    String
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(ts)
        ORDER BY (cluster, datname, schemaname, relname, indexrelname, ts)
        TTL ts + INTERVAL {metrics_days} DAY
        """,
        # Рекомендации по индексам (создать/удалить/дубликат).
        """
        CREATE TABLE IF NOT EXISTS recommendations (
            ts        DateTime,
            cluster   LowCardinality(String),
            kind      LowCardinality(String),
            datname   LowCardinality(String),
            tablename String,
            indexname String,
            columns   Array(String),
            reason    String,
            ddl       String,
            queryids  Array(Int64)
        ) ENGINE = ReplacingMergeTree(ts)
        ORDER BY (cluster, kind, datname, tablename, ddl)
        """,
    ]


def ensure_schema(cfg: Config) -> None:
    """Создаёт БД и таблицы, если их нет."""
    ch = cfg.clickhouse
    admin = clickhouse_connect.get_client(
        host=ch["host"], port=int(ch["port"]),
        username=ch["user"], password=ch["password"],
        secure=bool(ch.get("secure", False)),
    )
    admin.command(f"CREATE DATABASE IF NOT EXISTS {ch['database']}")
    admin.close()

    cli = make_client(cfg)
    for ddl in schema_ddl(cfg):
        cli.command(ddl)
    cli.close()
    log.info("Схема ClickHouse готова (%s)", ch["database"])


def insert_rows(cfg: Config, table: str, columns: Sequence[str],
                rows: Sequence[Sequence[Any]]) -> None:
    if not rows:
        return
    cli = thread_client(cfg)
    cli.insert(table, rows, column_names=list(columns))
