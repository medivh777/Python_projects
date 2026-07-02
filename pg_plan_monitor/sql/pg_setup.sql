-- Настройка наблюдаемого PostgreSQL для pg_plan_monitor.
-- Выполнять суперпользователем в каждой наблюдаемой БД (расширение)
-- и один раз на кластер (роль).

-- 1. Расширение pg_stat_statements (также требует в postgresql.conf:
--      shared_preload_libraries = 'pg_stat_statements'
--      pg_stat_statements.track = all
--      compute_query_id = on            -- query_id в pg_stat_activity (PG14+)
--      track_io_timing = on             -- время дискового I/O в статистике
--    и перезапуска сервера)
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- 2. Роль мониторинга: только чтение статистики, данные таблиц недоступны.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pgmon') THEN
        CREATE ROLE pgmon LOGIN PASSWORD 'pgmon';
    END IF;
END $$;

-- pg_monitor даёт доступ к pg_stat_statements, pg_stat_activity (полные
-- тексты запросов чужих сессий), pg_locks и прочим stat-представлениям.
GRANT pg_monitor TO pgmon;

-- 3. Для снятия планов (EXPLAIN) роли нужно право читать схему.
--    EXPLAIN не выполняет запрос, но требует SELECT-привилегий на таблицы.
--    Выдайте в каждой наблюдаемой БД:
GRANT pg_read_all_data TO pgmon;

-- 4. Разрешить подключение к наблюдаемым БД.
-- GRANT CONNECT ON DATABASE mydb TO pgmon;
