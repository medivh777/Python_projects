from pgmon.analysis.recommend import (
    MIN_TABLE_BYTES,
    extract_filter_columns,
    index_columns,
    recommend_create,
    recommend_drop,
    recommend_duplicates,
)


def test_extract_filter_columns():
    assert extract_filter_columns("(status = 'new'::text)") == ["status"]
    assert extract_filter_columns(
        "((status = 'new'::text) AND (created_at > now()))"
    ) == ["status", "created_at"]
    assert extract_filter_columns("(customer_id = ANY ('{1,2}'::integer[]))") == ["customer_id"]
    assert extract_filter_columns("(deleted_at IS NULL)") == ["deleted_at"]
    assert extract_filter_columns("") == []


def test_index_columns_parses_indexdef():
    d = "CREATE INDEX idx ON public.orders USING btree (customer_id, created_at DESC)"
    assert index_columns(d) == ["customer_id", "created_at"]
    d2 = "CREATE INDEX p ON t USING btree (a) WHERE deleted_at IS NULL"
    assert index_columns(d2) == ["a"]


def test_recommend_create_for_large_table_without_index():
    seq_scans = [{"datname": "db", "table": "orders",
                  "filter": "(status = 'new'::text)", "queryid": 1}]
    sizes = {("db", "orders"): MIN_TABLE_BYTES * 10}
    recs = recommend_create(seq_scans, sizes, existing_indexes={})
    assert len(recs) == 1
    assert recs[0].kind == "create_index"
    assert recs[0].columns == ["status"]
    assert "CREATE INDEX CONCURRENTLY" in recs[0].ddl


def test_recommend_create_skips_small_tables_and_covered():
    seq_scans = [{"datname": "db", "table": "small",
                  "filter": "(a = 1)", "queryid": 1},
                 {"datname": "db", "table": "big",
                  "filter": "(a = 1)", "queryid": 2}]
    sizes = {("db", "small"): 1024, ("db", "big"): MIN_TABLE_BYTES * 2}
    covered = {("db", "big"): [["a", "b"]]}  # индекс (a, b) покрывает фильтр по a
    assert recommend_create(seq_scans, sizes, covered) == []


def test_recommend_drop_unused_only():
    rows = [
        {"datname": "db", "schemaname": "public", "relname": "t",
         "indexrelname": "unused_idx", "idx_scan_delta": 0,
         "size_bytes": 50 * 1024 * 1024, "is_unique": 0, "is_primary": 0,
         "definition": "CREATE INDEX unused_idx ON public.t USING btree (x)"},
        {"datname": "db", "schemaname": "public", "relname": "t",
         "indexrelname": "used_idx", "idx_scan_delta": 100,
         "size_bytes": 50 * 1024 * 1024, "is_unique": 0, "is_primary": 0,
         "definition": "CREATE INDEX used_idx ON public.t USING btree (y)"},
        {"datname": "db", "schemaname": "public", "relname": "t",
         "indexrelname": "t_pkey", "idx_scan_delta": 0,
         "size_bytes": 50 * 1024 * 1024, "is_unique": 1, "is_primary": 1,
         "definition": "CREATE UNIQUE INDEX t_pkey ON public.t USING btree (id)"},
    ]
    recs = recommend_drop(rows)
    assert [r.indexname for r in recs] == ["unused_idx"]
    assert "DROP INDEX CONCURRENTLY" in recs[0].ddl


def test_recommend_duplicates_prefix():
    rows = [
        {"datname": "db", "schemaname": "public", "relname": "t",
         "indexrelname": "idx_a", "idx_scan_delta": 5, "size_bytes": 10,
         "is_unique": 0, "is_primary": 0,
         "definition": "CREATE INDEX idx_a ON public.t USING btree (a)"},
        {"datname": "db", "schemaname": "public", "relname": "t",
         "indexrelname": "idx_a_b", "idx_scan_delta": 5, "size_bytes": 20,
         "is_unique": 0, "is_primary": 0,
         "definition": "CREATE INDEX idx_a_b ON public.t USING btree (a, b)"},
    ]
    recs = recommend_duplicates(rows)
    assert len(recs) == 1
    assert recs[0].indexname == "idx_a"
