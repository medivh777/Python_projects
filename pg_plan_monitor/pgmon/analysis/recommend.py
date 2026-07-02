"""Рекомендации по индексам.

Три источника:
  1. CREATE — Seq Scan с фильтром на большой таблице в свежих планах:
     из выражения Filter извлекаются колонки-кандидаты, предлагается индекс.
  2. DROP — индекс, по которому idx_scan не рос за весь период наблюдения,
     не уникальный и не первичный ключ.
  3. DUPLICATE — индекс, чьи колонки являются префиксом другого индекса
     на той же таблице (избыточный).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

# колонка в выражении фильтра: (col = ...), (col > ...), (col ~~ ...), col IS NULL и т.п.
_COL_RE = re.compile(
    r"\(?\s*([a-z_][a-z0-9_]*)(?:\)::[a-z_ ]+)?\s*"
    r"(?:=|<>|>=|<=|>|<|~~\*?|!~~\*?|IS(?:\s+NOT)?\s+(?:NULL|TRUE|FALSE)|= ANY)",
    re.IGNORECASE,
)
# слова, которые матчится как «колонка», но ей не являются
_NOT_COLUMNS = {
    "and", "or", "not", "case", "when", "then", "else", "end",
    "true", "false", "null", "any", "all", "exists", "in", "is",
}

MIN_TABLE_BYTES = 10 * 1024 * 1024   # не рекомендуем индексы на мелких таблицах
MIN_UNUSED_BYTES = 1024 * 1024       # не предлагаем удалять совсем крошечные индексы


def extract_filter_columns(filter_expr: str) -> list[str]:
    """Колонки из выражения Filter плана, в порядке появления, без дублей."""
    if not filter_expr:
        return []
    cols: list[str] = []
    for m in _COL_RE.finditer(filter_expr):
        name = m.group(1).lower()
        if name in _NOT_COLUMNS or name in cols:
            continue
        cols.append(name)
    return cols


_IDX_COLS_RE = re.compile(r"\(([^)]*)\)\s*(?:WHERE|WITH|$)", re.IGNORECASE)


def index_columns(definition: str) -> list[str]:
    """Колонки из pg_get_indexdef: CREATE INDEX ... ON t USING btree (a, b DESC)."""
    m = _IDX_COLS_RE.search(definition or "")
    if not m:
        return []
    cols = []
    for part in m.group(1).split(","):
        token = part.strip().split()[0] if part.strip() else ""
        if token:
            cols.append(token.strip('"').lower())
    return cols


@dataclass
class Recommendation:
    kind: str                    # create_index | drop_index | duplicate_index
    datname: str
    tablename: str
    indexname: str = ""
    columns: list[str] = field(default_factory=list)
    reason: str = ""
    ddl: str = ""
    queryids: list[int] = field(default_factory=list)


def recommend_create(seq_scans: list[dict],
                     table_sizes: dict[tuple[str, str], int],
                     existing_indexes: dict[tuple[str, str], list[list[str]]]
                     ) -> list[Recommendation]:
    """seq_scans: [{datname, table, filter, queryid, plan_rows}, ...]
    table_sizes: (datname, table) -> bytes
    existing_indexes: (datname, table) -> [колонки индекса, ...]
    """
    # (datname, table, tuple(cols)) -> queryids
    grouped: dict[tuple[str, str, tuple[str, ...]], set[int]] = {}
    for s in seq_scans:
        cols = extract_filter_columns(s.get("filter", ""))
        if not cols:
            continue
        key = (s["datname"], s["table"], tuple(cols[:3]))  # максимум 3 колонки
        grouped.setdefault(key, set()).add(int(s.get("queryid", 0)))

    recs: list[Recommendation] = []
    for (datname, table, cols), qids in grouped.items():
        size = table_sizes.get((datname, table), 0)
        if size < MIN_TABLE_BYTES:
            continue
        # индекс с таким префиксом уже есть?
        covered = any(
            list(cols) == existing[: len(cols)]
            for existing in existing_indexes.get((datname, table), [])
        )
        if covered:
            continue
        idx_name = f"idx_{table}_{'_'.join(cols)}"[:63]
        ddl = (f"CREATE INDEX CONCURRENTLY {idx_name} "
               f"ON {table} ({', '.join(cols)});")
        recs.append(Recommendation(
            kind="create_index",
            datname=datname,
            tablename=table,
            indexname=idx_name,
            columns=list(cols),
            reason=(f"Seq Scan по таблице «{table}» ({size / 1024 / 1024:.0f} МБ) "
                    f"с фильтром по колонкам {', '.join(cols)} "
                    f"в {len(qids)} запросе(ах); подходящего индекса нет"),
            ddl=ddl,
            queryids=sorted(qids),
        ))
    return recs


def recommend_drop(index_rows: list[dict]) -> list[Recommendation]:
    """index_rows — по одному на индекс: {datname, schemaname, relname,
    indexrelname, idx_scan_delta, size_bytes, is_unique, is_primary, definition}.
    idx_scan_delta — прирост использования за весь период наблюдения."""
    recs: list[Recommendation] = []
    for r in index_rows:
        if r.get("is_unique") or r.get("is_primary"):
            continue
        if int(r.get("idx_scan_delta", 0)) > 0:
            continue
        if int(r.get("size_bytes", 0)) < MIN_UNUSED_BYTES:
            continue
        full = f"{r['schemaname']}.{r['indexrelname']}"
        recs.append(Recommendation(
            kind="drop_index",
            datname=r["datname"],
            tablename=f"{r['schemaname']}.{r['relname']}",
            indexname=r["indexrelname"],
            columns=index_columns(r.get("definition", "")),
            reason=(f"Индекс «{full}» ({int(r['size_bytes']) / 1024 / 1024:.0f} МБ) "
                    f"не использовался за весь период наблюдения "
                    f"(idx_scan не менялся); замедляет запись и занимает место"),
            ddl=f"DROP INDEX CONCURRENTLY {full};",
        ))
    return recs


def recommend_duplicates(index_rows: list[dict]) -> list[Recommendation]:
    """Ищет индексы, являющиеся префиксом другого индекса той же таблицы."""
    by_table: dict[tuple[str, str, str], list[dict]] = {}
    for r in index_rows:
        by_table.setdefault((r["datname"], r["schemaname"], r["relname"]), []).append(r)

    recs: list[Recommendation] = []
    for (datname, schema, table), idxs in by_table.items():
        for a in idxs:
            if a.get("is_unique") or a.get("is_primary"):
                continue
            cols_a = index_columns(a.get("definition", ""))
            if not cols_a:
                continue
            for b in idxs:
                if a is b:
                    continue
                cols_b = index_columns(b.get("definition", ""))
                if len(cols_b) > len(cols_a) and cols_b[: len(cols_a)] == cols_a:
                    full = f"{schema}.{a['indexrelname']}"
                    recs.append(Recommendation(
                        kind="duplicate_index",
                        datname=datname,
                        tablename=f"{schema}.{table}",
                        indexname=a["indexrelname"],
                        columns=cols_a,
                        reason=(f"Индекс «{full}» ({', '.join(cols_a)}) — префикс "
                                f"индекса «{b['indexrelname']}» "
                                f"({', '.join(cols_b)}); избыточен"),
                        ddl=f"DROP INDEX CONCURRENTLY {full};",
                    ))
                    break
    return recs
