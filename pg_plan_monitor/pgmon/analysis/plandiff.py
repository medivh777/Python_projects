"""Анализ планов выполнения: фингерпринт, извлечение структуры, сравнение.

Фингерпринт — sha1 от структурного скелета плана (типы узлов, отношения,
индексы, типы соединений) без стоимостей и оценок строк, поэтому косметические
изменения cost не создают «нового» плана, а смена Index Scan → Seq Scan — создаёт.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field

SCAN_NODES = {
    "Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Heap Scan",
    "Bitmap Index Scan", "Tid Scan", "Sample Scan", "Foreign Scan",
    "CTE Scan", "Subquery Scan", "Function Scan", "Values Scan",
}
JOIN_NODES = {"Nested Loop", "Hash Join", "Merge Join"}


@dataclass
class PlanInfo:
    fingerprint: str = ""
    total_cost: float = 0.0
    startup_cost: float = 0.0
    tables: list[str] = field(default_factory=list)
    indexes: list[str] = field(default_factory=list)
    seq_scan_tables: list[str] = field(default_factory=list)
    node_types: list[str] = field(default_factory=list)
    # relation -> набор индексов, которыми она читается
    index_by_table: dict[str, set[str]] = field(default_factory=dict)
    joins: list[str] = field(default_factory=list)
    # список Seq Scan узлов с фильтрами — сырьё для рекомендаций индексов
    seq_scan_filters: list[dict] = field(default_factory=list)


def _skeleton(node: dict) -> dict:
    """Структурный скелет узла без стоимостей."""
    sk: dict = {"t": node.get("Node Type", "?")}
    for key, short in (
        ("Relation Name", "rel"),
        ("Index Name", "idx"),
        ("Join Type", "join"),
        ("Strategy", "strat"),
        ("Parent Relationship", "pr"),
        ("Scan Direction", "dir"),
    ):
        if node.get(key):
            sk[short] = node[key]
    children = node.get("Plans") or []
    if children:
        sk["c"] = [_skeleton(ch) for ch in children]
    return sk


def analyze_plan(plan_json: dict | list) -> PlanInfo:
    """Разбирает вывод EXPLAIN (FORMAT JSON) — dict или [ {"Plan": ...} ]."""
    if isinstance(plan_json, list):
        plan_json = plan_json[0]
    root = plan_json.get("Plan", plan_json)

    info = PlanInfo()
    info.total_cost = float(root.get("Total Cost", 0.0))
    info.startup_cost = float(root.get("Startup Cost", 0.0))

    tables: set[str] = set()
    indexes: set[str] = set()
    seq_tables: set[str] = set()
    node_types: set[str] = set()

    def walk(node: dict) -> None:
        ntype = node.get("Node Type", "?")
        node_types.add(ntype)
        rel = node.get("Relation Name")
        idx = node.get("Index Name")
        if rel:
            tables.add(rel)
        if idx:
            indexes.add(idx)
            if rel:
                info.index_by_table.setdefault(rel, set()).add(idx)
        if ntype == "Seq Scan" and rel:
            seq_tables.add(rel)
            info.seq_scan_filters.append({
                "table": rel,
                "schema": node.get("Schema", ""),
                "filter": node.get("Filter", ""),
                "plan_rows": node.get("Plan Rows", 0),
                "total_cost": node.get("Total Cost", 0.0),
            })
        if ntype in JOIN_NODES:
            info.joins.append(ntype)
        for ch in node.get("Plans") or []:
            walk(ch)

    walk(root)

    info.tables = sorted(tables)
    info.indexes = sorted(indexes)
    info.seq_scan_tables = sorted(seq_tables)
    info.node_types = sorted(node_types)

    canon = json.dumps(_skeleton(root), sort_keys=True, ensure_ascii=False)
    info.fingerprint = hashlib.sha1(canon.encode("utf-8")).hexdigest()[:16]
    return info


@dataclass
class PlanChange:
    severity: str          # info | warning | critical
    kind: str              # index_to_seqscan | index_changed | join_changed | cost_increase | structure_changed
    description: str


def compare_plans(old: PlanInfo, new: PlanInfo,
                  cost_ratio_warning: float = 5.0) -> list[PlanChange]:
    """Сравнивает два плана одного запроса и возвращает список изменений.

    Пустой список — планы структурно совпадают и стоимость стабильна.
    """
    if old.fingerprint == new.fingerprint:
        return []

    changes: list[PlanChange] = []

    # 1. Индексный доступ заменился на полный проход таблицы — самое опасное.
    for rel in new.seq_scan_tables:
        if rel not in old.seq_scan_tables and rel in old.index_by_table:
            lost = ", ".join(sorted(old.index_by_table[rel]))
            changes.append(PlanChange(
                "critical", "index_to_seqscan",
                f"Таблица «{rel}»: раньше читалась по индексу ({lost}), "
                f"теперь Seq Scan (полное сканирование)",
            ))

    # 2. Наоборот: Seq Scan заменился индексом — полезно знать, но это улучшение.
    for rel in old.seq_scan_tables:
        if rel not in new.seq_scan_tables and rel in new.index_by_table:
            got = ", ".join(sorted(new.index_by_table[rel]))
            changes.append(PlanChange(
                "info", "seqscan_to_index",
                f"Таблица «{rel}»: Seq Scan заменился индексным доступом ({got})",
            ))

    # 3. Смена индекса на той же таблице.
    for rel, new_idx in new.index_by_table.items():
        old_idx = old.index_by_table.get(rel)
        if old_idx and old_idx != new_idx and rel not in {
            c.description for c in changes
        }:
            changes.append(PlanChange(
                "warning", "index_changed",
                f"Таблица «{rel}»: индекс изменился {sorted(old_idx)} → {sorted(new_idx)}",
            ))

    # 4. Смена метода соединения.
    if sorted(old.joins) != sorted(new.joins):
        changes.append(PlanChange(
            "warning", "join_changed",
            f"Метод соединения изменился: {sorted(set(old.joins))} → {sorted(set(new.joins))}",
        ))

    # 5. Существенный рост стоимости.
    if old.total_cost > 0 and new.total_cost / old.total_cost >= cost_ratio_warning:
        changes.append(PlanChange(
            "warning", "cost_increase",
            f"Оценочная стоимость выросла в {new.total_cost / old.total_cost:.1f} раза "
            f"({old.total_cost:.0f} → {new.total_cost:.0f})",
        ))

    if not changes:
        changes.append(PlanChange(
            "info", "structure_changed",
            "Структура плана изменилась (без смены индексов и соединений)",
        ))
    return changes
