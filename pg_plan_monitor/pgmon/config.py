"""Загрузка конфигурации из YAML с переопределением через переменные окружения.

PGMON_CLICKHOUSE_HOST=ch1 переопределит clickhouse.host и т.д.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

DEFAULTS: dict[str, Any] = {
    "clusters": [
        {"name": "main", "dsn": "postgresql://pgmon:pgmon@localhost:5432/postgres", "databases": []}
    ],
    "clickhouse": {
        "host": "localhost",
        "port": 8123,
        "user": "default",
        "password": "",
        "database": "pgmon",
        "secure": False,
    },
    "intervals": {
        "ash_sample": 2,
        "statements": 60,
        "locks": 10,
        "sysstat": 15,
        "plans": 300,
        "table_stats": 600,
        "bloat": 3600,
        "recommend": 1800,
    },
    "plans": {
        "top_n": 50,
        "min_calls": 5,
        "statement_timeout_ms": 3000,
        "use_generic_plan": True,
    },
    "retention": {
        "ash_days": 14,
        "locks_days": 7,
        "metrics_days": 90,
        "plans_days": 90,
    },
    "alerts": {"cost_ratio_warning": 5.0},
    "host_metrics": False,
    "web": {"listen": "0.0.0.0", "port": 8080},
}


@dataclass
class Config:
    raw: dict[str, Any] = field(default_factory=dict)

    def __getitem__(self, key: str) -> Any:
        return self.raw[key]

    def get(self, *path: str, default: Any = None) -> Any:
        node: Any = self.raw
        for p in path:
            if not isinstance(node, dict) or p not in node:
                return default
            node = node[p]
        return node

    @property
    def clusters(self) -> list[dict[str, Any]]:
        return self.raw["clusters"]

    @property
    def clickhouse(self) -> dict[str, Any]:
        return self.raw["clickhouse"]

    @property
    def intervals(self) -> dict[str, int]:
        return self.raw["intervals"]

    @property
    def retention(self) -> dict[str, int]:
        return self.raw["retention"]


def _deep_merge(base: dict, override: dict) -> dict:
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _apply_env(cfg: dict) -> dict:
    """PGMON_<SECTION>_<KEY> → cfg[section][key] (для плоских секций-словарей)."""
    for name, value in os.environ.items():
        if not name.startswith("PGMON_"):
            continue
        parts = name[len("PGMON_"):].lower().split("_", 1)
        if len(parts) != 2:
            continue
        section, key = parts
        if isinstance(cfg.get(section), dict) and key in cfg[section]:
            old = cfg[section][key]
            if isinstance(old, bool):
                cfg[section][key] = value.lower() in ("1", "true", "yes", "on")
            elif isinstance(old, int):
                cfg[section][key] = int(value)
            elif isinstance(old, float):
                cfg[section][key] = float(value)
            else:
                cfg[section][key] = value
    return cfg


def load_config(path: str | None = None) -> Config:
    cfg = dict(DEFAULTS)
    candidate = path or os.environ.get("PGMON_CONFIG", "config.yaml")
    p = Path(candidate)
    if p.exists():
        with p.open("r", encoding="utf-8") as fh:
            loaded = yaml.safe_load(fh) or {}
        cfg = _deep_merge(cfg, loaded)
    cfg = _apply_env(cfg)
    return Config(raw=cfg)
