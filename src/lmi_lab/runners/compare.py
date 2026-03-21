from __future__ import annotations

from lmi_lab.core.config import RunConfig
from lmi_lab.core.schema import CompareResult
from lmi_lab.impls import ENGINE_MAP
from lmi_lab.reference_impl import run_compare as reference_compare

ALL_ENGINES = ["duckdb", "pandas", "fireducks", "polars", "spark"]


def resolve_engines(value: str | list[str]) -> list[str]:
    if isinstance(value, list):
        return value
    v = value.strip().lower()
    if v == "all":
        return ALL_ENGINES
    return [x.strip() for x in v.split(",") if x.strip()]


def run_compare(config: RunConfig, engine: str) -> CompareResult:
    config.validate()
    engine = engine.lower()
    if engine == "reference":
        return reference_compare(config)

    mapper = ENGINE_MAP
    if engine not in mapper:
        raise ValueError(f"unsupported engine: {engine}")
    return mapper[engine](config)
