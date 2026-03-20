from __future__ import annotations

from typing import Any


def normalize_null(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_list_compat(value: Any, delimiter: str = "|") -> str:
    raw = normalize_null(value)
    if raw == "":
        return ""
    parts = [p.strip() for p in raw.split(delimiter) if p.strip()]
    return delimiter.join(sorted(parts))


def _parse_int_or_str(token: str) -> tuple[int, int | str]:
    try:
        return (0, int(token))
    except ValueError:
        return (1, token)


def normalize_list_strict(value: Any, delimiter: str = "|") -> str:
    raw = normalize_null(value)
    if raw == "":
        return ""
    tokens = [p.strip() for p in raw.split(delimiter) if p.strip()]
    keyed = [(_parse_int_or_str(token), token) for token in tokens]
    keyed.sort(key=lambda x: x[0])
    return delimiter.join(str(v[0][1]) if v[0][0] == 0 else v[1] for v in keyed)


def normalize_timestamps_strict(value: Any, delimiter: str = "|") -> str:
    raw = normalize_null(value)
    if raw == "":
        return ""
    return delimiter.join(sorted(t.strip() for t in raw.split(delimiter) if t.strip()))


def normalize_op_type(value: Any) -> str:
    op = normalize_null(value).upper()
    if op == "I":
        return "U"
    return op


def should_exclude_row(row: dict[str, Any]) -> bool:
    return normalize_op_type(row.get("op_type", "")) == "D"


def normalize_row(row: dict[str, Any], strictness: str) -> dict[str, str]:
    strictness = strictness.upper()
    norm = {k: normalize_null(v) for k, v in row.items()}
    norm["op_type"] = normalize_op_type(norm.get("op_type", ""))

    if strictness == "A":
        norm["tags"] = normalize_list_compat(norm.get("tags", ""))
        norm["levels"] = normalize_list_compat(norm.get("levels", ""))
        norm["timestamps"] = normalize_list_compat(norm.get("timestamps", ""))
        return norm

    norm["tags"] = normalize_list_strict(norm.get("tags", ""))
    norm["levels"] = normalize_list_strict(norm.get("levels", ""))
    norm["timestamps"] = normalize_timestamps_strict(norm.get("timestamps", ""))
    return norm
