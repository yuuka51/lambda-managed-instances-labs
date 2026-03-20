from __future__ import annotations

import csv
from pathlib import Path

from lmi_lab.core.normalize import normalize_row, should_exclude_row


def rows_from_tsv(path: Path, delimiter: str = "\t") -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter=delimiter))


def normalize_rows(rows: list[dict[str, str]], strictness: str) -> list[dict[str, str]]:
    return [normalize_row(row, strictness) for row in rows if not should_exclude_row(row)]


def build_key(row: dict[str, str], primary_keys: list[str]) -> str:
    missing_keys = [k for k in primary_keys if k not in row]
    if missing_keys:
        raise ValueError(f"Row is missing primary-key columns: {', '.join(missing_keys)}")
    return "||".join(row[k] for k in primary_keys)


def diff_rows(
    before_rows: list[dict[str, str]],
    after_rows: list[dict[str, str]],
    primary_keys: list[str],
) -> list[dict[str, str]]:
    bmap = {build_key(r, primary_keys): r for r in before_rows}
    amap = {build_key(r, primary_keys): r for r in after_rows}

    diffs: list[dict[str, str]] = []
    for key in sorted(set(bmap) | set(amap)):
        b = bmap.get(key)
        a = amap.get(key)
        if b is None:
            diffs.append({"key": key, "status": "INSERT", "changed_columns": ""})
            continue
        if a is None:
            diffs.append({"key": key, "status": "DELETE", "changed_columns": ""})
            continue
        changed = sorted(c for c in set(b) | set(a) if b.get(c, "") != a.get(c, ""))
        if changed:
            diffs.append({"key": key, "status": "UPDATE", "changed_columns": "|".join(changed)})
    return diffs


def write_diff(path: Path, diffs: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["key", "status", "changed_columns"])
        writer.writeheader()
        writer.writerows(diffs)
