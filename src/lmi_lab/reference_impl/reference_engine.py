from __future__ import annotations

import csv

from lmi_lab.core.config import RunConfig
from lmi_lab.core.metrics import Timer, peak_rss_mb
from lmi_lab.core.normalize import normalize_row, should_exclude_row
from lmi_lab.core.schema import CompareResult


def _build_key(row: dict[str, str], primary_keys: list[str]) -> str:
    return "||".join(row.get(k, "") for k in primary_keys)


def run_compare(config: RunConfig) -> CompareResult:
    timer = Timer()
    with config.before.open("r", encoding="utf-8", newline="") as f:
        before_rows = [
            normalize_row(r, config.strictness)
            for r in csv.DictReader(f, delimiter=config.delimiter)
            if not should_exclude_row(r)
        ]
    with config.after.open("r", encoding="utf-8", newline="") as f:
        after_rows = [
            normalize_row(r, config.strictness)
            for r in csv.DictReader(f, delimiter=config.delimiter)
            if not should_exclude_row(r)
        ]

    before_map = {_build_key(r, config.primary_keys): r for r in before_rows}
    after_map = {_build_key(r, config.primary_keys): r for r in after_rows}

    diffs: list[dict[str, str]] = []
    for key in sorted(set(before_map) | set(after_map)):
        b = before_map.get(key)
        a = after_map.get(key)
        if b is None:
            diffs.append({"key": key, "status": "INSERT", "changed_columns": ""})
            continue
        if a is None:
            diffs.append({"key": key, "status": "DELETE", "changed_columns": ""})
            continue
        changed = sorted(c for c in set(b) | set(a) if b.get(c, "") != a.get(c, ""))
        if changed:
            diffs.append({"key": key, "status": "UPDATE", "changed_columns": "|".join(changed)})

    diff_path = config.outdir / "diff_reference.csv"
    diff_path.parent.mkdir(parents=True, exist_ok=True)
    with diff_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["key", "status", "changed_columns"])
        writer.writeheader()
        writer.writerows(diffs)

    return CompareResult(
        engine="reference",
        diff_path=diff_path,
        rows_before=len(before_rows),
        rows_after=len(after_rows),
        rows_diff=len(diffs),
        elapsed_seconds=timer.elapsed(),
        peak_rss_mb=peak_rss_mb(),
        stats={"small_data_only": True, "strictness": config.strictness},
    )
