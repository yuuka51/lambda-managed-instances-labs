from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Iterable

from .config import RunConfig


def ensure_outdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_benchmark_md(path: Path, rows: Iterable[dict]) -> None:
    rows = list(rows)
    lines = [
        "# Benchmark Result",
        "",
        "|engine|success|elapsed_sec|peak_rss_mb|rows_before|rows_after|rows_diff|reason|",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        lines.append(
            f"|{row['engine']}|{row['success']}|{row['elapsed_seconds']:.4f}|{row['peak_rss_mb']:.1f}|"
            f"{row['rows_before']}|{row['rows_after']}|{row['rows_diff']}|{row.get('reason','')}|"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def runconfig_to_dict(cfg: RunConfig) -> dict:
    d = asdict(cfg)
    d["before"] = str(cfg.before)
    d["after"] = str(cfg.after)
    d["outdir"] = str(cfg.outdir)
    return d
