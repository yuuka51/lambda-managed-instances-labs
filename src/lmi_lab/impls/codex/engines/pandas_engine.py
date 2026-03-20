from __future__ import annotations

import glob
from pathlib import Path

from lmi_lab.core.config import RunConfig
from lmi_lab.core.metrics import Timer, peak_rss_mb
from lmi_lab.core.schema import CompareResult

from .common import diff_rows, normalize_rows, write_diff


def run_compare(config: RunConfig) -> CompareResult:
    try:
        import pandas as pd
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("pandas is not installed") from exc

    timer = Timer()
    
    # Handle wildcard patterns and multiple files
    before_path = str(config.before)
    after_path = str(config.after)
    
    # Expand wildcards if present
    before_files = sorted(glob.glob(before_path)) if '*' in before_path else [before_path]
    after_files = sorted(glob.glob(after_path)) if '*' in after_path else [after_path]
    
    if not before_files:
        raise FileNotFoundError(f"No files found matching: {before_path}")
    if not after_files:
        raise FileNotFoundError(f"No files found matching: {after_path}")
    
    # Read all files and concatenate
    before_df = pd.concat(
        [pd.read_csv(f, sep=config.delimiter, dtype=str) for f in before_files],
        ignore_index=True
    ).fillna("")
    
    after_df = pd.concat(
        [pd.read_csv(f, sep=config.delimiter, dtype=str) for f in after_files],
        ignore_index=True
    ).fillna("")

    before_rows = normalize_rows(before_df.to_dict(orient="records"), config.strictness)
    after_rows = normalize_rows(after_df.to_dict(orient="records"), config.strictness)
    diffs = diff_rows(before_rows, after_rows, config.primary_keys)

    diff_path = config.outdir / "diff_codex_pandas.csv"
    write_diff(diff_path, diffs)
    return CompareResult(
        engine="pandas",
        diff_path=diff_path,
        rows_before=len(before_rows),
        rows_after=len(after_rows),
        rows_diff=len(diffs),
        elapsed_seconds=timer.elapsed(),
        peak_rss_mb=peak_rss_mb(),
        stats={"strictness": config.strictness},
    )
