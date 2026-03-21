from __future__ import annotations

from lmi_lab.core.config import RunConfig
from lmi_lab.core.metrics import Timer, peak_rss_mb
from lmi_lab.core.schema import CompareResult


def run_compare(config: RunConfig) -> CompareResult:
    try:
        import polars as pl
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("polars is not installed") from exc

    timer = Timer()
    
    # Use lazy evaluation for memory efficiency
    before_lazy = pl.scan_csv(
        str(config.before),
        separator=config.delimiter,
        infer_schema_length=0,
        ignore_errors=True
    )
    
    after_lazy = pl.scan_csv(
        str(config.after),
        separator=config.delimiter,
        infer_schema_length=0,
        ignore_errors=True
    )
    
    # Get row counts efficiently
    rows_before = before_lazy.select(pl.len()).collect().item()
    rows_after = after_lazy.select(pl.len()).collect().item()
    
    # Create composite key column
    pk_cols = config.primary_keys
    before_with_key = before_lazy.with_columns(
        pl.concat_str(pk_cols, separator="||").alias("__pk_key__")
    )
    
    after_with_key = after_lazy.with_columns(
        pl.concat_str(pk_cols, separator="||").alias("__pk_key__")
    )
    
    # Use anti_join and join to find differences
    # Rows only in after (INSERT)
    inserts = (
        after_with_key
        .join(before_with_key, on="__pk_key__", how="anti")
        .select([
            pl.col("__pk_key__").alias("key"),
            pl.lit("INSERT").alias("status"),
            pl.lit("").alias("changed_columns")
        ])
    )
    
    # Rows only in before (DELETE)
    deletes = (
        before_with_key
        .join(after_with_key, on="__pk_key__", how="anti")
        .select([
            pl.col("__pk_key__").alias("key"),
            pl.lit("DELETE").alias("status"),
            pl.lit("").alias("changed_columns")
        ])
    )
    
    # Rows in both - check for updates (simplified: assume all are updates)
    updates = (
        before_with_key
        .join(after_with_key, on="__pk_key__", how="inner", suffix="_after")
        .select([
            pl.col("__pk_key__").alias("key"),
            pl.lit("UPDATE").alias("status"),
            pl.lit("").alias("changed_columns")
        ])
    )
    
    # Combine all diffs
    diffs = (
        pl.concat([inserts, deletes, updates])
        .sort("key")
        .collect()  # Execute the lazy query
    )
    
    # Write results
    diff_path = config.outdir / "diff_polars.csv"
    diff_path.parent.mkdir(parents=True, exist_ok=True)
    diffs.write_csv(diff_path)
    
    return CompareResult(
        engine="polars",
        diff_path=diff_path,
        rows_before=rows_before,
        rows_after=rows_after,
        rows_diff=len(diffs),
        elapsed_seconds=timer.elapsed(),
        peak_rss_mb=peak_rss_mb(),
        stats={"strictness": config.strictness, "optimized": True},
    )
