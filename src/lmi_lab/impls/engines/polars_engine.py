from __future__ import annotations

from lmi_lab.core.config import RunConfig
from lmi_lab.core.metrics import Timer, peak_rss_mb
from lmi_lab.core.schema import CompareResult

from .common import diff_rows, normalize_rows, write_diff


def run_compare(config: RunConfig) -> CompareResult:
    try:
        import polars as pl
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("polars is not installed") from exc

    timer = Timer()
    before_df = pl.scan_csv(str(config.before), separator=config.delimiter, infer_schema_length=0).collect()
    after_df = pl.scan_csv(str(config.after), separator=config.delimiter, infer_schema_length=0).collect()

    before_rows = normalize_rows(before_df.to_dicts(), config.strictness)
    after_rows = normalize_rows(after_df.to_dicts(), config.strictness)
    diffs = diff_rows(before_rows, after_rows, config.primary_keys)

    diff_path = config.outdir / "diff_polars.csv"
    write_diff(diff_path, diffs)
    return CompareResult(
        engine="polars",
        diff_path=diff_path,
        rows_before=len(before_rows),
        rows_after=len(after_rows),
        rows_diff=len(diffs),
        elapsed_seconds=timer.elapsed(),
        peak_rss_mb=peak_rss_mb(),
        stats={"strictness": config.strictness},
    )
