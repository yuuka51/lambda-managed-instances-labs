from __future__ import annotations

from lmi_lab.core.config import RunConfig
from lmi_lab.core.metrics import Timer, peak_rss_mb
from lmi_lab.core.schema import CompareResult

from .common import diff_rows, normalize_rows, write_diff


def run_compare(config: RunConfig) -> CompareResult:
    try:
        import duckdb
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("duckdb is not installed") from exc

    timer = Timer()
    con = duckdb.connect()
    before_rel = con.sql(
        "SELECT * FROM read_csv_auto(?, delim=?, header=true, all_varchar=true)",
        params=[str(config.before), config.delimiter],
    )
    after_rel = con.sql(
        "SELECT * FROM read_csv_auto(?, delim=?, header=true, all_varchar=true)",
        params=[str(config.after), config.delimiter],
    )
    before_rows = normalize_rows(before_rel.df().fillna("").to_dict(orient="records"), config.strictness)
    after_rows = normalize_rows(after_rel.df().fillna("").to_dict(orient="records"), config.strictness)
    diffs = diff_rows(before_rows, after_rows, config.primary_keys)

    diff_path = config.outdir / "diff_duckdb.csv"
    write_diff(diff_path, diffs)
    return CompareResult(
        engine="duckdb",
        diff_path=diff_path,
        rows_before=len(before_rows),
        rows_after=len(after_rows),
        rows_diff=len(diffs),
        elapsed_seconds=timer.elapsed(),
        peak_rss_mb=peak_rss_mb(),
        stats={"strictness": config.strictness},
    )
