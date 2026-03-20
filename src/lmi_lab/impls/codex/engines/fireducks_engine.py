from __future__ import annotations

from lmi_lab.core.config import RunConfig
from lmi_lab.core.metrics import Timer, peak_rss_mb
from lmi_lab.core.schema import CompareResult

from .common import diff_rows, normalize_rows, write_diff


def run_compare(config: RunConfig) -> CompareResult:
    try:
        import fireducks.pandas as pd
    except ImportError as exc:
        raise RuntimeError("fireducks is not installed") from exc

    timer = Timer()
    before_df = pd.read_csv(config.before, sep=config.delimiter, dtype=str).fillna("")
    after_df = pd.read_csv(config.after, sep=config.delimiter, dtype=str).fillna("")

    before_rows = normalize_rows(before_df.to_dict(orient="records"), config.strictness)
    after_rows = normalize_rows(after_df.to_dict(orient="records"), config.strictness)
    diffs = diff_rows(before_rows, after_rows, config.primary_keys)

    diff_path = config.outdir / "diff_codex_fireducks.csv"
    write_diff(diff_path, diffs)
    return CompareResult(
        engine="fireducks",
        diff_path=diff_path,
        rows_before=len(before_rows),
        rows_after=len(after_rows),
        rows_diff=len(diffs),
        elapsed_seconds=timer.elapsed(),
        peak_rss_mb=peak_rss_mb(),
        stats={"strictness": config.strictness},
    )
