from __future__ import annotations

from lmi_lab.core.config import RunConfig
from lmi_lab.core.metrics import Timer, peak_rss_mb
from lmi_lab.core.schema import CompareResult

from .common import diff_rows, normalize_rows, write_diff


def run_compare(config: RunConfig) -> CompareResult:
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("pyspark is not installed") from exc

    timer = Timer()
    spark = SparkSession.builder.master("local[*]").appName("lmi-lab").getOrCreate()
    try:
        before_df = spark.read.option("header", True).option("sep", config.delimiter).csv(str(config.before))
        after_df = spark.read.option("header", True).option("sep", config.delimiter).csv(str(config.after))
        before_rows = normalize_rows([r.asDict(recursive=True) for r in before_df.collect()], config.strictness)
        after_rows = normalize_rows([r.asDict(recursive=True) for r in after_df.collect()], config.strictness)
    finally:
        spark.stop()

    diffs = diff_rows(before_rows, after_rows, config.primary_keys)
    diff_path = config.outdir / "diff_spark.csv"
    write_diff(diff_path, diffs)
    return CompareResult(
        engine="spark",
        diff_path=diff_path,
        rows_before=len(before_rows),
        rows_after=len(after_rows),
        rows_diff=len(diffs),
        elapsed_seconds=timer.elapsed(),
        peak_rss_mb=peak_rss_mb(),
        stats={"strictness": config.strictness, "note": "strictness B uses shared python normalization"},
    )
