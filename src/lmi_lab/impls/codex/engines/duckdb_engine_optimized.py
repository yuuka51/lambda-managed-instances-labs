from __future__ import annotations

from lmi_lab.core.config import RunConfig
from lmi_lab.core.metrics import Timer, peak_rss_mb
from lmi_lab.core.schema import CompareResult


def run_compare(config: RunConfig) -> CompareResult:
    try:
        import duckdb
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("duckdb is not installed") from exc

    timer = Timer()
    con = duckdb.connect()
    
    # Primary keys for join
    pk_cols = config.primary_keys
    pk_join = " AND ".join(f"b.{col} = a.{col}" for col in pk_cols)
    pk_concat = " || '||' || ".join(f"COALESCE(b.{col}, a.{col}, '')" for col in pk_cols)
    
    # Normalize function (simplified for strictness A)
    # In production, implement full normalization logic as UDF
    
    # SQL-based diff calculation
    diff_sql = f"""
    WITH before AS (
        SELECT * FROM read_csv_auto(?, delim=?, header=true, all_varchar=true)
    ),
    after AS (
        SELECT * FROM read_csv_auto(?, delim=?, header=true, all_varchar=true)
    ),
    -- Full outer join to find all changes
    joined AS (
        SELECT 
            {pk_concat} as key,
            b.* as before_row,
            a.* as after_row
        FROM before b
        FULL OUTER JOIN after a ON {pk_join}
    ),
    -- Calculate diff status
    diffs AS (
        SELECT
            key,
            CASE
                WHEN before_row IS NULL THEN 'INSERT'
                WHEN after_row IS NULL THEN 'DELETE'
                ELSE 'UPDATE'
            END as status,
            '' as changed_columns  -- Simplified for now
        FROM joined
        WHERE before_row IS NULL OR after_row IS NULL OR before_row != after_row
    )
    SELECT * FROM diffs ORDER BY key
    """
    
    result_df = con.execute(
        diff_sql,
        [str(config.before), config.delimiter, str(config.after), config.delimiter]
    ).df()
    
    # Count rows directly in SQL
    count_sql = """
    SELECT 
        (SELECT COUNT(*) FROM read_csv_auto(?, delim=?, header=true)) as before_count,
        (SELECT COUNT(*) FROM read_csv_auto(?, delim=?, header=true)) as after_count
    """
    counts = con.execute(
        count_sql,
        [str(config.before), config.delimiter, str(config.after), config.delimiter]
    ).fetchone()
    
    # Write results
    diff_path = config.outdir / "diff_codex_duckdb.csv"
    diff_path.parent.mkdir(parents=True, exist_ok=True)
    result_df.to_csv(diff_path, index=False)
    
    return CompareResult(
        engine="duckdb",
        diff_path=diff_path,
        rows_before=counts[0],
        rows_after=counts[1],
        rows_diff=len(result_df),
        elapsed_seconds=timer.elapsed(),
        peak_rss_mb=peak_rss_mb(),
        stats={"strictness": config.strictness, "optimized": True},
    )
