from __future__ import annotations

import glob

from lmi_lab.core.config import RunConfig
from lmi_lab.core.metrics import Timer, peak_rss_mb
from lmi_lab.core.schema import CompareResult


def run_compare(config: RunConfig) -> CompareResult:
    try:
        import pandas as pd
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("pandas is not installed") from exc

    timer = Timer()
    
    # Handle wildcard patterns
    before_path = str(config.before)
    after_path = str(config.after)
    
    before_files = sorted(glob.glob(before_path)) if '*' in before_path else [before_path]
    after_files = sorted(glob.glob(after_path)) if '*' in after_path else [after_path]
    
    if not before_files:
        raise FileNotFoundError(f"No files found matching: {before_path}")
    if not after_files:
        raise FileNotFoundError(f"No files found matching: {after_path}")
    
    # Process in chunks to reduce memory usage
    chunk_size = 100000
    pk_cols = config.primary_keys
    
    # Build index of before data using chunks
    before_index = {}
    rows_before = 0
    
    for file in before_files:
        for chunk in pd.read_csv(
            file,
            sep=config.delimiter,
            dtype=str,
            chunksize=chunk_size,
            na_filter=False
        ):
            rows_before += len(chunk)
            # Create composite key
            chunk['__key__'] = chunk[pk_cols].apply(lambda x: '||'.join(x), axis=1)
            # Store row hashes for comparison
            for idx, row in chunk.iterrows():
                key = row['__key__']
                before_index[key] = hash(tuple(row.drop('__key__').items()))
    
    # Process after data and find differences
    after_index = {}
    rows_after = 0
    diffs = []
    
    for file in after_files:
        for chunk in pd.read_csv(
            file,
            sep=config.delimiter,
            dtype=str,
            chunksize=chunk_size,
            na_filter=False
        ):
            rows_after += len(chunk)
            chunk['__key__'] = chunk[pk_cols].apply(lambda x: '||'.join(x), axis=1)
            
            for idx, row in chunk.iterrows():
                key = row['__key__']
                row_hash = hash(tuple(row.drop('__key__').items()))
                after_index[key] = row_hash
                
                if key not in before_index:
                    diffs.append({'key': key, 'status': 'INSERT', 'changed_columns': ''})
                elif before_index[key] != row_hash:
                    diffs.append({'key': key, 'status': 'UPDATE', 'changed_columns': ''})
    
    # Find deletions
    for key in before_index:
        if key not in after_index:
            diffs.append({'key': key, 'status': 'DELETE', 'changed_columns': ''})
    
    # Sort and write results
    diffs_df = pd.DataFrame(diffs).sort_values('key')
    
    diff_path = config.outdir / "diff_pandas.csv"
    diff_path.parent.mkdir(parents=True, exist_ok=True)
    diffs_df.to_csv(diff_path, index=False)
    
    return CompareResult(
        engine="pandas",
        diff_path=diff_path,
        rows_before=rows_before,
        rows_after=rows_after,
        rows_diff=len(diffs),
        elapsed_seconds=timer.elapsed(),
        peak_rss_mb=peak_rss_mb(),
        stats={"strictness": config.strictness, "optimized": True},
    )
