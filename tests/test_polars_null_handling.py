"""Test that Polars engine handles null values correctly."""

from pathlib import Path
import tempfile
import csv

import pytest


def test_polars_engine_null_handling():
    """Test that polars engine fills null values with empty strings (Requirement 5.5)."""
    try:
        from lmi_lab.impls.engines.polars_engine import run_compare
        from lmi_lab.core.config import RunConfig
    except ImportError:
        pytest.skip("polars is not installed")
    
    # Create temporary TSV files with null values
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # Create before.tsv with null values
        before_tsv = tmpdir / "before.tsv"
        with before_tsv.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(["user_id", "tags", "levels", "timestamps", "op_type"])
            writer.writerow(["user1", "a|b", "", "t1|t2", "I"])  # Empty string for levels
            writer.writerow(["user2", "", "1|2", "t3|t4", "I"])  # Empty string for tags
        
        # Create after.tsv with null values
        after_tsv = tmpdir / "after.tsv"
        with after_tsv.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(["user_id", "tags", "levels", "timestamps", "op_type"])
            writer.writerow(["user1", "a|b", "", "t1|t2", "U"])  # Empty string for levels
            writer.writerow(["user2", "", "1|2", "t3|t4", "U"])  # Empty string for tags
        
        # Create output directory
        outdir = tmpdir / "output"
        
        # Create configuration
        config = RunConfig(
            before=before_tsv,
            after=after_tsv,
            outdir=outdir,
            engines=["polars"],
            strictness="A",
            primary_keys=["user_id"],
            list_columns=["tags", "levels", "timestamps"],
            delimiter="\t",
        )
        
        # Run the comparison
        try:
            result = run_compare(config)
        except RuntimeError as exc:
            if "not installed" in str(exc):
                pytest.skip("polars is not installed")
            raise
        
        # Verify the result
        assert result.rows_before == 2
        assert result.rows_after == 2
        # No differences because the data is the same (after normalization)
        assert result.rows_diff == 0
        
        # Verify the diff file exists and is empty (no differences)
        assert result.diff_path.exists()
        with result.diff_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 0  # No differences
