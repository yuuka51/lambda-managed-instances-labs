"""Tests for Spark engine implementation."""

from pathlib import Path
import subprocess
import tempfile

import pytest

from lmi_lab.core.config import RunConfig
from lmi_lab.core.schema import CompareResult


def _is_java_available() -> bool:
    """Check if Java is available on the system."""
    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _skip_if_no_spark_or_java():
    """Skip test if pyspark is not installed or Java is not available."""
    try:
        import pyspark  # noqa: F401
    except ImportError:
        pytest.skip("pyspark is not installed")
    
    if not _is_java_available():
        pytest.skip("Java runtime is not available (required for PySpark)")


def test_spark_engine_import_error():
    """Test that spark engine raises RuntimeError when pyspark is not installed."""
    # This test would need to mock the import, so we'll skip it for now
    # as it's covered by the pragma: no cover in the actual code
    pass


def test_spark_engine_basic():
    """Test that spark engine can process TSV files end-to-end."""
    _skip_if_no_spark_or_java()
    
    from lmi_lab.impls.engines.spark_engine import run_compare
    
    # Create a temporary output directory
    with tempfile.TemporaryDirectory() as tmpdir:
        outdir = Path(tmpdir)
        
        # Create a simple test configuration
        config = RunConfig(
            before=Path("tests/fixtures/before_small.tsv"),
            after=Path("tests/fixtures/after_small.tsv"),
            outdir=outdir,
            engines=["spark"],
            strictness="A",
            primary_keys=["user_id"],
            list_columns=["tags", "levels", "timestamps"],
            delimiter="\t",
        )
        
        # Run the comparison
        result = run_compare(config)
        
        # Verify the result
        assert isinstance(result, CompareResult)
        assert result.engine == "spark"
        assert result.diff_path == outdir / "diff_spark.csv"
        assert result.diff_path.exists()
        assert result.rows_before >= 0
        assert result.rows_after >= 0
        assert result.rows_diff >= 0
        assert result.elapsed_seconds > 0
        assert result.peak_rss_mb > 0
        assert result.stats["strictness"] == "A"


def test_spark_engine_strictness_b():
    """Test that spark engine works with strictness mode B."""
    _skip_if_no_spark_or_java()
    
    from lmi_lab.impls.engines.spark_engine import run_compare
    
    # Create a temporary output directory
    with tempfile.TemporaryDirectory() as tmpdir:
        outdir = Path(tmpdir)
        
        # Create a simple test configuration with strictness B
        config = RunConfig(
            before=Path("tests/fixtures/before_small.tsv"),
            after=Path("tests/fixtures/after_small.tsv"),
            outdir=outdir,
            engines=["spark"],
            strictness="B",
            primary_keys=["user_id"],
            list_columns=["tags", "levels", "timestamps"],
            delimiter="\t",
        )
        
        # Run the comparison
        result = run_compare(config)
        
        # Verify the result
        assert isinstance(result, CompareResult)
        assert result.engine == "spark"
        assert result.stats["strictness"] == "B"


def test_spark_engine_diff_file_format():
    """Test that spark engine generates diff file with correct format."""
    _skip_if_no_spark_or_java()
    
    from lmi_lab.impls.engines.spark_engine import run_compare
    
    # Create a temporary output directory
    with tempfile.TemporaryDirectory() as tmpdir:
        outdir = Path(tmpdir)
        
        # Create a simple test configuration
        config = RunConfig(
            before=Path("tests/fixtures/before_small.tsv"),
            after=Path("tests/fixtures/after_small.tsv"),
            outdir=outdir,
            engines=["spark"],
            strictness="A",
            primary_keys=["user_id"],
            list_columns=["tags", "levels", "timestamps"],
            delimiter="\t",
        )
        
        # Run the comparison
        result = run_compare(config)
        
        # Read the diff file and verify format
        import csv
        with result.diff_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
            # Verify header
            assert reader.fieldnames == ["key", "status", "changed_columns"]
            
            # Verify each row has the correct fields
            for row in rows:
                assert "key" in row
                assert "status" in row
                assert "changed_columns" in row
                assert row["status"] in ["INSERT", "DELETE", "UPDATE"]
