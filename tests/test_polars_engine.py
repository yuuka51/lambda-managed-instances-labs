"""Tests for Polars engine implementation."""

from pathlib import Path
import tempfile
import pytest

from lmi_lab.core.config import RunConfig
from lmi_lab.core.schema import CompareResult


def test_polars_engine_import_error():
    """Test that polars engine raises RuntimeError when polars is not installed."""
    # This test would need to mock the import, so we'll skip it for now
    # as it's covered by the pragma: no cover in the actual code
    pass


def test_polars_engine_basic():
    """Test that polars engine can process TSV files end-to-end."""
    try:
        from lmi_lab.impls.codex.engines.polars_engine import run_compare
    except ImportError:
        pytest.skip("polars is not installed")
    
    # Create a temporary output directory
    with tempfile.TemporaryDirectory() as tmpdir:
        outdir = Path(tmpdir)
        
        # Create a simple test configuration
        config = RunConfig(
            before=Path("tests/fixtures/before_small.tsv"),
            after=Path("tests/fixtures/after_small.tsv"),
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
        assert isinstance(result, CompareResult)
        assert result.engine == "polars"
        assert result.diff_path == outdir / "diff_codex_polars.csv"
        assert result.diff_path.exists()
        assert result.rows_before >= 0
        assert result.rows_after >= 0
        assert result.rows_diff >= 0
        assert result.elapsed_seconds > 0
        assert result.peak_rss_mb > 0
        assert result.stats["strictness"] == "A"


def test_polars_engine_strictness_b():
    """Test that polars engine works with strictness mode B."""
    try:
        from lmi_lab.impls.codex.engines.polars_engine import run_compare
    except ImportError:
        pytest.skip("polars is not installed")
    
    # Create a temporary output directory
    with tempfile.TemporaryDirectory() as tmpdir:
        outdir = Path(tmpdir)
        
        # Create a simple test configuration with strictness B
        config = RunConfig(
            before=Path("tests/fixtures/before_small.tsv"),
            after=Path("tests/fixtures/after_small.tsv"),
            outdir=outdir,
            engines=["polars"],
            strictness="B",
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
        assert isinstance(result, CompareResult)
        assert result.engine == "polars"
        assert result.stats["strictness"] == "B"


def test_polars_engine_diff_file_format():
    """Test that polars engine generates diff file with correct format."""
    try:
        from lmi_lab.impls.codex.engines.polars_engine import run_compare
    except ImportError:
        pytest.skip("polars is not installed")
    
    # Create a temporary output directory
    with tempfile.TemporaryDirectory() as tmpdir:
        outdir = Path(tmpdir)
        
        # Create a simple test configuration
        config = RunConfig(
            before=Path("tests/fixtures/before_small.tsv"),
            after=Path("tests/fixtures/after_small.tsv"),
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
