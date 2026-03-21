"""Tests for Pandas engine implementation."""

from pathlib import Path
import tempfile
import pytest

from lmi_lab.core.config import RunConfig
from lmi_lab.impls.engines.pandas_engine import run_compare


def test_pandas_engine_basic():
    """Test that pandas engine can process TSV files and generate diff."""
    # Create a temporary output directory
    with tempfile.TemporaryDirectory() as tmpdir:
        outdir = Path(tmpdir)
        
        config = RunConfig(
            before=Path("tests/fixtures/before_small.tsv"),
            after=Path("tests/fixtures/after_small.tsv"),
            outdir=outdir,
            engines=["pandas"],
            strictness="A",
            primary_keys=["user_id"],
            list_columns=["tags", "levels", "timestamps"],
            delimiter="\t",
        )
        
        try:
            result = run_compare(config)
        except RuntimeError as exc:
            if "not installed" in str(exc):
                pytest.skip("pandas is not installed")
            raise
        
        # Verify result structure
        assert result.engine == "pandas"
        assert result.diff_path == outdir / "diff_pandas.csv"
        assert result.diff_path.exists()
        
        # Verify row counts
        assert result.rows_before > 0
        assert result.rows_after > 0
        assert result.rows_diff > 0
        
        # Verify metrics
        assert result.elapsed_seconds > 0
        assert result.peak_rss_mb > 0
        
        # Verify stats
        assert result.stats["strictness"] == "A"


def test_pandas_engine_not_installed(monkeypatch):
    """Test that pandas engine raises RuntimeError when pandas is not installed."""
    import sys
    
    # Mock pandas import to fail
    import builtins
    original_import = builtins.__import__
    
    def mock_import(name, *args, **kwargs):
        if name == "pandas" or name.startswith("pandas."):
            raise ImportError("No module named 'pandas'")
        return original_import(name, *args, **kwargs)
    
    monkeypatch.setattr(builtins, "__import__", mock_import)
    
    # Remove pandas from sys.modules if it exists
    modules_to_remove = [k for k in sys.modules.keys() if k == "pandas" or k.startswith("pandas.")]
    for module in modules_to_remove:
        monkeypatch.delitem(sys.modules, module, raising=False)
    
    # Reload the module to trigger the import error
    import importlib
    from lmi_lab.impls.engines import pandas_engine
    importlib.reload(pandas_engine)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        config = RunConfig(
            before=Path("tests/fixtures/before_small.tsv"),
            after=Path("tests/fixtures/after_small.tsv"),
            outdir=Path(tmpdir),
            engines=["pandas"],
            strictness="A",
            primary_keys=["user_id"],
            list_columns=["tags", "levels", "timestamps"],
            delimiter="\t",
        )
        
        with pytest.raises(RuntimeError, match="pandas is not installed"):
            pandas_engine.run_compare(config)
