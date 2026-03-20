"""Tests for DuckDB engine implementation."""

from pathlib import Path
import tempfile
import pytest

from lmi_lab.core.config import RunConfig
from lmi_lab.impls.codex.engines.duckdb_engine import run_compare


def test_duckdb_engine_basic():
    """Test that duckdb engine can process TSV files and generate diff."""
    # Create a temporary output directory
    with tempfile.TemporaryDirectory() as tmpdir:
        outdir = Path(tmpdir)
        
        config = RunConfig(
            before=Path("tests/fixtures/before_small.tsv"),
            after=Path("tests/fixtures/after_small.tsv"),
            outdir=outdir,
            engines=["duckdb"],
            strictness="A",
            primary_keys=["user_id"],
            list_columns=["tags", "levels", "timestamps"],
            delimiter="\t",
        )
        
        try:
            result = run_compare(config)
        except RuntimeError as exc:
            if "not installed" in str(exc):
                pytest.skip("duckdb is not installed")
            raise
        
        # Verify result structure
        assert result.engine == "duckdb"
        assert result.diff_path == outdir / "diff_codex_duckdb.csv"
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


def test_duckdb_engine_not_installed(monkeypatch):
    """Test that duckdb engine raises RuntimeError when duckdb is not installed."""
    import sys
    
    # Mock duckdb import to fail
    import builtins
    original_import = builtins.__import__
    
    def mock_import(name, *args, **kwargs):
        if name == "duckdb" or name.startswith("duckdb."):
            raise ImportError("No module named 'duckdb'")
        return original_import(name, *args, **kwargs)
    
    monkeypatch.setattr(builtins, "__import__", mock_import)
    
    # Remove duckdb from sys.modules if it exists
    modules_to_remove = [k for k in sys.modules.keys() if k == "duckdb" or k.startswith("duckdb.")]
    for module in modules_to_remove:
        monkeypatch.delitem(sys.modules, module, raising=False)
    
    # Reload the module to trigger the import error
    import importlib
    from lmi_lab.impls.codex.engines import duckdb_engine
    importlib.reload(duckdb_engine)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        config = RunConfig(
            before=Path("tests/fixtures/before_small.tsv"),
            after=Path("tests/fixtures/after_small.tsv"),
            outdir=Path(tmpdir),
            engines=["duckdb"],
            strictness="A",
            primary_keys=["user_id"],
            list_columns=["tags", "levels", "timestamps"],
            delimiter="\t",
        )
        
        with pytest.raises(RuntimeError, match="duckdb is not installed"):
            duckdb_engine.run_compare(config)
