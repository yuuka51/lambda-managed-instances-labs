"""Tests for FireDucks engine implementation."""

from pathlib import Path
import tempfile
import pytest

from lmi_lab.core.config import RunConfig
from lmi_lab.impls.codex.engines.fireducks_engine import run_compare


def test_fireducks_engine_basic():
    """Test that fireducks engine can process TSV files and generate diff."""
    # Create a temporary output directory
    with tempfile.TemporaryDirectory() as tmpdir:
        outdir = Path(tmpdir)
        
        config = RunConfig(
            before=Path("tests/fixtures/before_small.tsv"),
            after=Path("tests/fixtures/after_small.tsv"),
            outdir=outdir,
            engines=["fireducks"],
            strictness="A",
            primary_keys=["user_id"],
            list_columns=["tags", "levels", "timestamps"],
            delimiter="\t",
        )
        
        try:
            result = run_compare(config)
        except RuntimeError as exc:
            if "not installed" in str(exc):
                pytest.skip("fireducks is not installed")
            raise
        
        # Verify result structure
        assert result.engine == "fireducks"
        assert result.diff_path == outdir / "diff_codex_fireducks.csv"
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


def test_fireducks_engine_not_installed(monkeypatch):
    """Test that fireducks engine raises RuntimeError when fireducks is not installed."""
    import sys
    
    # Mock fireducks import to fail
    import builtins
    original_import = builtins.__import__
    
    def mock_import(name, *args, **kwargs):
        if name == "fireducks" or name.startswith("fireducks."):
            raise ImportError("No module named 'fireducks'")
        return original_import(name, *args, **kwargs)
    
    monkeypatch.setattr(builtins, "__import__", mock_import)
    
    # Remove fireducks from sys.modules if it exists
    modules_to_remove = [k for k in sys.modules.keys() if k == "fireducks" or k.startswith("fireducks.")]
    for module in modules_to_remove:
        monkeypatch.delitem(sys.modules, module, raising=False)
    
    # Reload the module to trigger the import error
    import importlib
    from lmi_lab.impls.codex.engines import fireducks_engine
    importlib.reload(fireducks_engine)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        config = RunConfig(
            before=Path("tests/fixtures/before_small.tsv"),
            after=Path("tests/fixtures/after_small.tsv"),
            outdir=Path(tmpdir),
            engines=["fireducks"],
            strictness="A",
            primary_keys=["user_id"],
            list_columns=["tags", "levels", "timestamps"],
            delimiter="\t",
        )
        
        with pytest.raises(RuntimeError, match="fireducks is not installed"):
            fireducks_engine.run_compare(config)
