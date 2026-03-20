from pathlib import Path

import pytest

from lmi_lab.core.config import RunConfig
from lmi_lab.reference_impl.reference_engine import run_compare as reference_compare
from lmi_lab.runners.compare import run_compare


@pytest.fixture()
def sample_paths() -> tuple[Path, Path]:
    base = Path("tests/fixtures")
    return base / "before_small.tsv", base / "after_small.tsv"


def test_reference_and_duckdb_diff_match(tmp_path: Path, sample_paths: tuple[Path, Path]) -> None:
    pytest.importorskip("duckdb")
    before, after = sample_paths
    ref_cfg = RunConfig(before=before, after=after, outdir=tmp_path / "ref", strictness="B")
    duck_cfg = RunConfig(before=before, after=after, outdir=tmp_path / "duck", strictness="B")

    ref = reference_compare(ref_cfg)
    duck = run_compare(duck_cfg, "duckdb")

    assert ref.rows_diff == duck.rows_diff
    assert ref.diff_path.read_text(encoding="utf-8") == duck.diff_path.read_text(encoding="utf-8")


def test_reference_and_polars_diff_match(tmp_path: Path, sample_paths: tuple[Path, Path]) -> None:
    pytest.importorskip("polars")
    before, after = sample_paths
    ref_cfg = RunConfig(before=before, after=after, outdir=tmp_path / "ref", strictness="B")
    pl_cfg = RunConfig(before=before, after=after, outdir=tmp_path / "pl", strictness="B")

    ref = reference_compare(ref_cfg)
    pl = run_compare(pl_cfg, "polars")

    assert ref.rows_diff == pl.rows_diff
    assert ref.diff_path.read_text(encoding="utf-8") == pl.diff_path.read_text(encoding="utf-8")


def test_fireducks_optional(tmp_path: Path, sample_paths: tuple[Path, Path]) -> None:
    before, after = sample_paths
    cfg = RunConfig(before=before, after=after, outdir=tmp_path / "fd", strictness="B")
    try:
        run_compare(cfg, "fireducks")
    except RuntimeError as exc:
        if "not installed" in str(exc):
            pytest.skip("fireducks is not installed")
        raise
