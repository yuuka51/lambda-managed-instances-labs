import json
from pathlib import Path

from lmi_lab.cli import build_parser
from lmi_lab.data_prep.generate import GenerateConfig, run_generate


def test_generate_creates_split_files_and_manifest(tmp_path: Path) -> None:
    outdir = tmp_path / "dataset"
    cfg = GenerateConfig(
        rows=30,
        num_files=4,
        outdir=outdir,
        seed=42,
        mismatch_rate=0.2,
        missing_rate=0.1,
        optype_ratio="U:0.9,I:0.1,D:0.0",
        avg_tags=2,
        avg_levels=2,
        avg_ts=2,
        shuffle_rows=True,
        shuffle_list_elements=True,
        use_gzip=False,
        timestyle="mixed",
    )

    rc = run_generate(cfg)
    assert rc == 0

    before_parts = sorted((outdir / "before").glob("part-*.tsv"))
    after_parts = sorted((outdir / "after").glob("part-*.tsv"))
    assert len(before_parts) == 4
    assert len(after_parts) == 4
    for part in before_parts + after_parts:
        header = part.read_text(encoding="utf-8").splitlines()[0]
        assert header == "user_id\ttags\tlevels\tts\top_type"

    manifest = json.loads((outdir / "manifest.json").read_text())
    assert manifest["rows_requested"] == 30
    assert manifest["num_files"] == 4
    assert manifest["rows_before"] > 0
    assert manifest["rows_after"] > 0


def test_generate_cli_defaults_to_plain_tsv_output() -> None:
    parser = build_parser()
    args = parser.parse_args(["generate", "--outdir", "dataset"])
    assert args.use_gzip is False
