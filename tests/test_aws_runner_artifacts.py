from __future__ import annotations

from pathlib import Path

from lmi_lab.runners import aws_runner


def test_run_event_uploads_only_current_compare_artifacts(monkeypatch, tmp_path: Path) -> None:
    uploaded: list[tuple[str, str, str]] = []

    def fake_download_file(bucket: str, key: str, dest: Path) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text("user_id\ttags\tlevels\ttimestamps\top_type\n1\ta\t1\t2024-01-01 00:00:00\tupsert\n", encoding="utf-8")

    def fake_upload_file(bucket: str, key: str, src: Path) -> None:
        uploaded.append((bucket, key, src.name))

    class DummyResult:
        diff_path = Path("/tmp/lmi_lab/out/diff_codex_duckdb.csv")

        def to_dict(self) -> dict:
            return {"engine": "duckdb", "diff_path": str(self.diff_path)}

    def fake_run_compare(_cfg, _engine):
        diff_path = Path("/tmp/lmi_lab/out/diff_codex_duckdb.csv")
        diff_path.write_text("kind,user_id\n", encoding="utf-8")
        return DummyResult()

    monkeypatch.setattr(aws_runner, "download_file", fake_download_file)
    monkeypatch.setattr(aws_runner, "upload_file", fake_upload_file)
    monkeypatch.setattr(aws_runner, "run_compare", fake_run_compare)

    outdir = Path("/tmp/lmi_lab/out")
    outdir.mkdir(parents=True, exist_ok=True)
    stale = outdir / "diff_stale.csv"
    stale.write_text("stale", encoding="utf-8")

    response = aws_runner.run_event(
        {
            "action": "compare",
            "bucket": "b",
            "before_key": "before.tsv",
            "after_key": "after.tsv",
            "out_prefix": "results/test",
            "engines": "duckdb",
        }
    )

    assert response["ok"] is True
    uploaded_names = [name for _, _, name in uploaded]
    assert uploaded_names == ["summary.json", "diff_codex_duckdb.csv"]
    assert not stale.exists()
