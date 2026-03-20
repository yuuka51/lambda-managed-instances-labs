from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

from lmi_lab.runners import aws_runner


class DummyResult:
    diff_path = Path("/tmp/lmi_lab/out/diff_codex_duckdb.csv")

    def to_dict(self) -> dict:
        return {"engine": "duckdb", "diff_path": str(self.diff_path)}


def test_run_event_memray_missing_reports_failure(monkeypatch) -> None:
    uploaded: list[str] = []

    def fake_download_file(_bucket: str, _key: str, dest: Path) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text("user_id\n1\n", encoding="utf-8")

    def fake_upload_file(_bucket: str, key: str, _src: Path) -> None:
        uploaded.append(key)

    def fake_run_compare(_cfg, _engine):
        DummyResult.diff_path.write_text("key,status,changed_columns\n", encoding="utf-8")
        return DummyResult()

    monkeypatch.setattr(aws_runner, "download_file", fake_download_file)
    monkeypatch.setattr(aws_runner, "upload_file", fake_upload_file)
    monkeypatch.setattr(aws_runner, "run_compare", fake_run_compare)
    monkeypatch.delitem(sys.modules, "memray", raising=False)

    original_import = __import__

    def fake_import(name, *args, **kwargs):
        if name == "memray":
            raise ImportError("No module named 'memray'")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", fake_import)

    response = aws_runner.run_event(
        {
            "action": "compare",
            "bucket": "b",
            "before_key": "before.tsv",
            "after_key": "after.tsv",
            "out_prefix": "results/test",
            "engines": "duckdb",
            "memray": True,
        }
    )

    assert response["payload"]["memray_status"] == "failed"
    assert "No module named 'memray'" in response["payload"]["memray_error"]
    assert all(not key.endswith("memray.bin") for key in uploaded)


def test_run_event_memray_writes_real_artifact(monkeypatch) -> None:
    uploaded: list[str] = []

    def fake_download_file(_bucket: str, _key: str, dest: Path) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text("user_id\n1\n", encoding="utf-8")

    def fake_upload_file(_bucket: str, key: str, _src: Path) -> None:
        uploaded.append(key)

    def fake_run_compare(_cfg, _engine):
        DummyResult.diff_path.write_text("key,status,changed_columns\n", encoding="utf-8")
        return DummyResult()

    class FakeTracker:
        def __init__(self, output_path: str) -> None:
            self.output_path = Path(output_path)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            self.output_path.write_bytes(b"real-memray-data")
            return False

    monkeypatch.setattr(aws_runner, "download_file", fake_download_file)
    monkeypatch.setattr(aws_runner, "upload_file", fake_upload_file)
    monkeypatch.setattr(aws_runner, "run_compare", fake_run_compare)
    monkeypatch.setitem(sys.modules, "memray", SimpleNamespace(Tracker=FakeTracker))

    response = aws_runner.run_event(
        {
            "action": "compare",
            "bucket": "b",
            "before_key": "before.tsv",
            "after_key": "after.tsv",
            "out_prefix": "results/test",
            "engines": "duckdb",
            "memray": True,
        }
    )

    assert response["payload"]["memray_status"] == "ok"
    assert any(key.endswith("memray.bin") for key in uploaded)
