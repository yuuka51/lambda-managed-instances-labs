from __future__ import annotations

from pathlib import Path
import shutil

from lmi_lab.core.config import RunConfig
from lmi_lab.core.io_s3 import download_file, upload_file
from lmi_lab.core.result_writer import write_json
from lmi_lab.runners.benchmark import run_benchmark
from lmi_lab.runners.compare import resolve_engines, run_compare


def run_event(event: dict) -> dict:
    action = event.get("action", "compare")
    bucket = event["bucket"]
    before_key = event["before_key"]
    after_key = event["after_key"]
    out_prefix = event.get("out_prefix", "results/run000/lambda/tmp")

    workdir = Path("/tmp/lmi_lab")
    before = workdir / "before.tsv"
    after = workdir / "after.tsv"
    outdir = workdir / "out"
    shutil.rmtree(outdir, ignore_errors=True)
    outdir.mkdir(parents=True, exist_ok=True)

    download_file(bucket, before_key, before)
    download_file(bucket, after_key, after)

    cfg = RunConfig(
        before=before,
        after=after,
        outdir=outdir,
        engines=resolve_engines(event.get("engines", "duckdb")),
        strictness=event.get("strictness", "A"),
        action=action,
        memray=bool(event.get("memray", False)),
    )

    def run_action() -> tuple[dict, list[Path]]:
        paths = [outdir / "summary.json"]
        if action == "benchmark":
            results = run_benchmark(cfg)
            payload = {"action": action, "results": results}
            paths.extend([outdir / "benchmark.json", outdir / "benchmark.md"])
            paths.extend(
                Path(row["diff_path"])
                for row in results
                if row.get("success") and row.get("diff_path")
            )
            return payload, paths

        result = run_compare(cfg, cfg.engines[0])
        payload = {"action": action, "result": result.to_dict()}
        paths.append(result.diff_path)
        return payload, paths

    if cfg.memray:
        memray_path = outdir / "memray.bin"
        try:
            import memray  # type: ignore[import-not-found]
        except Exception as exc:  # noqa: BLE001
            payload, artifact_paths = run_action()
            payload["memray_status"] = "failed"
            payload["memray_error"] = str(exc)
        else:
            with memray.Tracker(str(memray_path)):
                payload, artifact_paths = run_action()
            if memray_path.exists() and memray_path.stat().st_size > 0:
                payload["memray_status"] = "ok"
                artifact_paths.append(memray_path)
            else:
                payload["memray_status"] = "failed"
                payload["memray_error"] = "memray did not produce a non-empty output file"
    else:
        payload, artifact_paths = run_action()

    summary_path = outdir / "summary.json"
    write_json(summary_path, payload)

    uploads = [path for path in dict.fromkeys(artifact_paths) if path.exists()]

    uploaded = []
    for path in uploads:
        key = f"{out_prefix.rstrip('/')}/{path.name}"
        upload_file(bucket, key, path)
        uploaded.append(key)

    return {"ok": True, "uploaded": uploaded, "payload": payload}
