from __future__ import annotations

from lmi_lab.core.config import RunConfig
from lmi_lab.core.result_writer import write_benchmark_md, write_json
from lmi_lab.runners.compare import run_compare


def run_benchmark(config: RunConfig) -> list[dict]:
    rows: list[dict] = []
    for engine in config.engines:
        try:
            result = run_compare(config, engine)
            rows.append(result.to_dict())
        except Exception as exc:  # noqa: BLE001
            rows.append(
                {
                    "engine": engine,
                    "diff_path": "",
                    "rows_before": 0,
                    "rows_after": 0,
                    "rows_diff": 0,
                    "elapsed_seconds": 0.0,
                    "peak_rss_mb": 0.0,
                    "success": False,
                    "reason": str(exc),
                    "stats": {},
                }
            )

    write_json(config.outdir / "benchmark.json", {"results": rows})
    write_benchmark_md(config.outdir / "benchmark.md", rows)
    return rows
