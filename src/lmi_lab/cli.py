from __future__ import annotations

import argparse
from pathlib import Path

from lmi_lab.core.config import RunConfig
from lmi_lab.data_prep.generate import add_generate_parser
from lmi_lab.data_prep.upload_s3 import add_upload_parser
from lmi_lab.runners.benchmark import run_benchmark
from lmi_lab.runners.compare import resolve_engines, run_compare


def _build_run_config(args: argparse.Namespace) -> RunConfig:
    return RunConfig(
        before=args.before,
        after=args.after,
        outdir=args.outdir,
        engines=resolve_engines(args.engines),
        strictness=args.strictness,
        action=args.command,
    )


def _cmd_compare(args: argparse.Namespace) -> int:
    config = _build_run_config(args)
    engine = config.engines[0]
    result = run_compare(config, engine)
    print(result.to_dict())
    return 0


def _cmd_benchmark(args: argparse.Namespace) -> int:
    config = _build_run_config(args)
    rows = run_benchmark(config)
    print({"results": rows})
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lmi_lab")
    sub = parser.add_subparsers(dest="command", required=True)

    add_generate_parser(sub)
    add_upload_parser(sub)

    for name in ("compare", "benchmark"):
        p = sub.add_parser(name)
        p.add_argument("--before", type=Path, required=True)
        p.add_argument("--after", type=Path, required=True)
        p.add_argument("--outdir", type=Path, default=Path("out"))
        p.add_argument("--engines", default="duckdb")
        p.add_argument("--strictness", choices=["A", "B"], default="A")
        p.set_defaults(func=_cmd_compare if name == "compare" else _cmd_benchmark)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    raise SystemExit(args.func(args))
