from __future__ import annotations

import argparse
from pathlib import Path


def run_upload_s3(src: Path, bucket: str, dst_prefix: str, include_manifest: bool = True) -> int:
    try:
        import boto3
    except Exception as e:  # pragma: no cover
        raise RuntimeError("boto3 が必要です。`pip install '.[aws]'` を実行してください。") from e

    s3 = boto3.client("s3")
    norm_prefix = dst_prefix.strip("/")
    files: list[tuple[Path, str]] = []

    for side in ("before", "after"):
        side_dir = src / side
        if not side_dir.exists():
            continue
        for p in sorted(side_dir.glob("part-*.tsv*")):
            key = f"{norm_prefix}/{side}/{p.name}" if norm_prefix else f"{side}/{p.name}"
            files.append((p, key))

    manifest = src / "manifest.json"
    if include_manifest and manifest.exists():
        key = f"{norm_prefix}/manifest.json" if norm_prefix else "manifest.json"
        files.append((manifest, key))

    if not files:
        raise ValueError(f"アップロード対象がありません: {src}")

    for path, key in files:
        print(f"upload: {path} -> s3://{bucket}/{key}")
        s3.upload_file(str(path), bucket, key)

    print(f"uploaded {len(files)} files")
    return 0


def add_upload_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    p = subparsers.add_parser("upload-s3", help="生成済みデータセットをS3にアップロード")
    p.add_argument("--bucket", required=True)
    p.add_argument("--src", type=Path, required=True)
    p.add_argument("--dst-prefix", required=True)
    p.add_argument("--include-manifest", default="true")

    def _cmd(args: argparse.Namespace) -> int:
        include_manifest = str(args.include_manifest).lower() in {"1", "true", "yes", "on"}
        return run_upload_s3(args.src, args.bucket, args.dst_prefix, include_manifest=include_manifest)

    p.set_defaults(func=_cmd)
