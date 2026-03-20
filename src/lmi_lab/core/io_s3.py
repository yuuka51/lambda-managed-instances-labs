from __future__ import annotations

from pathlib import Path


def download_file(bucket: str, key: str, dest: Path) -> None:
    import boto3

    dest.parent.mkdir(parents=True, exist_ok=True)
    boto3.client("s3").download_file(bucket, key, str(dest))


def upload_file(bucket: str, key: str, src: Path) -> None:
    import boto3

    boto3.client("s3").upload_file(str(src), bucket, key)
