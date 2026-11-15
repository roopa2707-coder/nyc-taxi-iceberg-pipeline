"""Part 1: download one month of Yellow Taxi trips + the zone lookup and land
them, unchanged, in the private S3 bucket under raw/.

    python scripts/ingest_to_s3.py                       # month from .env
    python scripts/ingest_to_s3.py --year 2025 --month 1
    python scripts/ingest_to_s3.py --local-only          # no S3, for CATALOG_BACKEND=local

Safe to re-run:
  * S3 keys are deterministic (one key per source file), so a re-run can only
    overwrite, never add a second copy.
  * Each object stores the file's SHA-256 in its metadata. If the object
    already exists with the same hash, the upload is skipped.
  * Downloads go to a .part file that is renamed only when complete, so a
    half-finished download is never uploaded.
Credentials come from the default AWS chain (env vars or an AWS profile for
the least-privilege `taxi-pipeline` user), never from this file.
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.config import (  # noqa: E402
    RAW_ZONE_KEY,
    REPO_ROOT,
    TRIP_URL_TEMPLATE,
    ZONE_LOOKUP_URL,
    raw_trip_key,
)

log = logging.getLogger("ingest")
HEADERS = {"User-Agent": "taxi-iceberg-pipeline/1.0 (take-home exercise)"}


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def download(url: str, dest: Path, force: bool = False) -> Path:
    """Download url to dest atomically; reuse an existing local copy unless force."""
    if dest.exists() and not force:
        log.info("local copy exists, not downloading again: %s", dest)
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    log.info("downloading %s", url)
    with requests.get(url, headers=HEADERS, stream=True, timeout=60) as r:
        r.raise_for_status()
        with tmp.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    tmp.replace(dest)
    return dest


def validate(path: Path) -> None:
    """Cheap sanity checks so we never land an HTML error page as 'data'."""
    if path.stat().st_size == 0:
        raise ValueError(f"{path} is empty")
    if path.suffix == ".parquet":
        with path.open("rb") as f:
            head = f.read(4)
            f.seek(-4, os.SEEK_END)
            tail = f.read(4)
        if head != b"PAR1" or tail != b"PAR1":
            raise ValueError(f"{path} is not a valid Parquet file")
    elif path.suffix == ".csv":
        first = path.open(encoding="utf-8").readline()
        if "LocationID" not in first:
            raise ValueError(f"{path} does not look like the taxi zone lookup CSV")


def upload_if_changed(s3, bucket: str, key: str, path: Path) -> str:
    """Upload path to s3://bucket/key unless an identical object is already there.

    Returns "skipped" or "uploaded".
    """
    from botocore.exceptions import ClientError

    digest = sha256_of(path)
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        if head.get("Metadata", {}).get("sha256") == digest:
            log.info("unchanged, skipping s3://%s/%s", bucket, key)
            return "skipped"
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
            raise
    s3.upload_file(
        str(path),
        bucket,
        key,
        ExtraArgs={"Metadata": {"sha256": digest}, "ServerSideEncryption": "AES256"},
    )
    log.info("uploaded s3://%s/%s (%s bytes)", bucket, key, f"{path.stat().st_size:,}")
    return "uploaded"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, default=int(os.getenv("TRIP_YEAR", "2025")))
    p.add_argument("--month", type=int, default=int(os.getenv("TRIP_MONTH", "1")))
    p.add_argument("--bucket", default=os.getenv("S3_BUCKET"))
    p.add_argument("--data-dir", type=Path, default=Path(os.getenv("LOCAL_DATA_DIR", REPO_ROOT / "data")))
    p.add_argument("--local-only", action="store_true", help="download only, do not upload to S3")
    p.add_argument("--force-download", action="store_true")
    args = p.parse_args()

    files = [
        (TRIP_URL_TEMPLATE.format(year=args.year, month=args.month), raw_trip_key(args.year, args.month)),
        (ZONE_LOOKUP_URL, RAW_ZONE_KEY),
    ]
    # The local cache mirrors the S3 layout, so local mode reads the same paths.
    local = [(download(url, args.data_dir / key, args.force_download), key) for url, key in files]
    for path, _ in local:
        validate(path)

    if args.local_only:
        log.info("--local-only: files are in %s", args.data_dir)
        return
    if not args.bucket:
        sys.exit("S3_BUCKET is not set (use --bucket or .env)")

    import boto3  # imported here so --local-only works without boto3

    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
    for path, key in local:
        upload_if_changed(s3, args.bucket, key, path)


if __name__ == "__main__":
    main()
