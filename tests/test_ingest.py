"""Ingest idempotency, tested against an in-memory fake S3 (moto)."""
from __future__ import annotations

import importlib.util
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

spec = importlib.util.spec_from_file_location(
    "ingest", Path(__file__).resolve().parent.parent / "scripts" / "ingest_to_s3.py"
)
ingest = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ingest)

BUCKET = "test-bucket"
KEY = "raw/yellow_trips/year=2025/month=01/yellow_tripdata_2025-01.parquet"


@pytest.fixture
def s3(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


@pytest.fixture
def parquet_file(tmp_path):
    p = tmp_path / "yellow_tripdata_2025-01.parquet"
    p.write_bytes(b"PAR1" + b"x" * 100 + b"PAR1")
    return p


def test_first_upload_then_rerun_is_skipped(s3, parquet_file):
    assert ingest.upload_if_changed(s3, BUCKET, KEY, parquet_file) == "uploaded"
    assert ingest.upload_if_changed(s3, BUCKET, KEY, parquet_file) == "skipped"
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET)["Contents"]]
    assert keys == [KEY]  # still exactly one object


def test_changed_file_is_reuploaded_to_the_same_key(s3, parquet_file):
    ingest.upload_if_changed(s3, BUCKET, KEY, parquet_file)
    parquet_file.write_bytes(b"PAR1" + b"y" * 100 + b"PAR1")
    assert ingest.upload_if_changed(s3, BUCKET, KEY, parquet_file) == "uploaded"
    assert s3.list_objects_v2(Bucket=BUCKET)["KeyCount"] == 1
    meta = s3.head_object(Bucket=BUCKET, Key=KEY)["Metadata"]
    assert meta["sha256"] == ingest.sha256_of(parquet_file)


def test_validate_rejects_non_parquet(tmp_path):
    bad = tmp_path / "x.parquet"
    bad.write_text("<html>Access denied</html>")
    with pytest.raises(ValueError):
        ingest.validate(bad)


def test_validate_rejects_wrong_csv(tmp_path):
    bad = tmp_path / "x.csv"
    bad.write_text("a,b,c\n1,2,3\n")
    with pytest.raises(ValueError):
        ingest.validate(bad)


def test_s3_keys_are_deterministic():
    from pipeline.config import raw_trip_key

    assert raw_trip_key(2025, 1) == KEY
