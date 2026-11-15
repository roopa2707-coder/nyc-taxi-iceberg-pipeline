"""Central configuration, read from environment variables (or a local .env file).

Nothing secret is hard-coded here: credentials come from the environment
(AWS default credential chain, POLARIS_CLIENT_SECRET, etc.).
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:  # python-dotenv is optional; it just makes local runs easier.
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover
    pass

REPO_ROOT = Path(__file__).resolve().parent.parent

TRIP_URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def trip_file_name(year: int, month: int) -> str:
    return f"yellow_tripdata_{year}-{month:02d}.parquet"


def raw_trip_key(year: int, month: int) -> str:
    """S3 key (and local relative path) of the raw monthly trip file."""
    return f"raw/yellow_trips/year={year}/month={month:02d}/{trip_file_name(year, month)}"


RAW_ZONE_KEY = "raw/taxi_zone_lookup/taxi_zone_lookup.csv"


@dataclass(frozen=True)
class Config:
    catalog_backend: str  # polaris | glue | local
    aws_region: str
    s3_bucket: str
    namespace: str
    year: int
    month: int
    local_data_dir: Path
    polaris_uri: str
    polaris_catalog: str
    polaris_client_id: str
    polaris_client_secret: str

    # Spark catalog name used in every SQL statement: lake.<namespace>.<table>
    spark_catalog: str = "lake"

    @property
    def is_local(self) -> bool:
        return self.catalog_backend == "local"

    @property
    def raw_base(self) -> str:
        if self.is_local:
            return str(self.local_data_dir)
        return f"s3a://{self.s3_bucket}"

    def raw_trips_path(self, year: int | None = None, month: int | None = None) -> str:
        return f"{self.raw_base}/{raw_trip_key(year or self.year, month or self.month)}"

    @property
    def zone_lookup_path(self) -> str:
        return f"{self.raw_base}/{RAW_ZONE_KEY}"

    @property
    def warehouse(self) -> str:
        if self.is_local:
            return str(REPO_ROOT / "warehouse")
        return f"s3://{self.s3_bucket}/warehouse"

    def table(self, name: str) -> str:
        return f"{self.spark_catalog}.{self.namespace}.{name}"


def load_config() -> Config:
    backend = os.getenv("CATALOG_BACKEND", "polaris").lower()
    if backend not in {"polaris", "glue", "local"}:
        raise ValueError(f"CATALOG_BACKEND must be polaris, glue or local, got {backend!r}")
    bucket = os.getenv("S3_BUCKET", "")
    if backend != "local" and not bucket:
        raise ValueError("S3_BUCKET must be set unless CATALOG_BACKEND=local")
    return Config(
        catalog_backend=backend,
        aws_region=os.getenv("AWS_REGION", "us-east-1"),
        s3_bucket=bucket,
        namespace=os.getenv("ICEBERG_NAMESPACE", "taxi"),
        year=int(os.getenv("TRIP_YEAR", "2025")),
        month=int(os.getenv("TRIP_MONTH", "1")),
        local_data_dir=Path(os.getenv("LOCAL_DATA_DIR", str(REPO_ROOT / "data"))),
        polaris_uri=os.getenv("POLARIS_URI", "http://localhost:8181/api/catalog"),
        polaris_catalog=os.getenv("POLARIS_CATALOG", "taxi_catalog"),
        polaris_client_id=os.getenv("POLARIS_CLIENT_ID", "root"),
        polaris_client_secret=os.getenv("POLARIS_CLIENT_SECRET", ""),
    )
