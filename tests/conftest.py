from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql import SparkSession

# One raw TLC-shaped row; tests copy it and change fields.
BASE_ROW = {
    "VendorID": 1,
    "tpep_pickup_datetime": dt.datetime(2025, 1, 10, 8, 0, 0),
    "tpep_dropoff_datetime": dt.datetime(2025, 1, 10, 8, 30, 0),
    "passenger_count": 1.0,
    "trip_distance": 5.0,
    "RatecodeID": 1.0,
    "store_and_fwd_flag": "N",
    "PULocationID": 132,  # JFK Airport
    "DOLocationID": 161,  # Midtown Center
    "payment_type": 1,
    "fare_amount": 20.0,
    "extra": 0.0,
    "mta_tax": 0.5,
    "tip_amount": 4.0,
    "tolls_amount": 0.0,
    "improvement_surcharge": 1.0,
    "total_amount": 25.5,
    "congestion_surcharge": 0.0,
    "Airport_fee": 0.0,
    "cbd_congestion_fee": 0.0,
}

RAW_DDL = (
    "VendorID int, tpep_pickup_datetime timestamp_ntz, tpep_dropoff_datetime timestamp_ntz, "
    "passenger_count double, trip_distance double, RatecodeID double, store_and_fwd_flag string, "
    "PULocationID int, DOLocationID int, payment_type bigint, fare_amount double, extra double, "
    "mta_tax double, tip_amount double, tolls_amount double, improvement_surcharge double, "
    "total_amount double, congestion_surcharge double, Airport_fee double, cbd_congestion_fee double"
)

ZONES = [
    ("1", "EWR", "Newark Airport", "EWR"),
    ("132", "Queens", "JFK Airport", "Airports"),
    ("138", "Queens", "LaGuardia Airport", "Airports"),
    ("161", "Manhattan", "Midtown Center", "Yellow Zone"),
    ("236", "Manhattan", "Upper East Side North", "Yellow Zone"),
]


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        # same as pipeline/spark_session.py: avoids optimizer blow-up on long CASE chains
        .config("spark.sql.constraintPropagation.enabled", "false")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture
def make_raw(spark):
    """make_raw(dict(...overrides), ...) -> raw TLC-shaped DataFrame."""

    def _make(*overrides: dict):
        rows = [{**BASE_ROW, **o} for o in overrides]
        cols = list(BASE_ROW)
        return spark.createDataFrame([tuple(r[c] for c in cols) for r in rows], RAW_DDL)

    return _make


@pytest.fixture
def zones_raw(spark):
    return spark.createDataFrame(ZONES, "LocationID string, Borough string, Zone string, service_zone string")
