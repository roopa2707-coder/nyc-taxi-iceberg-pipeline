from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from pipeline import transforms as T

JAN = dict(year=2025, month=1)
BASE_PICKUP = dt.datetime(2025, 1, 10, 8, 0, 0)  # pickup time of conftest.BASE_ROW


def reasons(df):
    return [r["reject_reason"] for r in df.orderBy("reject_reason").collect()]


# ---------- month_bounds ---------------------------------------------------
def test_month_bounds_handles_december():
    assert T.month_bounds(2024, 12) == (dt.datetime(2024, 12, 1), dt.datetime(2025, 1, 1))
    assert T.month_bounds(2025, 1) == (dt.datetime(2025, 1, 1), dt.datetime(2025, 2, 1))


# ---------- standardize_trips ----------------------------------------------
def test_standardize_renames_and_casts(make_raw):
    df = T.standardize_trips(make_raw({}))
    assert df.columns == T.STANDARD_COLUMNS
    types = dict(df.dtypes)
    assert types["pickup_datetime"] == "timestamp_ntz"
    assert types["passenger_count"] == "int"
    assert types["fare_amount"] == "decimal(10,2)"


def test_standardize_adds_missing_columns_as_null(make_raw):
    raw = make_raw({}).drop("cbd_congestion_fee")  # e.g. a 2024 file
    row = T.standardize_trips(raw).first()
    assert row["cbd_congestion_fee"] is None


def test_standardize_keeps_extra_columns(make_raw):
    raw = make_raw({}).withColumn("source_file", F.lit("s3a://b/raw/x.parquet"))
    assert T.standardize_trips(raw).columns[-1] == "source_file"


# ---------- row_hash ---------------------------------------------------------
def test_row_hash_is_null_safe(spark):
    df = spark.createDataFrame([(None, "a"), ("a", None)], "x string, y string")
    hashes = [r[0] for r in df.select(T.row_hash(["x", "y"])).collect()]
    assert hashes[0] != hashes[1]


# ---------- reject rules ---------------------------------------------------
@pytest.mark.parametrize(
    "override, expected",
    [
        ({"tpep_pickup_datetime": None}, T.R_NULL_TS),
        ({"tpep_dropoff_datetime": None}, T.R_NULL_TS),
        ({"tpep_dropoff_datetime": dt.datetime(2025, 1, 10, 7, 59)}, T.R_DROPOFF_BEFORE_PICKUP),
        ({"fare_amount": 0.0}, T.R_NON_POSITIVE_FARE),
        ({"fare_amount": -5.0}, T.R_NON_POSITIVE_FARE),
        ({"fare_amount": None}, T.R_NON_POSITIVE_FARE),
        (
            {
                "tpep_pickup_datetime": dt.datetime(2024, 12, 31, 23, 50),
                "tpep_dropoff_datetime": dt.datetime(2025, 1, 1, 0, 10),
            },
            T.R_OUTSIDE_MONTH,
        ),
        (
            {
                "tpep_pickup_datetime": dt.datetime(2025, 2, 1, 0, 0),
                "tpep_dropoff_datetime": dt.datetime(2025, 2, 1, 0, 10),
            },
            T.R_OUTSIDE_MONTH,
        ),
    ],
)
def test_each_reject_rule(make_raw, zones_raw, override, expected):
    clean, rejected = T.build_clean_and_rejected(make_raw(override), zones_raw, **JAN)
    assert clean.count() == 0
    assert reasons(rejected) == [expected]


def test_valid_edge_cases_are_kept(make_raw, zones_raw):
    raw = make_raw(
        # first and last instant of the month
        {"tpep_pickup_datetime": dt.datetime(2025, 1, 1, 0, 0), "tpep_dropoff_datetime": dt.datetime(2025, 1, 1, 0, 5)},
        {
            "tpep_pickup_datetime": dt.datetime(2025, 1, 31, 23, 59, 59),
            "tpep_dropoff_datetime": dt.datetime(2025, 2, 1, 0, 20),  # dropoff may spill into next month
        },
        # zero-minute trip is not "dropoff before pickup"
        {"tpep_dropoff_datetime": BASE_PICKUP},
    )
    clean, rejected = T.build_clean_and_rejected(raw, zones_raw, **JAN)
    assert clean.count() == 3
    assert rejected.count() == 0


def test_rule_precedence_first_reason_wins(make_raw, zones_raw):
    # dropoff before pickup AND fare <= 0 -> reported as dropoff_before_pickup
    raw = make_raw({"tpep_dropoff_datetime": dt.datetime(2025, 1, 10, 7, 0), "fare_amount": 0.0})
    _, rejected = T.build_clean_and_rejected(raw, zones_raw, **JAN)
    assert reasons(rejected) == [T.R_DROPOFF_BEFORE_PICKUP]


def test_exact_duplicates_keep_one_copy(make_raw, zones_raw):
    raw = make_raw({}, {}, {}, {"tip_amount": 1.0})  # 3 identical + 1 different
    clean, rejected = T.build_clean_and_rejected(raw, zones_raw, **JAN)
    assert clean.count() == 2
    assert reasons(rejected) == [T.R_DUPLICATE, T.R_DUPLICATE]
    # every reject has its own id, even copies of the same row
    assert rejected.select("reject_id").distinct().count() == 2


def test_duplicates_of_invalid_rows_get_the_rule_reason(make_raw, zones_raw):
    raw = make_raw({"fare_amount": 0.0}, {"fare_amount": 0.0})
    _, rejected = T.build_clean_and_rejected(raw, zones_raw, **JAN)
    assert reasons(rejected) == [T.R_NON_POSITIVE_FARE, T.R_NON_POSITIVE_FARE]


def test_every_raw_row_lands_in_exactly_one_output(make_raw, zones_raw):
    raw = make_raw({}, {}, {"fare_amount": 0.0}, {"tpep_pickup_datetime": None}, {"tip_amount": 2.0})
    clean, rejected = T.build_clean_and_rejected(raw, zones_raw, **JAN)
    assert raw.count() == clean.count() + rejected.count()


# ---------- derived columns ------------------------------------------------
def test_derived_columns(make_raw, zones_raw):
    clean, _ = T.build_clean_and_rejected(make_raw({}), zones_raw, **JAN)
    r = clean.first()
    assert r["trip_minutes"] == 30.0
    assert r["avg_mph"] == 10.0  # 5 miles in 30 min
    assert r["tip_pct"] == 20.0  # 4 / 20
    assert r["pickup_date"] == dt.date(2025, 1, 10)
    assert r["pickup_hour"] == 8
    assert r["payment_type_desc"] == "Credit card"
    assert r["total_amount"] == Decimal("25.50")


def test_avg_mph_is_null_for_zero_minute_trip(make_raw, zones_raw):
    clean, _ = T.build_clean_and_rejected(make_raw({"tpep_dropoff_datetime": BASE_PICKUP}), zones_raw, **JAN)
    r = clean.first()
    assert r["trip_minutes"] == 0.0
    assert r["avg_mph"] is None


def test_unknown_payment_code(spark):
    df = spark.createDataFrame([(99,), (None,), (2,)], "c int")
    out = [r[0] for r in df.select(T.payment_type_desc(F.col("c"))).collect()]
    assert out == ["Unknown", "Unknown", "Cash"]


# ---------- zone enrichment ------------------------------------------------
def test_zone_names_joined_for_pickup_and_dropoff(make_raw, zones_raw):
    clean, _ = T.build_clean_and_rejected(make_raw({}), zones_raw, **JAN)
    r = clean.first()
    assert (r["pickup_borough"], r["pickup_zone"]) == ("Queens", "JFK Airport")
    assert (r["dropoff_borough"], r["dropoff_zone"]) == ("Manhattan", "Midtown Center")


def test_unknown_zone_keeps_the_trip(make_raw, zones_raw):
    clean, _ = T.build_clean_and_rejected(make_raw({"PULocationID": 999}), zones_raw, **JAN)
    r = clean.first()
    assert r["pickup_zone"] is None  # left join: trip kept, DQ check flags the gap


def test_trip_id_is_deterministic(make_raw, zones_raw):
    a, _ = T.build_clean_and_rejected(make_raw({}), zones_raw, **JAN)
    b, _ = T.build_clean_and_rejected(make_raw({}), zones_raw, **JAN)
    assert a.first()["trip_id"] == b.first()["trip_id"]
    assert len(a.first()["trip_id"]) == 64


def test_clean_column_order(make_raw, zones_raw):
    clean, _ = T.build_clean_and_rejected(make_raw({}), zones_raw, **JAN)
    assert clean.columns == T.CLEAN_COLUMN_ORDER


# ---------- schema-evolution column ----------------------------------------
def test_is_airport_trip(make_raw, zones_raw):
    raw = make_raw(
        {"PULocationID": 132},  # JFK pickup
        {"PULocationID": 161, "DOLocationID": 138},  # LGA dropoff
        {"PULocationID": 161, "DOLocationID": 236},  # no airport
    )
    clean, _ = T.build_clean_and_rejected(raw, zones_raw, **JAN)
    out = T.add_is_airport_trip(clean)
    got = {(r["pu_location_id"], r["do_location_id"]): r["is_airport_trip"] for r in out.collect()}
    assert got == {(132, 161): True, (161, 138): True, (161, 236): False}


# ---------- daily_zone_metrics -----------------------------------------------
def test_daily_zone_metrics(make_raw, zones_raw):
    raw = make_raw(
        {},  # JFK, 30 min, tip 20%, total 25.5
        {"tip_amount": 2.0, "total_amount": 23.5,
         "tpep_dropoff_datetime": dt.datetime(2025, 1, 10, 8, 10)},  # JFK, 10 min, tip 10%
        {"PULocationID": 161},  # different zone
        {"tpep_pickup_datetime": dt.datetime(2025, 1, 11, 8, 0),
         "tpep_dropoff_datetime": dt.datetime(2025, 1, 11, 8, 30)},  # different day
    )
    clean, _ = T.build_clean_and_rejected(raw, zones_raw, **JAN)
    m = {(r["pickup_date"], r["pickup_zone"]): r for r in T.daily_zone_metrics(clean).collect()}
    jfk = m[(dt.date(2025, 1, 10), "JFK Airport")]
    assert jfk["trips"] == 2
    assert jfk["total_revenue"] == Decimal("49.00")
    assert jfk["avg_tip_pct"] == 15.0
    assert jfk["avg_trip_minutes"] == 20.0
    assert len(m) == 3
