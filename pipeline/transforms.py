"""Pure DataFrame transformations for the taxi pipeline.

Every function takes and returns Spark DataFrames and does no I/O, so each one
can be unit tested with a tiny in-memory DataFrame (see tests/).
"""
from __future__ import annotations

import datetime as dt

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

# Raw TLC column -> (clean column name, Spark type). Order defines the clean schema.
RAW_TO_CLEAN = {
    "VendorID": ("vendor_id", "int"),
    "tpep_pickup_datetime": ("pickup_datetime", "timestamp_ntz"),
    "tpep_dropoff_datetime": ("dropoff_datetime", "timestamp_ntz"),
    "passenger_count": ("passenger_count", "int"),
    "trip_distance": ("trip_distance", "double"),
    "RatecodeID": ("ratecode_id", "int"),
    "store_and_fwd_flag": ("store_and_fwd_flag", "string"),
    "PULocationID": ("pu_location_id", "int"),
    "DOLocationID": ("do_location_id", "int"),
    "payment_type": ("payment_type", "int"),
    "fare_amount": ("fare_amount", "decimal(10,2)"),
    "extra": ("extra", "decimal(10,2)"),
    "mta_tax": ("mta_tax", "decimal(10,2)"),
    "tip_amount": ("tip_amount", "decimal(10,2)"),
    "tolls_amount": ("tolls_amount", "decimal(10,2)"),
    "improvement_surcharge": ("improvement_surcharge", "decimal(10,2)"),
    "total_amount": ("total_amount", "decimal(10,2)"),
    "congestion_surcharge": ("congestion_surcharge", "decimal(10,2)"),
    "Airport_fee": ("airport_fee", "decimal(10,2)"),
    "cbd_congestion_fee": ("cbd_congestion_fee", "decimal(10,2)"),  # new in 2025 files
}
STANDARD_COLUMNS = [name for name, _ in RAW_TO_CLEAN.values()]

# TLC data dictionary, payment_type codes.
PAYMENT_TYPES = {
    0: "Flex Fare",
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip",
}

# Taxi zone IDs for Newark (EWR), JFK and LaGuardia.
AIRPORT_ZONE_IDS = (1, 132, 138)

# Reject reasons, checked in this order; a row gets the first reason that applies.
R_NULL_TS = "null_pickup_or_dropoff"
R_DROPOFF_BEFORE_PICKUP = "dropoff_before_pickup"
R_NON_POSITIVE_FARE = "non_positive_fare"
R_OUTSIDE_MONTH = "pickup_outside_month"
R_DUPLICATE = "duplicate_row"
REJECT_REASONS = (R_NULL_TS, R_DROPOFF_BEFORE_PICKUP, R_NON_POSITIVE_FARE, R_OUTSIDE_MONTH, R_DUPLICATE)


def month_bounds(year: int, month: int) -> tuple[dt.datetime, dt.datetime]:
    """[start, end) of a calendar month."""
    start = dt.datetime(year, month, 1)
    end = dt.datetime(year + (month == 12), month % 12 + 1, 1)
    return start, end


def _ntz(value: dt.datetime) -> Column:
    """Timestamp-without-timezone literal. Built from a string on purpose:
    F.lit(datetime) is converted using the machine's local timezone, which
    would shift the month boundaries on a laptop in New York."""
    return F.lit(value.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp_ntz")


def standardize_trips(raw: DataFrame) -> DataFrame:
    """Rename raw TLC columns to snake_case and cast to a fixed schema.

    Columns missing from a given month's file (e.g. cbd_congestion_fee before
    2025) are added as typed NULLs so every month has the same schema. Extra
    columns (such as the job's source_file) are kept at the end.
    """
    raw_lower = {c.lower(): c for c in raw.columns}
    cols = []
    for raw_name, (clean_name, dtype) in RAW_TO_CLEAN.items():
        actual = raw_lower.get(raw_name.lower())  # TLC changed "airport_fee" casing over time
        src = F.col(f"`{actual}`") if actual else F.lit(None)
        cols.append(src.cast(dtype).alias(clean_name))
    known = {k.lower() for k in RAW_TO_CLEAN}
    extras = [F.col(f"`{c}`") for c in raw.columns if c.lower() not in known]
    return raw.select(*cols, *extras)


def row_hash(columns: list[str]) -> Column:
    """Deterministic SHA-256 over the given columns.

    NULLs are replaced by a sentinel so that (NULL, 'a') and ('a', NULL) hash
    differently. Used as trip_id: identical raw rows -> identical id.
    """
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in columns]
    return F.sha2(F.concat_ws("|", *parts), 256)


def flag_rejects(trips: DataFrame, year: int, month: int) -> DataFrame:
    """Add row_hash, dup_rank and reject_reason (NULL = valid row).

    Expects the output of standardize_trips().
    """
    start, end = month_bounds(year, month)
    pickup, dropoff = F.col("pickup_datetime"), F.col("dropoff_datetime")

    hashed = trips.withColumn("row_hash", row_hash(STANDARD_COLUMNS))
    # Rows with the same hash are exact duplicates; rank them 1, 2, 3, ...
    # Ordering inside a group doesn't matter because the rows are identical.
    w = Window.partitionBy("row_hash").orderBy("row_hash")
    hashed = hashed.withColumn("dup_rank", F.row_number().over(w))

    reason = (
        F.when(pickup.isNull() | dropoff.isNull(), F.lit(R_NULL_TS))
        .when(dropoff < pickup, F.lit(R_DROPOFF_BEFORE_PICKUP))
        .when(F.col("fare_amount").isNull() | (F.col("fare_amount") <= 0), F.lit(R_NON_POSITIVE_FARE))
        .when(
            (pickup < _ntz(start)) | (pickup >= _ntz(end)),
            F.lit(R_OUTSIDE_MONTH),
        )
        .when(F.col("dup_rank") > 1, F.lit(R_DUPLICATE))
    )
    return hashed.withColumn("reject_reason", reason)


def add_derived_columns(trips: DataFrame) -> DataFrame:
    """trip_minutes, avg_mph, tip_pct plus handy date/hour/payment columns."""
    seconds = F.expr("timestampdiff(SECOND, pickup_datetime, dropoff_datetime)")
    minutes = F.round(seconds / 60.0, 2)
    return (
        trips.withColumn("trip_minutes", minutes)
        .withColumn(
            "avg_mph",
            F.when(
                F.col("trip_minutes") > 0,
                F.round(F.col("trip_distance") / (F.col("trip_minutes") / 60.0), 2),
            ),
        )
        # tip as a percentage of the fare (fare_amount > 0 is guaranteed for clean rows)
        .withColumn(
            "tip_pct",
            F.round(F.col("tip_amount").cast("double") / F.col("fare_amount").cast("double") * 100.0, 2),
        )
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
        .withColumn("payment_type_desc", payment_type_desc(F.col("payment_type")))
    )


def payment_type_desc(code: Column) -> Column:
    expr = F.when(code.isNull(), F.lit("Unknown"))
    for k, v in PAYMENT_TYPES.items():
        expr = expr.when(code == k, F.lit(v))
    return expr.otherwise(F.lit("Unknown"))


def standardize_zones(zones_raw: DataFrame) -> DataFrame:
    """Zone lookup CSV -> (location_id int, borough, zone, service_zone)."""
    return zones_raw.select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
        F.col("service_zone").alias("service_zone"),
    )


def enrich_with_zones(trips: DataFrame, zones: DataFrame) -> DataFrame:
    """Left-join pickup and dropoff zone names (zones is tiny -> broadcast)."""
    pu = F.broadcast(
        zones.select(
            F.col("location_id").alias("pu_location_id"),
            F.col("borough").alias("pickup_borough"),
            F.col("zone").alias("pickup_zone"),
        )
    )
    do = F.broadcast(
        zones.select(
            F.col("location_id").alias("do_location_id"),
            F.col("borough").alias("dropoff_borough"),
            F.col("zone").alias("dropoff_zone"),
        )
    )
    return trips.join(pu, "pu_location_id", "left").join(do, "do_location_id", "left")


def add_is_airport_trip(trips: DataFrame) -> DataFrame:
    """Column added later through Iceberg schema evolution (Part 3)."""
    return trips.withColumn(
        "is_airport_trip",
        F.col("pu_location_id").isin(*AIRPORT_ZONE_IDS) | F.col("do_location_id").isin(*AIRPORT_ZONE_IDS),
    )


CLEAN_COLUMN_ORDER = (
    ["trip_id", "pickup_date", "pickup_hour"]
    + STANDARD_COLUMNS
    + [
        "payment_type_desc",
        "pickup_borough",
        "pickup_zone",
        "dropoff_borough",
        "dropoff_zone",
        "trip_minutes",
        "avg_mph",
        "tip_pct",
        "source_file",
    ]
)


def build_clean_and_rejected(
    raw: DataFrame, zones_raw: DataFrame, year: int, month: int
) -> tuple[DataFrame, DataFrame]:
    """End-to-end transform: raw trips + zone CSV -> (trips_clean, trips_rejected).

    Every raw row lands in exactly one of the two outputs, so
    count(raw) == count(clean) + count(rejected).
    """
    std = standardize_trips(raw)
    if "source_file" not in std.columns:
        std = std.withColumn("source_file", F.lit(None).cast("string"))
    flagged = flag_rejects(std, year, month)

    valid = flagged.filter(F.col("reject_reason").isNull())
    clean = enrich_with_zones(add_derived_columns(valid), standardize_zones(zones_raw))
    clean = clean.withColumn("trip_id", F.col("row_hash")).select(*CLEAN_COLUMN_ORDER)

    rejected = (
        flagged.filter(F.col("reject_reason").isNotNull())
        # dup_rank makes the id unique even for duplicate copies of the same row
        .withColumn("reject_id", F.concat_ws("-", F.col("row_hash"), F.col("dup_rank").cast("string")))
        .select("reject_id", "reject_reason", *STANDARD_COLUMNS, "source_file")
    )
    return clean, rejected


def daily_zone_metrics(clean: DataFrame) -> DataFrame:
    """Daily results by pickup zone."""
    return (
        clean.groupBy("pickup_date", "pu_location_id", "pickup_borough", "pickup_zone")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.sum("total_amount").cast("decimal(18,2)").alias("total_revenue"),
            F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"),
            F.round(F.avg("trip_minutes"), 2).alias("avg_trip_minutes"),
        )
    )
