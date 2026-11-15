"""Part 2 + Part 3.1: raw trips -> Iceberg trips_clean (+ trips_rejected quarantine).

Usage:
    python -m jobs.build_trips_clean                # whole month from .env
    python -m jobs.build_trips_clean --max-day 15   # only load pickups up to the 15th
                                                    # (used to demo an incremental load)
Re-running is safe: MERGE INTO only inserts trip_ids that aren't loaded yet.
"""
from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from pipeline.config import load_config
from pipeline.iceberg_io import create_table_if_missing, current_snapshot_id, merge_insert_new
from pipeline.spark_session import build_spark
from pipeline.transforms import add_is_airport_trip, build_clean_and_rejected


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int)
    p.add_argument("--month", type=int)
    p.add_argument("--max-day", type=int, help="only load pickups on or before this day of the month")
    args = p.parse_args()

    cfg = load_config()
    year, month = args.year or cfg.year, args.month or cfg.month
    spark = build_spark(cfg, "build_trips_clean")
    clean_t, rejected_t = cfg.table("trips_clean"), cfg.table("trips_rejected")

    raw = spark.read.parquet(cfg.raw_trips_path(year, month)).withColumn("source_file", F.input_file_name())
    zones = spark.read.option("header", True).csv(cfg.zone_lookup_path)

    clean, rejected = build_clean_and_rejected(raw, zones, year, month)
    clean, rejected = clean.persist(), rejected.persist()

    if args.max_day:
        clean = clean.filter(F.dayofmonth("pickup_date") <= args.max_day)

    created = create_table_if_missing(spark, clean, clean_t, partition_col="pickup_date")
    create_table_if_missing(spark, rejected, rejected_t, partition_col=None)

    # After Part 3 adds is_airport_trip, new loads populate it; old rows keep NULL.
    if "is_airport_trip" in spark.table(clean_t).columns:
        clean = add_is_airport_trip(clean)

    before = spark.table(clean_t).count()
    merge_insert_new(spark, clean, clean_t, keys=["trip_id", "pickup_date"])
    merge_insert_new(spark, rejected, rejected_t, keys=["reject_id"])
    after = spark.table(clean_t).count()

    print("=" * 60)
    print(f"source file      : {cfg.raw_trips_path(year, month)}")
    print(f"table created    : {created}")
    print(f"clean rows staged: {clean.count():,}  rejected rows staged: {rejected.count():,}")
    print(f"{clean_t} rows: {before:,} -> {after:,} (inserted {after - before:,})")
    print(f"current snapshot : {current_snapshot_id(spark, clean_t)}")
    print("rejects by reason:")
    rejected.groupBy("reject_reason").count().orderBy(F.desc("count")).show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
