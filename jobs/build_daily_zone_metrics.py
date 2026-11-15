"""Part 3.4: trips_clean -> daily_zone_metrics.

    python -m jobs.build_daily_zone_metrics

Idempotent: metrics are recomputed from trips_clean and written with
overwritePartitions(), which replaces only the pickup_date partitions present
in the new result. Re-running gives the same table, never duplicates.
"""
from __future__ import annotations

from pipeline.config import load_config
from pipeline.iceberg_io import create_table_if_missing
from pipeline.spark_session import build_spark
from pipeline.transforms import daily_zone_metrics


def main() -> None:
    cfg = load_config()
    spark = build_spark(cfg, "build_daily_zone_metrics")
    target = cfg.table("daily_zone_metrics")

    metrics = daily_zone_metrics(spark.table(cfg.table("trips_clean")))
    create_table_if_missing(spark, metrics, target, partition_col="pickup_date")
    metrics.writeTo(target).overwritePartitions()

    print(f"{target}: {spark.table(target).count():,} rows")
    spark.sql(
        f"SELECT pickup_date, count(*) AS zones, sum(trips) AS trips, sum(total_revenue) AS revenue "
        f"FROM {target} GROUP BY 1 ORDER BY 1"
    ).show(40)
    spark.stop()


if __name__ == "__main__":
    main()
