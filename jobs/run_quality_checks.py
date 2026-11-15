"""Part 5: automated data quality checks.

    python -m jobs.run_quality_checks

Prints every check, writes results/dq_report.json and exits with status 1 if
any "error" severity check fails (so it can gate a scheduler or CI step).
"""
from __future__ import annotations

import json
import sys

from pyspark.sql import functions as F

from pipeline.config import REPO_ROOT, load_config, trip_file_name
from pipeline.quality import run_all_checks
from pipeline.spark_session import build_spark
from pipeline.transforms import month_bounds


def main() -> None:
    cfg = load_config()
    spark = build_spark(cfg, "run_quality_checks")
    file_name = trip_file_name(cfg.year, cfg.month)

    # Scope everything to this month's source file so other months don't skew the counts.
    def this_month(df):
        return df.filter(F.col("source_file").endswith(file_name))

    raw_count = spark.read.parquet(cfg.raw_trips_path()).count()
    clean = this_month(spark.table(cfg.table("trips_clean")))
    rejected = this_month(spark.table(cfg.table("trips_rejected")))
    start, end = month_bounds(cfg.year, cfg.month)
    metrics = spark.table(cfg.table("daily_zone_metrics")).filter(
        (F.col("pickup_date") >= start.date()) & (F.col("pickup_date") < end.date())
    )

    results = run_all_checks(clean, rejected, metrics, raw_count, cfg.year, cfg.month)

    width = max(len(r.name) for r in results)
    for r in results:
        status = "PASS" if r.passed else ("WARN" if r.severity == "warn" else "FAIL")
        print(f"[{status}] {r.name:<{width}}  {r.observed}")

    report = REPO_ROOT / "results" / "dq_report.json"
    report.parent.mkdir(exist_ok=True)
    report.write_text(json.dumps([r.to_dict() for r in results], indent=2))
    print(f"report written to {report}")
    spark.stop()

    failed = [r for r in results if not r.passed and r.severity == "error"]
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
