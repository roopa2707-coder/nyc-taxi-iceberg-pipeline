from __future__ import annotations

import datetime as dt

from pyspark.sql import functions as F

from pipeline import quality as Q
from pipeline import transforms as T


def _build(make_raw, zones_raw, *rows):
    raw = make_raw(*rows)
    clean, rejected = T.build_clean_and_rejected(raw, zones_raw, 2025, 1)
    return raw, clean, rejected, T.daily_zone_metrics(clean)


def test_all_checks_pass_on_good_pipeline_output(make_raw, zones_raw):
    raw, clean, rejected, metrics = _build(make_raw, zones_raw, {}, {}, {"fare_amount": 0.0}, {"tip_amount": 1.0})
    results = Q.run_all_checks(clean, rejected, metrics, raw.count(), 2025, 1)
    failed = [r.name for r in results if not r.passed]
    assert failed == []
    assert len(results) >= 5


def test_reconciliation_detects_lost_rows():
    r = Q.check_reconciliation(raw_count=10, clean_count=7, rejected_count=2)
    assert not r.passed
    assert "diff=1" in r.observed
    assert Q.check_reconciliation(10, 8, 2).passed


def test_unique_check_fails_on_duplicate_ids(spark):
    df = spark.createDataFrame([("a",), ("a",), ("b",)], "trip_id string")
    assert not Q.check_unique(df, "trip_id", "u").passed


def test_not_null_check(spark):
    df = spark.createDataFrame([("a", None), ("b", "x")], "k string, v string")
    assert not Q.check_not_null(df, ["k", "v"], "nn").passed
    assert Q.check_not_null(df, ["k"], "nn").passed


def test_expression_check_and_severity(spark):
    df = spark.createDataFrame([(5.0,), (-1.0,)], "fare_amount double")
    r = Q.check_expression(df, "fare_amount <= 0", "fare", severity="warn")
    assert not r.passed and r.severity == "warn"


def test_pickup_in_month_check(make_raw, zones_raw):
    _, clean, _, _ = _build(make_raw, zones_raw, {})
    assert Q.check_pickup_in_month(clean, 2025, 1).passed
    shifted = clean.withColumn("pickup_date", F.lit(dt.date(2025, 2, 1)))
    assert not Q.check_pickup_in_month(shifted, 2025, 1).passed


def test_metrics_reconcile_detects_mismatch(make_raw, zones_raw):
    _, clean, _, metrics = _build(make_raw, zones_raw, {}, {"PULocationID": 161})
    assert Q.check_metrics_reconcile(clean, metrics).passed
    assert not Q.check_metrics_reconcile(clean, metrics.limit(1)).passed


def test_reject_reason_check(spark):
    bad = spark.createDataFrame([("x", "not_a_reason")], "reject_id string, reject_reason string")
    assert not Q.check_reject_reasons(bad).passed
