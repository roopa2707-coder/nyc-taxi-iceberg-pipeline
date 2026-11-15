"""Data quality checks. Each check returns a CheckResult; none of them raise.

The runner (jobs/run_quality_checks.py) prints all results, writes a JSON
report and exits non-zero if any check with severity "error" fails.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipeline.transforms import REJECT_REASONS, month_bounds


@dataclass
class CheckResult:
    name: str
    passed: bool
    observed: str
    expected: str
    severity: str = "error"  # "error" fails the run, "warn" is reported only

    def to_dict(self) -> dict:
        return asdict(self)


def check_not_null(df: DataFrame, columns: list[str], name: str) -> CheckResult:
    cond = None
    for c in columns:
        cond = F.col(c).isNull() if cond is None else cond | F.col(c).isNull()
    bad = df.filter(cond).count()
    return CheckResult(name, bad == 0, f"{bad} rows with NULL in {columns}", "0")


def check_unique(df: DataFrame, key: str, name: str) -> CheckResult:
    dupes = df.groupBy(key).count().filter("count > 1").count()
    return CheckResult(name, dupes == 0, f"{dupes} duplicated {key} values", "0")


def check_expression(df: DataFrame, bad_condition: str, name: str, severity: str = "error") -> CheckResult:
    """Fails if any row matches the SQL condition describing a *bad* row."""
    bad = df.filter(bad_condition).count()
    return CheckResult(name, bad == 0, f"{bad} rows where {bad_condition}", "0", severity)


def check_pickup_in_month(df: DataFrame, year: int, month: int) -> CheckResult:
    start, end = month_bounds(year, month)
    row = df.agg(F.min("pickup_date").alias("lo"), F.max("pickup_date").alias("hi")).first()
    lo, hi = row["lo"], row["hi"]
    ok = lo is None or (lo >= start.date() and hi < end.date())
    return CheckResult(
        "clean_pickup_date_within_month", ok, f"pickup_date range {lo}..{hi}", f"{start.date()}..<{end.date()}"
    )


def check_reject_reasons(rejected: DataFrame) -> CheckResult:
    bad = rejected.filter(F.col("reject_reason").isNull() | ~F.col("reject_reason").isin(*REJECT_REASONS)).count()
    return CheckResult("rejected_reason_valid", bad == 0, f"{bad} rows with missing/unknown reason", "0")


def check_reconciliation(raw_count: int, clean_count: int, rejected_count: int) -> CheckResult:
    return CheckResult(
        "reconcile_raw_eq_clean_plus_rejected",
        raw_count == clean_count + rejected_count,
        f"raw={raw_count}, clean={clean_count}, rejected={rejected_count}, "
        f"diff={raw_count - clean_count - rejected_count}",
        "raw == clean + rejected",
    )


def check_metrics_reconcile(clean: DataFrame, metrics: DataFrame) -> CheckResult:
    c = clean.agg(F.count(F.lit(1)).alias("n"), F.sum("total_amount").alias("rev")).first()
    m = metrics.agg(F.sum("trips").alias("n"), F.sum("total_revenue").alias("rev")).first()
    c_rev, m_rev = Decimal(c["rev"] or 0), Decimal(m["rev"] or 0)
    ok = c["n"] == (m["n"] or 0) and abs(c_rev - m_rev) <= Decimal("0.01")
    return CheckResult(
        "reconcile_metrics_vs_clean",
        ok,
        f"clean trips={c['n']} revenue={c_rev}; metrics trips={m['n']} revenue={m_rev}",
        "equal trips and revenue",
    )


def run_all_checks(
    clean: DataFrame,
    rejected: DataFrame,
    metrics: DataFrame,
    raw_count: int,
    year: int,
    month: int,
) -> list[CheckResult]:
    clean_count = clean.count()
    rejected_count = rejected.count()
    return [
        check_not_null(clean, ["trip_id"], "clean_trip_id_not_null"),
        check_unique(clean, "trip_id", "clean_trip_id_unique"),
        check_not_null(clean, ["pickup_datetime", "dropoff_datetime"], "clean_timestamps_not_null"),
        check_expression(clean, "dropoff_datetime < pickup_datetime", "clean_dropoff_not_before_pickup"),
        check_expression(clean, "fare_amount <= 0", "clean_fare_amount_positive"),
        check_pickup_in_month(clean, year, month),
        check_not_null(clean, ["pickup_zone", "dropoff_zone"], "clean_zone_lookup_coverage"),
        check_expression(clean, "tip_pct < 0 OR tip_pct > 500", "clean_tip_pct_in_range", severity="warn"),
        check_expression(clean, "avg_mph > 100", "clean_avg_mph_plausible", severity="warn"),
        check_reject_reasons(rejected),
        check_reconciliation(raw_count, clean_count, rejected_count),
        check_metrics_reconcile(clean, metrics),
    ]
