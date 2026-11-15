"""Part 3.2 + 3.3: schema evolution and time travel on trips_clean.

Run after at least two loads (see README: partial load, then full load) so
there is an earlier snapshot to travel back to.

    python -m jobs.iceberg_features

Writes the evidence to results/iceberg_features_report.md.
"""
from __future__ import annotations

from pipeline.config import REPO_ROOT, load_config
from pipeline.spark_session import build_spark


def main() -> None:
    cfg = load_config()
    spark = build_spark(cfg, "iceberg_features")
    t = cfg.table("trips_clean")
    out: list[str] = ["# Iceberg features evidence", ""]

    def section(title: str, df_or_text) -> None:
        out.extend([f"## {title}", ""])
        if isinstance(df_or_text, str):
            out.extend([df_or_text, ""])
        else:
            pdf = df_or_text.toPandas()
            out.extend([pdf.to_markdown(index=False), ""])
            print(f"\n--- {title}\n{pdf.to_string(index=False)}")

    # ---- Snapshot history ---------------------------------------------------
    snapshots = spark.sql(
        f"""SELECT snapshot_id, committed_at, operation,
                   summary['added-records'] AS added_records,
                   summary['total-records'] AS total_records
            FROM {t}.snapshots ORDER BY committed_at"""
    )
    section("Snapshot history", snapshots)
    snap_rows = snapshots.collect()
    # Skip the empty snapshot written when the table was created: compare the first
    # snapshot that actually holds data (the half-month load) with the current one.
    loaded = [r for r in snap_rows if r["total_records"] not in (None, "0")]
    first_snap = (loaded or snap_rows)[0]["snapshot_id"]
    current_snap = snap_rows[-1]["snapshot_id"]

    # ---- Schema evolution ---------------------------------------------------
    files_before = spark.sql(f"SELECT count(*) AS n FROM {t}.files").first()["n"]
    if "is_airport_trip" not in spark.table(t).columns:
        spark.sql(
            f"ALTER TABLE {t} ADD COLUMNS (is_airport_trip boolean "
            f"COMMENT 'pickup or dropoff at EWR (1), JFK (132) or LGA (138)')"
        )
        action = "Added column `is_airport_trip boolean`."
    else:
        action = "Column `is_airport_trip` already existed (job was re-run)."
    files_after = spark.sql(f"SELECT count(*) AS n FROM {t}.files").first()["n"]
    snap_after_alter = spark.sql(
        f"SELECT snapshot_id FROM {t}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).first()["snapshot_id"]
    section(
        "Schema evolution",
        f"{action}\n\n"
        f"- data files before ALTER: {files_before}, after ALTER: {files_after}\n"
        f"- current snapshot before ALTER: {current_snap}, after ALTER: {snap_after_alter}\n\n"
        "Same files and same snapshot: the change is metadata-only, no data was rewritten. "
        "Existing rows read the new column as NULL; later loads populate it.",
    )
    section(
        "is_airport_trip values in the current table",
        spark.sql(f"SELECT is_airport_trip, count(*) AS trips FROM {t} GROUP BY 1 ORDER BY 1"),
    )

    # ---- Time travel ------------------------------------------------------
    counts = spark.sql(
        f"""SELECT 'first data snapshot' AS version, {first_snap} AS snapshot_id, count(*) AS row_count,
                   min(pickup_date) AS min_pickup_date, max(pickup_date) AS max_pickup_date
            FROM {t} VERSION AS OF {first_snap}
            UNION ALL
            SELECT 'current' AS version, {current_snap} AS snapshot_id, count(*) AS row_count,
                   min(pickup_date), max(pickup_date)
            FROM {t}"""
    )
    section("Time travel: row counts before and after the change", counts)
    section(
        "Columns visible at the first snapshot (time travel uses that snapshot's schema)",
        ", ".join(spark.sql(f"SELECT * FROM {t} VERSION AS OF {first_snap}").columns),
    )

    path = REPO_ROOT / "results" / "iceberg_features_report.md"
    path.parent.mkdir(exist_ok=True)
    path.write_text("\n".join(out))
    print(f"\nreport written to {path}")
    spark.stop()


if __name__ == "__main__":
    main()
