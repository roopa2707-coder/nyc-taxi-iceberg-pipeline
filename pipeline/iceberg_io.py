"""Small helpers around Iceberg tables in Spark."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

TABLE_PROPERTIES = {
    "format-version": "2",
    "write.parquet.compression-codec": "zstd",
}


def table_exists(spark: SparkSession, table: str) -> bool:
    try:
        spark.table(table).schema  # noqa: B018 - forces resolution
        return True
    except AnalysisException:
        return False


def create_table_if_missing(spark: SparkSession, df: DataFrame, table: str, partition_col: str | None) -> bool:
    """Create an empty Iceberg table with df's schema. Returns True if created."""
    if table_exists(spark, table):
        return False
    w = df.limit(0).writeTo(table).using("iceberg")
    for k, v in TABLE_PROPERTIES.items():
        w = w.tableProperty(k, v)
    if partition_col:
        w = w.partitionedBy(F.col(partition_col))
    w.create()
    return True


def align_to_table(spark: SparkSession, df: DataFrame, table: str) -> DataFrame:
    """Select df in the target table's column order; add typed NULLs for target
    columns the source doesn't have (e.g. a column added by schema evolution)."""
    target = spark.table(table).schema
    cols = []
    for field in target.fields:
        if field.name in df.columns:
            cols.append(F.col(field.name).cast(field.dataType).alias(field.name))
        else:
            cols.append(F.lit(None).cast(field.dataType).alias(field.name))
    return df.select(*cols)


def merge_insert_new(spark: SparkSession, df: DataFrame, table: str, keys: list[str]) -> None:
    """Idempotent load with MERGE INTO: insert only rows whose key isn't there yet.

    The keys are content hashes of the raw row, so a matched key means the row
    is already loaded and identical; there is nothing to update. An insert-only
    MERGE also lets Iceberg commit a plain append instead of rewriting files.
    """
    view = "staged_" + table.split(".")[-1]
    align_to_table(spark, df, table).createOrReplaceTempView(view)
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    spark.sql(f"MERGE INTO {table} t USING {view} s ON {on} WHEN NOT MATCHED THEN INSERT *")


def current_snapshot_id(spark: SparkSession, table: str) -> int | None:
    row = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
    return row["snapshot_id"] if row else None
