"""Builds a SparkSession wired to an Iceberg catalog.

One Spark catalog called ``lake`` is configured, backed by one of:
  * polaris - Apache Polaris REST catalog (preferred, runs in Docker)
  * glue    - AWS Glue Data Catalog (fallback)
  * local   - Iceberg Hadoop catalog on the local disk (offline dry runs, no AWS)
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

from pipeline.config import Config

SPARK_SCALA = "3.5_2.12"
ICEBERG_VERSION = "1.7.1"
HADOOP_AWS_VERSION = "3.3.4"  # must match the Hadoop version bundled with PySpark 3.5
AWS_SDK_V1_VERSION = "1.12.262"


def _packages(cfg: Config) -> str:
    pkgs = [f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_SCALA}:{ICEBERG_VERSION}"]
    if not cfg.is_local:
        pkgs += [
            # S3FileIO + Glue client used by Iceberg (AWS SDK v2)
            f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
            # s3a:// filesystem used by Spark to read the raw Parquet/CSV files
            f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}",
            f"com.amazonaws:aws-java-sdk-bundle:{AWS_SDK_V1_VERSION}",
        ]
    return ",".join(pkgs)


def build_spark(cfg: Config, app_name: str) -> SparkSession:
    cat = f"spark.sql.catalog.{cfg.spark_catalog}"
    b = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", _packages(cfg))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.driver.memory", "4g")
        # Use only the C1 JIT compiler: the C2 compiler in Homebrew OpenJDK 17 on
        # Apple Silicon crashed with "refcount underflow" during the DQ job.
        .config("spark.driver.extraJavaOptions", "-XX:TieredStopAtLevel=1")
        # zstd-jni's recycling buffer pool corrupted JVM memory when many tasks read
        # zstd Parquet files in parallel on Apple Silicon; disable it and cap threads.
        .config("spark.hadoop.parquet.compression.codec.zstd.bufferPool.enabled", "false")
        # Iceberg's vectorized Arrow reader crashed the JVM (shaded Netty/Arrow native
        # memory) on Apple Silicon + Java 17; use the row-based Parquet reader instead.
        .config("spark.sql.iceberg.vectorization.enabled", "false")
        .master(os.getenv("SPARK_MASTER", "local[4]"))
        # Constraint propagation explodes on the long CASE/WHEN chains in transforms.py
        # (known Spark optimizer issue, SPARK-19875); turning it off is safe.
        .config("spark.sql.constraintPropagation.enabled", "false")
        .config(cat, "org.apache.iceberg.spark.SparkCatalog")
    )

    if cfg.catalog_backend == "polaris":
        b = (
            b.config(f"{cat}.type", "rest")
            .config(f"{cat}.uri", cfg.polaris_uri)
            .config(f"{cat}.warehouse", cfg.polaris_catalog)  # Polaris catalog name
            .config(f"{cat}.credential", f"{cfg.polaris_client_id}:{cfg.polaris_client_secret}")
            .config(f"{cat}.scope", "PRINCIPAL_ROLE:ALL")
            .config(f"{cat}.token-refresh-enabled", "true")
            .config(f"{cat}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config(f"{cat}.client.region", cfg.aws_region)
        )
    elif cfg.catalog_backend == "glue":
        b = (
            b.config(f"{cat}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .config(f"{cat}.warehouse", cfg.warehouse)
            .config(f"{cat}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config(f"{cat}.client.region", cfg.aws_region)
        )
    else:  # local
        b = b.config(f"{cat}.type", "hadoop").config(f"{cat}.warehouse", cfg.warehouse)

    if not cfg.is_local:
        # Credentials come from the default AWS chain (env vars / ~/.aws profile), never from code.
        b = (
            b.config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            )
            .config("spark.hadoop.fs.s3a.endpoint.region", cfg.aws_region)
        )

    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cfg.spark_catalog}.{cfg.namespace}")
    return spark
