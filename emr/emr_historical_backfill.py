"""
Studocu Historical Backfill — EMR PySpark Job
==============================================

One-time job to process the 12.4 TB of historical OpenSearch event logs
sitting in S3 bronze into validated, partitioned Parquet in S3 silver.

Designed to run on an EMR cluster (Spark 3.x), not Glue. EMR is chosen
for this bounded one-time workload because:
  - Spot instance support (60-90% cheaper than on-demand)
  - Memory-optimized instance types (r5 family) for shuffle-heavy
    JSON-to-Parquet conversions
  - Cluster-level Spark config tuning for multi-TB workloads

Output schema is identical to the daily Glue pipeline, so downstream
consumers (Catalog, Athena, dbt, Spectrum) don't care which pipeline
produced any given partition.

Cluster sizing (recommended):
  - Instance type: r5.4xlarge (memory-optimized, good for shuffles)
  - Worker count: 20-50, with auto-scaling enabled
  - Pricing: 70% spot, 30% on-demand for resilience
  - Expected runtime: 4-8 hours for full 12.4 TB

Submission:
  spark-submit \\
    --conf spark.dynamicAllocation.enabled=true \\
    --conf spark.shuffle.service.enabled=true \\
    --conf spark.sql.shuffle.partitions=400 \\
    emr_historical_backfill.py \\
      --source_path s3://studocu-data-lake/bronze/ \\
      --silver_path s3://studocu-data-lake/silver/ \\
      --rejected_path s3://studocu-data-lake/rejected/ \\
      --start_date 2023-01-01 \\
      --end_date 2026-05-31
"""

import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    BooleanType
)


# ----------------------------------------------------------------------
# Argument parsing
# ----------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Studocu historical backfill")
    parser.add_argument("--source_path", required=True,
                        help="S3 path to bronze layer (raw JSON)")
    parser.add_argument("--silver_path", required=True,
                        help="S3 path to silver layer (output Parquet)")
    parser.add_argument("--rejected_path", required=True,
                        help="S3 path for quarantined bad rows")
    parser.add_argument("--start_date", required=True,
                        help="Start of backfill range, YYYY-MM-DD")
    parser.add_argument("--end_date", required=True,
                        help="End of backfill range, YYYY-MM-DD")
    return parser.parse_args()


# ----------------------------------------------------------------------
# Expected schema — same contract as the daily Glue job
# ----------------------------------------------------------------------
EXPECTED_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("event_timestamp", StringType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("user_id", StringType(), nullable=False),
    StructField("session_id", StringType(), nullable=True),
    StructField("document_id", StringType(), nullable=True),
    StructField("university_id", StringType(), nullable=True),
    StructField("country_code", StringType(), nullable=True),
    StructField("device_type", StringType(), nullable=True),
    StructField("page_url", StringType(), nullable=True),
    StructField("referrer", StringType(), nullable=True),
    StructField("duration_seconds", IntegerType(), nullable=True),
    StructField("is_premium_user", BooleanType(), nullable=True),
])

VALID_EVENT_TYPES = [
    "document_view", "document_download", "search",
    "signup", "login", "quiz_attempt"
]


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("studocu-historical-backfill")
        # Spark tuning for shuffle-heavy multi-TB conversions
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Snappy is the default Parquet codec but set explicitly
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    print(f"Backfill range: {args.start_date} to {args.end_date}")
    print(f"Reading from:   {args.source_path}")
    print(f"Writing to:     {args.silver_path}")
    print(f"Rejected to:    {args.rejected_path}")

    # ------------------------------------------------------------------
    # Read bronze
    # Reading by partition path is much faster than scanning the whole
    # bronze layer; we filter to the requested date range using
    # partition discovery.
    # ------------------------------------------------------------------
    raw_df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .json(args.source_path)
    )

    # Filter by event_timestamp range — we can't rely on the partition
    # path alone for the date filter because OpenSearch export
    # partitioning may differ from event date.
    raw_df = raw_df.filter(
        (F.to_date(F.col("event_timestamp")) >= F.lit(args.start_date)) &
        (F.to_date(F.col("event_timestamp")) <= F.lit(args.end_date))
    )

    bronze_count = raw_df.count()
    print(f"Bronze row count in range: {bronze_count:,}")

    # ------------------------------------------------------------------
    # Validation — same rules as the daily Glue job
    # ------------------------------------------------------------------
    validated_df = raw_df.withColumn(
        "validation_error",
        F.when(F.col("event_id").isNull(), F.lit("missing_event_id"))
         .when(F.col("user_id").isNull(), F.lit("missing_user_id"))
         .when(F.col("event_type").isNull(), F.lit("missing_event_type"))
         .when(~F.col("event_type").isin(VALID_EVENT_TYPES),
               F.lit("invalid_event_type"))
         .when(F.to_timestamp(F.col("event_timestamp")).isNull(),
               F.lit("bad_timestamp"))
         .when(F.col("duration_seconds").cast("int").isNull() &
               F.col("duration_seconds").isNotNull(),
               F.lit("duration_not_integer"))
         .otherwise(None)
    )

    good_df = validated_df.filter(F.col("validation_error").isNull()).drop("validation_error")
    bad_df = validated_df.filter(F.col("validation_error").isNotNull())

    good_count = good_df.count()
    bad_count = bad_df.count()
    print(f"Good rows: {good_count:,}")
    print(f"Bad rows:  {bad_count:,}")
    if bronze_count > 0:
        print(f"Rejection rate: {100 * bad_count / bronze_count:.2f}%")

    # ------------------------------------------------------------------
    # Transform good rows — identical to the daily Glue job
    # ------------------------------------------------------------------
    silver_df = (
        good_df
        .withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp")))
        .withColumn("duration_seconds", F.col("duration_seconds").cast(IntegerType()))
        .withColumn("event_date", F.to_date(F.col("event_timestamp")))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        # Coalesce to a sensible number of output files per partition
        # to avoid the small-files problem at 12.4 TB scale.
        .repartition("event_date", "event_type")
    )

    # ------------------------------------------------------------------
    # Write silver — partitioned, Snappy Parquet, same schema as daily
    # mode="append" because we're backfilling alongside any daily
    # data that may already exist for overlapping dates.
    # ------------------------------------------------------------------
    print(f"Writing silver to: {args.silver_path}")
    (
        silver_df.write
        .mode("append")
        .partitionBy("event_date", "event_type")
        .option("compression", "snappy")
        .parquet(args.silver_path)
    )

    # ------------------------------------------------------------------
    # Write rejected rows — JSON with the validation error attached
    # ------------------------------------------------------------------
    if bad_count > 0:
        print(f"Writing rejected to: {args.rejected_path}")
        (
            bad_df
            .withColumn("rejected_at", F.current_timestamp())
            .write
            .mode("append")
            .json(args.rejected_path)
        )

    print("Historical backfill complete.")
    spark.stop()


if __name__ == "__main__":
    main()