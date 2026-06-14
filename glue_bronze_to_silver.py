import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType

# === GLUE BOILERPLATE ===
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_path', 'silver_path', 'rejected_path'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# === CONFIG ===
SOURCE_PATH = args['source_path']      # s3://.../bronze/
SILVER_PATH = args['silver_path']      # s3://.../silver/
REJECTED_PATH = args['rejected_path']  # s3://.../rejected/

# === EXPECTED SCHEMA ===
# Defining schema explicitly is a production best practice.
# It prevents schema drift and gives us a single source of truth for validation.
expected_schema = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("event_timestamp", StringType(), nullable=False),  # parsed later
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

VALID_EVENT_TYPES = ["document_view", "document_download", "search", "signup", "login", "quiz_attempt"]

# === READ BRONZE ===
print(f"Reading from: {SOURCE_PATH}")
raw_df = spark.read.json(SOURCE_PATH)
print(f"Bronze row count: {raw_df.count()}")

# === VALIDATION ===
# We add a column 'validation_error' that captures WHY a row is bad.
# Good rows have validation_error = NULL.
# Bad rows get routed to /rejected with the error reason for debugging.

validated_df = raw_df.withColumn(
    "validation_error",
    F.when(F.col("event_id").isNull(), F.lit("missing_event_id"))
     .when(F.col("user_id").isNull(), F.lit("missing_user_id"))
     .when(F.col("event_type").isNull(), F.lit("missing_event_type"))
     .when(~F.col("event_type").isin(VALID_EVENT_TYPES), F.lit("invalid_event_type"))
     .when(F.to_timestamp(F.col("event_timestamp")).isNull(), F.lit("bad_timestamp"))
     .when(F.col("duration_seconds").cast("int").isNull() & F.col("duration_seconds").isNotNull(), F.lit("duration_not_integer"))
     .otherwise(None)
)

# === SPLIT GOOD AND BAD ===
good_df = validated_df.filter(F.col("validation_error").isNull()).drop("validation_error")
bad_df = validated_df.filter(F.col("validation_error").isNotNull())

good_count = good_df.count()
bad_count = bad_df.count()
print(f"Good rows: {good_count}")
print(f"Bad rows: {bad_count}")

# === TRANSFORM GOOD ROWS ===
# Cast timestamp properly and extract partition columns
silver_df = good_df \
    .withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp"))) \
    .withColumn("duration_seconds", F.col("duration_seconds").cast(IntegerType())) \
    .withColumn("event_date", F.to_date(F.col("event_timestamp"))) \
    .withColumn("ingestion_timestamp", F.current_timestamp())

# === WRITE SILVER (good rows, partitioned Parquet with Snappy) ===
print(f"Writing silver to: {SILVER_PATH}")
silver_df.write \
    .mode("overwrite") \
    .partitionBy("event_date", "event_type") \
    .option("compression", "snappy") \
    .parquet(SILVER_PATH)

# === WRITE REJECTED (bad rows, with error reason) ===
if bad_count > 0:
    print(f"Writing rejected to: {REJECTED_PATH}")
    bad_df \
        .withColumn("rejected_at", F.current_timestamp()) \
        .write \
        .mode("append") \
        .json(REJECTED_PATH)

print("Glue job complete.")
job.commit()