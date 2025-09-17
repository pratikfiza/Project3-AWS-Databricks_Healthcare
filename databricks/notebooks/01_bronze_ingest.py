# Databricks Bronze ingest notebook (script form)
# Reads raw claims CSV/JSON files from S3 and writes to Bronze Delta path.

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, DateType

# Configurable paths (set via job parameters or hardcode for dev)
raw_path = dbutils.widgets.get("raw_path") if 'dbutils' in globals() and dbutils.widgets.get("raw_path") else "s3a://my-claims-bucket/raw/*/*.csv"
bronze_path = dbutils.widgets.get("bronze_path") if 'dbutils' in globals() and dbutils.widgets.get("bronze_path") else "s3a://my-claims-bucket/bronze/claims/"

# Define expected schema for CSV for performance
schema = StructType([
    StructField("claim_id", StringType(), True),
    StructField("patient_id", StringType(), True),
    StructField("patient_name", StringType(), True),
    StructField("provider_id", StringType(), True),
    StructField("provider_name", StringType(), True),
    StructField("claim_amount", DoubleType(), True),
    StructField("diagnosis_code", StringType(), True),
    StructField("service_date", StringType(), True),
    StructField("received_date", StringType(), True),
    StructField("claim_status", StringType(), True)
])

# Read CSVs (supports header)
df = (spark.read
      .option("header", True)
      .schema(schema)
      .csv(raw_path))

# Add ingestion metadata
df = df.withColumn("ingest_ts", F.current_timestamp())
# convert date strings to date types
df = df.withColumn("service_date", F.to_date("service_date", "yyyy-MM-dd")) \
       .withColumn("received_date", F.to_date("received_date", "yyyy-MM-dd"))

# Write to Bronze Delta (append)
(df.write
   .format("delta")
   .mode("append")
   .partitionBy("service_date")
   .save(bronze_path))

display(df.limit(5))
