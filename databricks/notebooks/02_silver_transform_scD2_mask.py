# Databricks Silver transform notebook
# - Reads Bronze delta
# - Implements SCD2 for patient_dim and provider_dim
# - Masks PII (patient_name) using HIPAA-style hashing/pseudonymization
# - Writes Silver fact and dims as Delta (SCD2-enabled)

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Paths (job params or defaults)
bronze_path = dbutils.widgets.get("bronze_path") if 'dbutils' in globals() else "s3a://my-claims-bucket/bronze/claims/"
silver_fact_path = dbutils.widgets.get("silver_fact_path") if 'dbutils' in globals() else "s3a://my-claims-bucket/silver/claims_fact/"
patient_dim_path = dbutils.widgets.get("patient_dim_path") if 'dbutils' in globals() else "s3a://my-claims-bucket/silver/dim_patient/"
provider_dim_path = dbutils.widgets.get("provider_dim_path") if 'dbutils' in globals() else "s3a://my-claims-bucket/silver/dim_provider/"

# Read bronze
bronze_df = spark.read.format("delta").load(bronze_path)

# ---------- PII Masking function ----------
# Use SHA256 hash for pseudonymization (HIPAA-style pseudonymization)
def mask_pii(col_expr):
    return F.sha2(F.coalesce(col_expr, F.lit("")), 256)

# Build patient dimension SCD2
# Extract distinct patient attributes from bronze
patients_new = (bronze_df.select("patient_id", "patient_name")
                .where(F.col("patient_id").isNotNull())
                .dropDuplicates(["patient_id"]))

# Mask patient_name -> pseudonym
patients_new = patients_new.withColumn("patient_name_masked", mask_pii(F.col("patient_name"))).drop("patient_name")

# Add SCD2 fields
patients_new = patients_new.withColumn("effective_from", F.current_timestamp()) \
                           .withColumn("effective_to", F.lit(None).cast("timestamp")) \
                           .withColumn("is_current", F.lit(True))

# Merge into existing patient_dim using Delta SCD2 pattern
if DeltaTable.isDeltaTable(spark, patient_dim_path):
    patient_delta = DeltaTable.forPath(spark, patient_dim_path)
    # Close existing records for changed keys
    # Upsert new records: if patient_id exists and masked value changed -> set is_current false and insert new
    # Approach: detect changes by joining
    existing = patient_delta.toDF().select("patient_id", "patient_name_masked", "is_current")
    joined = patients_new.alias("new").join(existing.alias("old"), "patient_id", "left")
    changed = joined.filter((F.coalesce(F.col("old.patient_name_masked"), F.lit("")) != F.coalesce(F.col("new.patient_name_masked"), F.lit(""))) | F.col("old.patient_id").isNull())
    changed_ids = [r.patient_id for r in changed.select("patient_id").distinct().collect()]
    # For changed ids, set is_current=False in existing
    for pid in changed_ids:
        patient_delta.update(
            condition = F.expr(f"patient_id = '{pid}' AND is_current = true"),
            set = {"is_current": F.lit(False), "effective_to": F.current_timestamp()}
        )
    # Append new/changed rows
    patients_to_append = patients_new.filter(F.col("patient_id").isin(changed_ids))
    (patients_to_append.write.format("delta").mode("append").save(patient_dim_path))
else:
    # Create table
    (patients_new.write.format("delta").mode("overwrite").save(patient_dim_path))

# ---------- Provider dimension (SCD2) similar approach ----------
providers_new = (bronze_df.select("provider_id", "provider_name")
                 .where(F.col("provider_id").isNotNull())
                 .dropDuplicates(["provider_id"]))
providers_new = providers_new.withColumn("provider_name_masked", mask_pii(F.col("provider_name"))).drop("provider_name")
providers_new = providers_new.withColumn("effective_from", F.current_timestamp()) \
                             .withColumn("effective_to", F.lit(None).cast("timestamp")) \
                             .withColumn("is_current", F.lit(True))

if DeltaTable.isDeltaTable(spark, provider_dim_path):
    provider_delta = DeltaTable.forPath(spark, provider_dim_path)
    existing = provider_delta.toDF().select("provider_id", "provider_name_masked", "is_current")
    joined = providers_new.alias("new").join(existing.alias("old"), "provider_id", "left")
    changed = joined.filter((F.coalesce(F.col("old.provider_name_masked"), F.lit("")) != F.coalesce(F.col("new.provider_name_masked"), F.lit(""))) | F.col("old.provider_id").isNull())
    changed_ids = [r.provider_id for r in changed.select("provider_id").distinct().collect()]
    for pid in changed_ids:
        provider_delta.update(
            condition = F.expr(f"provider_id = '{pid}' AND is_current = true"),
            set = {"is_current": F.lit(False), "effective_to": F.current_timestamp()}
        )
    providers_to_append = providers_new.filter(F.col("provider_id").isin(changed_ids))
    (providers_to_append.write.format("delta").mode("append").save(provider_dim_path))
else:
    (providers_new.write.format("delta").mode("overwrite").save(provider_dim_path))

# ---------- Silver fact table ----------
# Join to masked patient/provider dims by patient_id/provider_id to ensure we don't store names in fact
# Read latest patient dim (is_current true)
patient_dim_df = spark.read.format("delta").load(patient_dim_path).where(F.col("is_current") == True).select("patient_id", "patient_name_masked")
provider_dim_df = spark.read.format("delta").load(provider_dim_path).where(F.col("is_current") == True).select("provider_id", "provider_name_masked")

fact = (bronze_df.join(patient_dim_df, on="patient_id", how="left")
        .join(provider_dim_df, on="provider_id", how="left")
        .select(
            "claim_id", "patient_id", "patient_name_masked",
            "provider_id", "provider_name_masked",
            "claim_amount", "diagnosis_code", "service_date", "received_date", "claim_status",
            F.current_timestamp().alias("loaded_ts")
        ))

# Write fact table as Delta partitioned by year/month from service_date
fact = fact.withColumn("year", F.year("service_date")).withColumn("month", F.month("service_date"))

fact.write.format("delta").mode("append").partitionBy("year", "month").save(silver_fact_path)

display(fact.limit(5))
