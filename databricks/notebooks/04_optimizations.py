# Databricks optimizations notebook
from pyspark.sql import functions as F
from delta.tables import DeltaTable

silver_fact_path = dbutils.widgets.get("silver_fact_path") if 'dbutils' in globals() else "s3a://my-claims-bucket/silver/claims_fact/"
patient_dim_path = dbutils.widgets.get("patient_dim_path") if 'dbutils' in globals() else "s3a://my-claims-bucket/silver/dim_patient/"

# Example: OPTIMIZE & ZORDER silver fact by provider_id for faster provider queries
try:
    spark.sql(f"OPTIMIZE delta.`{silver_fact_path}` ZORDER BY (provider_id)")
except Exception as e:
    print("Optimize not supported in this environment:", e)

# Vacuum old files to reclaim space (retention 168 hours -> 7 days)
try:
    dt = DeltaTable.forPath(spark, silver_fact_path)
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
    dt.vacuum(168)  # in hours
except Exception as e:
    print("Vacuum may be unsupported here:", e)

# Display partition list
display(spark.sql(f"SHOW PARTITIONS delta.`{silver_fact_path}`"))
