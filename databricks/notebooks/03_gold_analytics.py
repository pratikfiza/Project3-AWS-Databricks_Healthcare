# Databricks Gold analytics notebook (script)
# Produces aggregated datasets for BI: costs per provider, per diagnosis, monthly trends
from pyspark.sql import functions as F

silver_fact_path = dbutils.widgets.get("silver_fact_path") if 'dbutils' in globals() else "s3a://my-claims-bucket/silver/claims_fact/"
gold_costs_path = dbutils.widgets.get("gold_costs_path") if 'dbutils' in globals() else "s3a://my-claims-bucket/gold/costs/"

df = spark.read.format("delta").load(silver_fact_path)

# Total claims and total cost by provider (last 30 days)
recent = (df.filter(F.col("service_date") >= F.date_sub(F.current_date(), 30))
          .groupBy("provider_id", "provider_name_masked")
          .agg(F.count("*").alias("claim_count"), F.sum("claim_amount").alias("total_cost"))
          .orderBy(F.desc("total_cost")))

# Save gold table
(recent.write.format("delta")
 .mode("overwrite")
 .save(gold_costs_path))

display(recent.limit(20))

# ------------ Simple fraud scoring (illustrative) ------------
# Flag claims with unusual amounts (e.g., > mean + 3*std for provider)
stats = df.groupBy("provider_id").agg(F.mean("claim_amount").alias("mean_amt"), F.stddev("claim_amount").alias("std_amt"))
joined = df.join(stats, on="provider_id", how="left")
scored = joined.withColumn("zscore", (F.col("claim_amount") - F.col("mean_amt"))/F.col("std_amt"))
suspicious = scored.filter(F.col("zscore") > 3).select("claim_id", "provider_id", "claim_amount", "zscore")
display(suspicious)
