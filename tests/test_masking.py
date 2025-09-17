import hashlib
from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, col

# Basic test to check masking/pseudonymization logic consistent with SHA256
def test_masking_consistency():
    s = "Pratik"
    # Python SHA256 hex
    py_hash = hashlib.sha256(s.encode("utf-8")).hexdigest()
    # Compute via Spark sha2 to ensure same output (requires spark)
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    df = spark.createDataFrame([(s,)], ["name"])
    df = df.select(sha2(col("name"), 256).alias("mask"))
    mask_val = df.collect()[0]["mask"]
    assert mask_val == py_hash
    spark.stop()
