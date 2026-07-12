from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ingestion_bronze").getOrCreate()

raw_path = "/opt/data/raw/*.json"
bronze_path = "/opt/data/bronze/tomtom"

df = spark.read.option("multiLine", True).json(raw_path)

# keep raw structure intact (multi-city supported)
df.write.mode("append").parquet(bronze_path)

spark.stop()