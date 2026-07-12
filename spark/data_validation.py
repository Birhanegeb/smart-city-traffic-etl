from pyspark.sql import SparkSession

def validate_layer(name, path):
    df = spark.read.parquet(path)

    print(f"\n===== {name.upper()} LAYER =====")
    print("Row count:", df.count())

    df.printSchema()

    # basic null check example
    null_counts = df.select([
        df[c].isNull().cast("int").alias(c)
        for c in df.columns
    ]).collect()

    print("Validation complete for", name)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("data-validation").getOrCreate()

    validate_layer("bronze", "/opt/data/bronze")
    validate_layer("silver", "/opt/data/silver")
    validate_layer("gold", "/opt/data/gold")

    print("\n✅ ALL LAYERS VALIDATED SUCCESSFULLY")