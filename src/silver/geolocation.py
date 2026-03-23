from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(name="silver_geolocation")
def silver_geolocation():

    df = spark.read.table("workspace.01_bronze.bronze_geolocation")

    df = (
        df.select(
            F.col("geolocation_zip_code_prefix").cast("int").alias("zip_code_prefix"),
            F.col("geolocation_lat").cast("double").alias("geolocation_lat"),
            F.col("geolocation_lng").cast("double").alias("geolocation_lng"),
            F.trim(F.col("geolocation_city")).alias("city"),
            F.upper(F.trim(F.col("geolocation_state"))).alias("state"),
            F.col("ingestion_time").cast("timestamp").alias("ingestion_time"),
            F.col("source_file").cast("string").alias("source_file")
        )
        .withColumn("city", F.when(F.col("city") == "", None).otherwise(F.col("city")))
        .withColumn("state", F.when(F.col("state") == "", None).otherwise(F.col("state")))
        .filter(F.col("zip_code_prefix").isNotNull())
        .filter(F.col("geolocation_lat").isNotNull())
        .filter(F.col("geolocation_lng").isNotNull())
        .filter(F.col("state").isNotNull())
        .filter((F.col("geolocation_lat") >= -90) & (F.col("geolocation_lat") <= 90))
        .filter((F.col("geolocation_lng") >= -180) & (F.col("geolocation_lng") <= 180))
    )

    w = Window.partitionBy("zip_code_prefix", "city", "state").orderBy(F.col("ingestion_time").desc_nulls_last())

    return (
        df.withColumn("rn", F.row_number().over(w))
          .filter(F.col("rn") == 1)
          .drop("rn")
          .withColumn("silver_processed_at", F.current_timestamp())
          .withColumn("record_source", F.lit("bronze_geolocation"))
    )