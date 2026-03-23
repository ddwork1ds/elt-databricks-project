from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(name="silver_sellers")
def silver_sellers_v2():

    df = spark.read.table("workspace.01_bronze.bronze_sellers")

    df = (
        df.select(
            F.trim(F.col("seller_id")).alias("seller_id"),
            F.col("seller_zip_code_prefix").cast("int").alias("seller_zip_code_prefix"),
            F.trim(F.col("seller_city")).alias("seller_city"),
            F.upper(F.trim(F.col("seller_state"))).alias("seller_state"),
            F.col("ingestion_time").cast("timestamp").alias("ingestion_time"),
            F.col("source_file").cast("string").alias("source_file")
        )
        .withColumn("seller_id", F.when(F.col("seller_id") == "", None).otherwise(F.col("seller_id")))
        .withColumn("seller_city", F.when(F.col("seller_city") == "", None).otherwise(F.col("seller_city")))
        .withColumn("seller_state", F.when(F.col("seller_state") == "", None).otherwise(F.col("seller_state")))
        .filter(F.col("seller_id").isNotNull())
        .filter(F.col("seller_zip_code_prefix").isNotNull())
        .filter(F.col("seller_state").isNotNull())
    )

    w = Window.partitionBy("seller_id").orderBy(F.col("ingestion_time").desc_nulls_last())

    return (
        df.withColumn("rn", F.row_number().over(w))
          .filter(F.col("rn") == 1)
          .drop("rn")
          .withColumn("silver_processed_at", F.current_timestamp())
          .withColumn("record_source", F.lit("bronze_sellers"))
    )