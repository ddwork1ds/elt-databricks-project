from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(name="silver_customers")
def silver_customers():

    df = spark.read.table("workspace.01_bronze.bronze_customers")

    df = (
        df.select(
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.trim(F.col("customer_unique_id")).alias("customer_unique_id"),
            F.col("customer_zip_code_prefix").cast("int").alias("customer_zip_code_prefix"),
            F.trim(F.col("customer_city")).alias("customer_city"),
            F.upper(F.trim(F.col("customer_state"))).alias("customer_state"),
            F.col("ingestion_time").cast("timestamp").alias("ingestion_time"),
            F.col("source_file").cast("string").alias("source_file")
        )
        .withColumn("customer_id", F.when(F.col("customer_id") == "", None).otherwise(F.col("customer_id")))
        .withColumn("customer_unique_id", F.when(F.col("customer_unique_id") == "", None).otherwise(F.col("customer_unique_id")))
        .withColumn("customer_city", F.when(F.col("customer_city") == "", None).otherwise(F.col("customer_city")))
        .withColumn("customer_state", F.when(F.col("customer_state") == "", None).otherwise(F.col("customer_state")))
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("customer_unique_id").isNotNull())
        .filter(F.col("customer_zip_code_prefix").isNotNull())
    )

    w = Window.partitionBy("customer_id").orderBy(F.col("ingestion_time").desc_nulls_last())

    return (
        df.withColumn("rn", F.row_number().over(w))
          .filter(F.col("rn") == 1)
          .drop("rn")
          .withColumn("silver_processed_at", F.current_timestamp())
          .withColumn("record_source", F.lit("bronze_customers"))
    )