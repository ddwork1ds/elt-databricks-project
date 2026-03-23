from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(name="silver_order_items")
def silver_order_items_v2():

    df = spark.read.table("workspace.01_bronze.bronze_order_items")

    df = (
        df.select(
            F.trim(F.col("order_id")).alias("order_id"),
            F.col("order_item_id").cast("int").alias("order_item_id"),
            F.trim(F.col("product_id")).alias("product_id"),
            F.trim(F.col("seller_id")).alias("seller_id"),
            F.to_timestamp("shipping_limit_date").alias("shipping_limit_ts"),
            F.col("price").cast("double").alias("price"),
            F.col("freight_value").cast("double").alias("freight_value"),
            F.col("ingestion_time").cast("timestamp").alias("ingestion_time"),
            F.col("source_file").cast("string").alias("source_file")
        )
        .withColumn("order_id", F.when(F.col("order_id") == "", None).otherwise(F.col("order_id")))
        .withColumn("product_id", F.when(F.col("product_id") == "", None).otherwise(F.col("product_id")))
        .withColumn("seller_id", F.when(F.col("seller_id") == "", None).otherwise(F.col("seller_id")))
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("order_item_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("seller_id").isNotNull())
        .filter((F.col("price").isNotNull()) & (F.col("price") >= 0))
        .filter((F.col("freight_value").isNotNull()) & (F.col("freight_value") >= 0))
    )

    w = Window.partitionBy("order_id", "order_item_id").orderBy(F.col("ingestion_time").desc_nulls_last())

    return (
        df.withColumn("rn", F.row_number().over(w))
          .filter(F.col("rn") == 1)
          .drop("rn")
          .withColumn("item_total_value", F.col("price") + F.col("freight_value"))
          .withColumn("silver_processed_at", F.current_timestamp())
          .withColumn("record_source", F.lit("bronze_order_items"))
    )