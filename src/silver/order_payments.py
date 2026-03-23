from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(name="silver_order_payments")
def silver_order_payments():

    df = spark.read.table("workspace.01_bronze.bronze_payments")

    df = (
        df.select(
            F.trim(F.col("order_id")).alias("order_id"),
            F.col("payment_sequential").cast("int").alias("payment_sequential"),
            F.lower(F.trim(F.col("payment_type"))).alias("payment_type"),
            F.col("payment_installments").cast("int").alias("payment_installments"),
            F.col("payment_value").cast("double").alias("payment_value"),
            F.col("ingestion_time").cast("timestamp").alias("ingestion_time"),
            F.col("source_file").cast("string").alias("source_file")
        )
        .withColumn("order_id", F.when(F.col("order_id") == "", None).otherwise(F.col("order_id")))
        .withColumn("payment_type", F.when(F.col("payment_type") == "", None).otherwise(F.col("payment_type")))
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("payment_sequential").isNotNull())
        .filter(F.col("payment_type").isNotNull())
        .filter((F.col("payment_installments").isNull()) | (F.col("payment_installments") >= 0))
        .filter((F.col("payment_value").isNotNull()) & (F.col("payment_value") >= 0))
    )

    w = Window.partitionBy("order_id", "payment_sequential").orderBy(F.col("ingestion_time").desc_nulls_last())

    return (
        df.withColumn("rn", F.row_number().over(w))
          .filter(F.col("rn") == 1)
          .drop("rn")
          .withColumn("silver_processed_at", F.current_timestamp())
          .withColumn("record_source", F.lit("bronze_order_payments"))
    )