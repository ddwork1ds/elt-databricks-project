from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(name="silver_orders")
def silver_orders():

    df = spark.read.table("workspace.01_bronze.bronze_orders")

    df = (
        df.select(
            F.trim(F.col("order_id")).alias("order_id"),
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.lower(F.trim(F.col("order_status"))).alias("order_status"),
            F.to_timestamp("order_purchase_timestamp").alias("order_purchase_ts"),
            F.to_timestamp("order_approved_at").alias("order_approved_at"),
            F.to_timestamp("order_delivered_carrier_date").alias("order_delivered_carrier_ts"),
            F.to_timestamp("order_delivered_customer_date").alias("order_delivered_customer_ts"),
            F.to_timestamp("order_estimated_delivery_date").alias("order_estimated_delivery_ts"),
            F.col("ingestion_time").cast("timestamp").alias("ingestion_time"),
            F.col("source_file").cast("string").alias("source_file")
        )
        .withColumn("order_id", F.when(F.col("order_id") == "", None).otherwise(F.col("order_id")))
        .withColumn("customer_id", F.when(F.col("customer_id") == "", None).otherwise(F.col("customer_id")))
        .withColumn("order_status", F.when(F.col("order_status") == "", None).otherwise(F.col("order_status")))
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("order_purchase_ts").isNotNull())
        .filter(
            F.col("order_status").isin(
                "created", "approved", "invoiced", "processing",
                "shipped", "delivered", "unavailable", "canceled"
            )
        )
        .withColumn("order_purchase_date", F.to_date("order_purchase_ts"))
        .withColumn("order_approved_date", F.to_date("order_approved_at"))
        .withColumn(
            "delivery_delay_days",
            F.when(
                F.col("order_delivered_customer_ts").isNotNull() & F.col("order_estimated_delivery_ts").isNotNull(),
                F.datediff(F.to_date("order_delivered_customer_ts"), F.to_date("order_estimated_delivery_ts"))
            )
        )
    )

    w = Window.partitionBy("order_id").orderBy(F.col("ingestion_time").desc_nulls_last())

    return (
        df.withColumn("rn", F.row_number().over(w))
          .filter(F.col("rn") == 1)
          .drop("rn")
          .withColumn("silver_processed_at", F.current_timestamp())
          .withColumn("record_source", F.lit("bronze_orders"))
    )