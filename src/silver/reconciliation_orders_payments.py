from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="reconciliation_orders_payments")
def reconciliation_orders_payments():

    items = (
        spark.read.table("silver_order_items")
        .groupBy("order_id")
        .agg(
            F.count("*").alias("item_count"),
            F.sum("price").alias("items_price_total"),
            F.sum("freight_value").alias("freight_total"),
            F.sum("item_total_value").alias("items_grand_total")
        )
    )

    payments = (
        spark.read.table("silver_order_payments_dev")
        .groupBy("order_id")
        .agg(
            F.count("*").alias("payment_record_count"),
            F.sum("payment_value").alias("payment_total")
        )
    )

    orders = spark.read.table("silver_orders_dev").select("order_id", "order_status", "order_purchase_date")

    return (
        orders.alias("o")
        .join(items.alias("i"), on="order_id", how="left")
        .join(payments.alias("p"), on="order_id", how="left")
        .withColumn("item_count", F.coalesce(F.col("item_count"), F.lit(0)))
        .withColumn("payment_record_count", F.coalesce(F.col("payment_record_count"), F.lit(0)))
        .withColumn("items_price_total", F.coalesce(F.col("items_price_total"), F.lit(0.0)))
        .withColumn("freight_total", F.coalesce(F.col("freight_total"), F.lit(0.0)))
        .withColumn("items_grand_total", F.coalesce(F.col("items_grand_total"), F.lit(0.0)))
        .withColumn("payment_total", F.coalesce(F.col("payment_total"), F.lit(0.0)))
        .withColumn("difference_amount", F.round(F.col("payment_total") - F.col("items_grand_total"), 2))
        .withColumn(
            "reconciliation_status",
            F.when((F.col("item_count") == 0) & (F.col("payment_record_count") == 0), F.lit("missing_items_and_payments"))
             .when(F.col("item_count") == 0, F.lit("missing_items"))
             .when(F.col("payment_record_count") == 0, F.lit("missing_payments"))
             .when(F.abs(F.col("difference_amount")) <= F.lit(0.01), F.lit("matched"))
             .otherwise(F.lit("mismatched"))
        )
        .withColumn("checked_at", F.current_timestamp())
        .withColumn("record_source", F.lit("silver_order_items + silver_order_payments"))
    )