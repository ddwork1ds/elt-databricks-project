from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="fact_orders")
def fact_orders():

    orders = spark.read.table("workspace.02_silver.silver_orders").alias("o")

    items_agg = (
        spark.read.table("workspace.02_silver.silver_order_items")
        .groupBy("order_id")
        .agg(
            F.count("*").alias("item_count"),
            F.sum("price").alias("items_price_total"),
            F.sum("freight_value").alias("freight_total"),
            F.sum("item_total_value").alias("gross_order_value")
        )
        .alias("i")
    )

    payments_agg = (
        spark.read.table("workspace.02_silver.silver_order_payments")
        .groupBy("order_id")
        .agg(
            F.count("*").alias("payment_record_count"),
            F.sum("payment_value").alias("payment_total")
        )
        .alias("p")
    )

    customers = spark.read.table("workspace.03_gold.dim_customer").alias("c")
    geo = spark.read.table("workspace.03_gold.dim_geolocation").alias("g")

    return (
        orders.join(items_agg, on="order_id", how="left")
              .join(payments_agg, on="order_id", how="left")
              .join(customers, on="customer_id", how="left")
              .join(geo,F.col("c.customer_zip_code_prefix") == F.col("g.zip_code_prefix"),how="left")
              .select(
                  F.col("order_id"),
                  F.date_format("order_purchase_date", "yyyyMMdd").cast("int").alias("order_date_key"),
                  F.col("customer_key"),
                  F.col("customer_id"),
                  F.col("g.geolocation_key").alias("customer_geolocation_key"),
                  F.col("order_status"),
                  F.coalesce(F.col("item_count"), F.lit(0)).alias("item_count"),
                  F.round(F.coalesce(F.col("items_price_total"), F.lit(0.0)), 2).cast("decimal(12,2)").alias("items_price_total"),
                  F.round(F.coalesce(F.col("freight_total"), F.lit(0.0)), 2).cast("decimal(12,2)").alias("freight_total"),
                  F.round(F.coalesce(F.col("gross_order_value"), F.lit(0.0)), 2).cast("decimal(12,2)").alias("gross_order_value"),
                  F.coalesce(F.col("payment_record_count"), F.lit(0)).alias("payment_record_count"),
                  F.round(F.coalesce(F.col("payment_total"), F.lit(0.0)), 2).cast("decimal(12,2)").alias("payment_total"),
                  F.round(
                  F.coalesce(F.col("payment_total"), F.lit(0.0)) -F.coalesce(F.col("gross_order_value"), F.lit(0.0)),2).alias("difference_amount"),
                  F.when(F.col("order_status") == "delivered", F.lit(1)).otherwise(F.lit(0)).alias("is_delivered"),
                  F.when(F.col("order_status") == "canceled", F.lit(1)).otherwise(F.lit(0)).alias("is_canceled"),
                  F.col("delivery_delay_days"),
                  F.col("order_purchase_ts"),
                  F.col("order_approved_at"),
                  F.col("order_delivered_carrier_ts"),
                  F.col("order_delivered_customer_ts"),
                  F.col("order_estimated_delivery_ts")
              )
    )