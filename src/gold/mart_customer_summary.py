from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="mart_customer_summary")
def fact_customer_summary():

    customers = spark.read.table("workspace.03_gold.dim_customer").alias("c")
    orders = spark.read.table("workspace.03_gold.fact_orders").alias("fo")
    reviews = spark.read.table("workspace.02_silver.silver_order_reviews").alias("r")
    geo = spark.read.table("workspace.03_gold.dim_geolocation").alias("g")

    order_metrics = (
        orders.groupBy("customer_key", "customer_id", "customer_geolocation_key")
              .agg(
                  F.countDistinct("order_id").alias("total_orders"),
                  F.sum("payment_total").alias("total_revenue"),
                  F.avg("payment_total").alias("avg_order_value"),
                  F.sum("is_delivered").alias("delivered_order_count"),
                  F.sum("is_canceled").alias("canceled_order_count"),
                  F.min("order_date_key").alias("first_order_date_key"),
                  F.max("order_date_key").alias("last_order_date_key")
              )
              .alias("om")
    )

    review_metrics = (
        spark.read.table("workspace.02_silver.silver_orders").alias("o")
        .join(reviews, on="order_id", how="left")
        .groupBy("customer_id")
        .agg(
            F.avg("review_score").alias("avg_review_score"),
            F.count("review_id").alias("review_count")
        )
        .alias("rm")
    )

    return (
        customers.join(order_metrics, on=["customer_key", "customer_id"], how="left")
                 .join(review_metrics, on="customer_id", how="left")
                 .join(
                     geo,
                     F.col("c.customer_zip_code_prefix") == F.col("g.zip_code_prefix"),
                     how="left"
                 )
                 .select(
                     F.col("customer_key"),
                     F.col("customer_id"),
                     F.col("customer_unique_id"),
                     F.col("g.geolocation_key").alias("customer_geolocation_key"),
                     F.col("customer_zip_code_prefix"),
                     F.col("customer_city"),
                     F.col("customer_state"),
                     F.coalesce(F.col("om.total_orders"), F.lit(0)).alias("total_orders"),
                     F.coalesce(F.round(F.col("total_revenue"), 2), F.lit(0.0)).alias("total_revenue"),
                     F.coalesce(F.round(F.col("avg_order_value"), 2), F.lit(0.0)).alias("avg_order_value"),
                     F.coalesce(F.col("delivered_order_count"), F.lit(0)).alias("delivered_order_count"),
                     F.coalesce(F.col("canceled_order_count"), F.lit(0)).alias("canceled_order_count"),
                     F.col("first_order_date_key"),
                     F.col("last_order_date_key"),
                     F.round(F.col("avg_review_score"), 2).alias("avg_review_score"),
                     F.coalesce(F.col("review_count"), F.lit(0)).alias("review_count"),
                     F.when(F.coalesce(F.col("om.total_orders"), F.lit(0)) > 1, F.lit(1)).otherwise(F.lit(0)).alias("is_repeat_customer"),
                     F.when(F.coalesce(F.col("total_revenue"), F.lit(0.0)) >= 500, F.lit("high_value"))
                      .when(F.coalesce(F.col("total_revenue"), F.lit(0.0)) >= 200, F.lit("mid_value"))
                      .when(F.coalesce(F.col("total_revenue"), F.lit(0.0)) > 0, F.lit("low_value"))
                      .otherwise(F.lit("no_purchase"))
                      .alias("customer_segment")
                 )
    )