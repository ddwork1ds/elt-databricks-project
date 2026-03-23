from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="dim_customer")
def dim_customer():

    customers = spark.read.table("workspace.02_silver.silver_customers").alias("c")

    first_orders = (
        spark.read.table("workspace.02_silver.silver_orders")
        .groupBy("customer_id")
        .agg(
            F.min("order_purchase_date").alias("first_order_date"),
            F.countDistinct("order_id").alias("total_orders")
        )
        .alias("o")
    )

    return (
        customers.join(first_orders, on="customer_id", how="left")
        .select(
            F.pmod(F.xxhash64("customer_id"), F.lit(9223372036854775807)).alias("customer_key"),
            F.col("customer_id"),
            F.col("customer_unique_id"),
            F.col("customer_zip_code_prefix"),
            F.col("customer_city"),
            F.col("customer_state"),
            F.col("first_order_date"),
            F.coalesce(F.col("total_orders"), F.lit(0)).alias("total_orders"),
            F.when(F.coalesce(F.col("total_orders"), F.lit(0)) > 1, F.lit("repeat"))
             .when(F.coalesce(F.col("total_orders"), F.lit(0)) == 1, F.lit("new"))
             .otherwise(F.lit("unknown"))
             .alias("customer_type")
        )
    )