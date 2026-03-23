from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="fact_order_items")
def fact_order_items():

    order_items = spark.read.table("workspace.02_silver.silver_order_items").alias("oi")
    orders = spark.read.table("workspace.02_silver.silver_orders").alias("o")
    products = spark.read.table("workspace.03_gold.dim_product").alias("p")
    sellers = spark.read.table("workspace.03_gold.dim_seller").alias("s")
    geo = spark.read.table("workspace.03_gold.dim_geolocation").alias("g")

    return (
        order_items.join(orders, on="order_id", how="left")
                   .join(products, on="product_id", how="left")
                   .join(sellers, on="seller_id", how="left")
                   .join(
                       geo,
                       F.col("s.seller_zip_code_prefix") == F.col("g.zip_code_prefix"),
                       how="left"
                   )
                   .select(
                       F.col("order_id"),
                       F.col("order_item_id"),
                       F.date_format("order_purchase_date", "yyyyMMdd").cast("int").alias("order_date_key"),
                       F.col("product_key"),
                       F.col("product_id"),
                       F.col("seller_key"),
                       F.col("seller_id"),
                       F.col("g.geolocation_key").alias("seller_geolocation_key"),
                       F.col("price"),
                       F.col("freight_value"),
                       F.round(F.col("item_total_value"),2).cast("decimal(6,4)").alias("item_total_value"),
                       F.col("shipping_limit_ts"),
                       F.col("order_status")
                   )
    )