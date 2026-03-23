from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="dim_product")
def dim_product():

    products = spark.read.table("workspace.02_silver.silver_products_enriched")

    return (
        products.select(
            F.pmod(F.xxhash64("product_id"), F.lit(9223372036854775807)).alias("product_key"),
            F.col("product_id"),
            F.col("product_category_name"),
            F.col("product_category_name_english"),
            F.col("product_name_length"),
            F.col("product_description_length"),
            F.col("product_photos_qty"),
            F.col("product_weight_g"),
            F.col("product_length_cm"),
            F.col("product_height_cm"),
            F.col("product_width_cm")
        )
    )