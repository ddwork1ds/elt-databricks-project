from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="dim_seller")
def dim_seller():

    sellers = spark.read.table("workspace.02_silver.silver_sellers")

    return (
        sellers.select(
            F.pmod(F.xxhash64("seller_id"), F.lit(9223372036854775807)).alias("seller_key"),
            F.col("seller_id"),
            F.col("seller_zip_code_prefix"),
            F.col("seller_city"),
            F.col("seller_state")
        )
    )