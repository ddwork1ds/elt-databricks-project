from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="dim_geolocation")
def dim_geolocation():

    geo = spark.read.table("workspace.02_silver.silver_geolocation")

    return (
        geo.select(
            F.pmod(
                F.xxhash64(
                    F.col("zip_code_prefix"),
                    F.coalesce(F.col("city"), F.lit("unknown")),
                    F.coalesce(F.col("state"), F.lit("unknown"))
                ),
                F.lit(9223372036854775807)
            ).alias("geolocation_key"),
            F.col("zip_code_prefix"),
            F.col("city"),
            F.col("state"),
            F.col("geolocation_lat").alias("latitude"),
            F.col("geolocation_lng").alias("longitude")
        )
        .dropDuplicates(["zip_code_prefix", "city", "state"])
    )