from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="dim_campaign")
def dim_campaign():
    campaigns = spark.read.table("workspace.02_silver.bridge_campaign_mapping")

    return (
        campaigns.select(
            F.pmod(F.xxhash64(F.col("campaign_id")), F.lit(9223372036854775807)).alias("campaign_key"),
            F.col("campaign_id"),
            F.col("ads_campaign_name"),
            F.col("utm_campaign"),
            F.col("source"),
            F.col("medium"),
            F.col("channel"),
            F.col("mapping_quality"),
            F.col("first_seen_date"),
            F.col("last_seen_date")
        )
        .dropDuplicates(["campaign_id"])
    )