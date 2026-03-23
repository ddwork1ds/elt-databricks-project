from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="fact_web_funnel_daily")
def fact_web_funnel_daily():

    web = spark.read.table("workspace.02_silver.silver_web_events").alias("w")
    campaigns = spark.read.table("workspace.03_gold.dim_campaign").alias("c")
    channels = spark.read.table("workspace.03_gold.dim_channel").alias("ch")

    enriched = (
        web.join(campaigns, on="campaign_id", how="left")
           .join(
               channels,
               (
                   (F.coalesce(F.col("c.channel"), F.lit("unknown")) == F.col("ch.channel_name")) &
                   (F.coalesce(F.col("c.source"), F.lit("unknown")) == F.col("ch.source")) &
                   (F.coalesce(F.col("c.medium"), F.lit("unknown")) == F.col("ch.medium"))
               ),
               how="left"
           )
    )

    return (
        enriched.groupBy(
            F.date_format("event_date", "yyyyMMdd").cast("int").alias("date_key"),
            F.col("campaign_key"),
            F.col("channel_key"),
            F.coalesce(F.col("device"), F.lit("unknown")).alias("device")
        )
        .agg(
            F.sum(F.when(F.col("event_type") == "view_product", 1).otherwise(0)).alias("view_product_count"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_event_count"),
            F.countDistinct("session_id").alias("session_count"),
            F.countDistinct("user_id").alias("user_count")
        )
        .withColumn(
            "funnel_conversion_rate",
            F.when(F.col("view_product_count") > 0, F.col("purchase_event_count") / F.col("view_product_count"))
        )
    )