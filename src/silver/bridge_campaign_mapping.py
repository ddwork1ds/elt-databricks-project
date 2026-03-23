from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="bridge_campaign_mapping")
def bridge_campaign_mapping():

    web = spark.read.table("silver_web_events").alias("w")
    ads = spark.read.table("silver_ads_campaigns").alias("a")

    joined = (
        web.join(
            ads,
            F.col("w.campaign_id") == F.col("a.campaign_id"),
            "full"
        )
        .select(
            F.coalesce(F.col("w.campaign_id"), F.col("a.campaign_id")).alias("campaign_id"),
            F.col("a.campaign_name").alias("ads_campaign_name"),
            F.col("w.utm_campaign").alias("utm_campaign"),
            F.coalesce(F.col("w.utm_source"), F.col("a.source")).alias("source"),
            F.coalesce(F.col("w.utm_medium"), F.col("a.medium")).alias("medium"),
            F.col("a.channel").alias("channel"),
            F.coalesce(F.col("w.event_date"), F.col("a.campaign_date")).alias("activity_date")
        )
        .filter(F.col("campaign_id").isNotNull())
    )

    return (
        joined.groupBy(
            "campaign_id",
            "ads_campaign_name",
            "utm_campaign",
            "source",
            "medium",
            "channel"
        )
        .agg(
            F.min("activity_date").alias("first_seen_date"),
            F.max("activity_date").alias("last_seen_date")
        )
        .withColumn(
            "mapping_quality",
            F.when(F.col("ads_campaign_name").isNotNull() & F.col("utm_campaign").isNotNull(), F.lit("high"))
             .when(F.col("ads_campaign_name").isNotNull() | F.col("utm_campaign").isNotNull(), F.lit("medium"))
             .otherwise(F.lit("low"))
        )
        .withColumn("match_rule", F.lit("campaign_id direct match"))
        .withColumn("silver_processed_at", F.current_timestamp())
        .withColumn("record_source", F.lit("silver_web_events + silver_ads_campaigns"))
    )