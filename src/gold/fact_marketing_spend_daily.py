from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="fact_marketing_spend_daily")
def fact_marketing_spend_daily():

    ads = spark.read.table("workspace.02_silver.silver_ads_campaigns").alias("a")
    campaigns = spark.read.table("workspace.03_gold.dim_campaign").alias("c")

    return (
        ads.join(campaigns, on="campaign_id", how="left")
           .select(
               F.date_format("campaign_date", "yyyyMMdd").cast("int").alias("date_key"),
               F.col("campaign_key"),
               F.col("campaign_id"),
               F.col("campaign_name"),
               F.col("a.channel").alias("channel"),
               F.col("a.source").alias("source"),
               F.col("a.medium").alias("medium"),
               F.col("impressions"),
               F.col("clicks"),
               F.col("conversions"),
               F.col("cost"),
               F.col("currency"),
               F.round(F.col("ctr"),4).cast("decimal(6,4)").alias("ctr"),
               F.round(F.col("cvr"),4).cast("decimal(6,4)").alias("cvr"),
               F.round(F.col("cpc"),2).cast("decimal(10,2)").alias("cpc")
           )
    )
