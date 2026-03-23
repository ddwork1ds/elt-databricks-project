from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(name="silver_ads_campaigns")
def silver_ads_campaigns():

    df = spark.read.table("workspace.01_bronze.bronze_ads_campaigns")

    df = (
        df.select(
            F.to_date("date").alias("campaign_date"),
            F.trim(F.col("campaign_id")).alias("campaign_id"),
            F.trim(F.col("campaign_name")).alias("campaign_name"),
            F.lower(F.trim(F.col("channel"))).alias("channel"),
            F.lower(F.trim(F.col("source"))).alias("source"),
            F.lower(F.trim(F.col("medium"))).alias("medium"),
            F.col("impressions").cast("int").alias("impressions"),
            F.col("clicks").cast("int").alias("clicks"),
            F.col("conversions").cast("int").alias("conversions"),
            F.col("cost").cast("double").alias("cost"),
            F.upper(F.trim(F.col("currency"))).alias("currency"),
            F.col("ingestion_time").cast("timestamp").alias("ingestion_time"),
            F.col("source_api").cast("string").alias("source_api")
        )
        .withColumn("campaign_id", F.when(F.col("campaign_id") == "", None).otherwise(F.col("campaign_id")))
        .withColumn("campaign_name", F.when(F.col("campaign_name") == "", None).otherwise(F.col("campaign_name")))
        .withColumn("channel", F.when(F.col("channel") == "", None).otherwise(F.col("channel")))
        .withColumn("source", F.when(F.col("source") == "", None).otherwise(F.col("source")))
        .withColumn("medium", F.when(F.col("medium") == "", None).otherwise(F.col("medium")))
        .withColumn("currency", F.when(F.col("currency") == "", None).otherwise(F.col("currency")))
        .filter(F.col("campaign_date").isNotNull())
        .filter(F.col("campaign_id").isNotNull())
        .filter(F.col("campaign_name").isNotNull())
        .filter(F.col("impressions").isNotNull() & (F.col("impressions") >= 0))
        .filter(F.col("clicks").isNotNull() & (F.col("clicks") >= 0))
        .filter(F.col("conversions").isNotNull() & (F.col("conversions") >= 0))
        .filter(F.col("cost").isNotNull() & (F.col("cost") >= 0))
        .filter(F.col("clicks") <= F.col("impressions"))
        .filter(F.col("conversions") <= F.col("clicks"))
    )

    w = Window.partitionBy("campaign_date", "campaign_id").orderBy(F.col("ingestion_time").desc_nulls_last())

    return (
        df.withColumn("rn", F.row_number().over(w))
          .filter(F.col("rn") == 1)
          .drop("rn")
          .withColumn("ctr", F.when(F.col("impressions") > 0, F.col("clicks") / F.col("impressions")))
          .withColumn("cvr", F.when(F.col("clicks") > 0, F.col("conversions") / F.col("clicks")))
          .withColumn("cpc", F.when(F.col("clicks") > 0, F.col("cost") / F.col("clicks")))
          .withColumn("silver_processed_at", F.current_timestamp())
          .withColumn("record_source", F.lit("bronze_ads_campaigns"))
    )