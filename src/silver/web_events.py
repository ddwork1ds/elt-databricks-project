from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(name="silver_web_events_dev")
def silver_web_events():

    df = spark.read.table("workspace.01_bronze.bronze_web_events")

    df = (
        df.select(
            F.trim(F.col("event_id")).alias("event_id"),
            F.to_timestamp("event_time").alias("event_time"),
            F.trim(F.col("session_id")).alias("session_id"),
            F.trim(F.col("user_id")).alias("user_id"),
            F.lower(F.trim(F.col("event_type"))).alias("event_type"),
            F.trim(F.col("product_id")).alias("product_id"),
            F.trim(F.col("order_id")).alias("order_id"),
            F.col("quantity").cast("int").alias("quantity"),
            F.col("price_at_event").cast("double").alias("price_at_event"),
            F.trim(F.col("page_url")).alias("page_url"),
            F.trim(F.col("referrer")).alias("referrer"),
            F.lower(F.trim(F.col("utm_source"))).alias("utm_source"),
            F.lower(F.trim(F.col("utm_medium"))).alias("utm_medium"),
            F.lower(F.trim(F.col("utm_campaign"))).alias("utm_campaign"),
            F.trim(F.col("campaign_id")).alias("campaign_id"),
            F.lower(F.trim(F.col("device"))).alias("device"),
            F.lower(F.trim(F.col("browser"))).alias("browser"),
            F.lower(F.trim(F.col("store_channel"))).alias("store_channel"),
            F.col("ingestion_time").cast("timestamp").alias("ingestion_time"),
            F.col("source_file").cast("string").alias("source_file")
        )
        .withColumn("event_id", F.when(F.col("event_id") == "", None).otherwise(F.col("event_id")))
        .withColumn("session_id", F.when(F.col("session_id") == "", None).otherwise(F.col("session_id")))
        .withColumn("user_id", F.when(F.col("user_id") == "", None).otherwise(F.col("user_id")))
        .withColumn("event_type", F.when(F.col("event_type") == "", None).otherwise(F.col("event_type")))
        .withColumn("product_id", F.when(F.col("product_id") == "", None).otherwise(F.col("product_id")))
        .withColumn("order_id", F.when(F.col("order_id") == "", None).otherwise(F.col("order_id")))
        .withColumn("campaign_id", F.when(F.col("campaign_id") == "", None).otherwise(F.col("campaign_id")))
        .withColumn("page_url", F.when(F.col("page_url") == "", None).otherwise(F.col("page_url")))
        .withColumn("referrer", F.when(F.col("referrer") == "", None).otherwise(F.col("referrer")))
        .withColumn("utm_source", F.when(F.col("utm_source") == "", None).otherwise(F.col("utm_source")))
        .withColumn("utm_medium", F.when(F.col("utm_medium") == "", None).otherwise(F.col("utm_medium")))
        .withColumn("utm_campaign", F.when(F.col("utm_campaign") == "", None).otherwise(F.col("utm_campaign")))
        .withColumn("device", F.when(F.col("device") == "", None).otherwise(F.col("device")))
        .withColumn("browser", F.when(F.col("browser") == "", None).otherwise(F.col("browser")))
        .withColumn("store_channel", F.when(F.col("store_channel") == "", None).otherwise(F.col("store_channel")))
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("event_time").isNotNull())
        .filter(F.col("session_id").isNotNull())
        .filter(F.col("event_type").isin("view_product", "add_to_cart", "purchase"))
        .filter((F.col("quantity").isNull()) | (F.col("quantity") > 0))
        .filter((F.col("price_at_event").isNull()) | (F.col("price_at_event") >= 0))
        .filter(
            ((F.col("event_type") == "purchase") & F.col("order_id").isNotNull()) |
            (F.col("event_type") != "purchase")
        )
    )

    w = Window.partitionBy("event_id").orderBy(F.col("ingestion_time").desc_nulls_last())

    return (
        df.withColumn("rn", F.row_number().over(w))
          .filter(F.col("rn") == 1)
          .drop("rn")
          .withColumn("event_date", F.to_date("event_time"))
          .withColumn("event_hour", F.hour("event_time"))
          .withColumn(
              "normalized_channel",
              F.coalesce(F.col("utm_source"), F.col("referrer"), F.col("store_channel"))
          )
          .withColumn("silver_processed_at", F.current_timestamp())
          .withColumn("record_source", F.lit("bronze_web_events"))
    )