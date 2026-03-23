from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="quarantine_web_events")
def quarantine_web_events():

    df = spark.read.table("workspace.01_bronze.bronze_web_events")

    base = (
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
    )

    return (
        base.withColumn(
            "error_reason",
            F.when(F.col("event_id").isNull(), F.lit("missing_event_id"))
             .when(F.col("event_time").isNull(), F.lit("invalid_or_missing_event_time"))
             .when(F.col("session_id").isNull(), F.lit("missing_session_id"))
             .when(~F.col("event_type").isin("view_product", "add_to_cart", "purchase"), F.lit("invalid_event_type"))
             .when((F.col("event_type") == "purchase") & F.col("order_id").isNull(), F.lit("purchase_missing_order_id"))
             .when(F.col("quantity").isNotNull() & (F.col("quantity") <= 0), F.lit("invalid_quantity"))
             .when(F.col("price_at_event").isNotNull() & (F.col("price_at_event") < 0), F.lit("invalid_price_at_event"))
        )
        .filter(F.col("error_reason").isNotNull())
        .withColumn("quarantined_at", F.current_timestamp())
        .withColumn("record_source", F.lit("workspace.01_bronze.bronze_web_events"))
    )