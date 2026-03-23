from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="bridge_identity")
def bridge_identity():

    web = spark.read.table("silver_web_events").alias("w")
    customers = spark.read.table("customers_dev").alias("c")

    return (
        web.filter(F.col("w.user_id").isNotNull())
           .join(
               customers,
               F.col("w.user_id") == F.col("c.customer_unique_id"),
               "inner"
           )
           .groupBy(
               F.col("w.user_id").alias("website_user_id"),
               F.col("c.customer_unique_id").alias("customer_unique_id"),
               F.col("c.customer_id").alias("customer_id")
           )
           .agg(
               F.min("w.event_time").alias("first_seen_at"),
               F.max("w.event_time").alias("last_seen_at"),
               F.countDistinct("w.session_id").alias("session_count"),
               F.count("*").alias("event_count")
           )
           .withColumn("match_rule", F.lit("web.user_id = customers.customer_unique_id"))
           .withColumn("is_current", F.lit(True))
           .withColumn("silver_processed_at", F.current_timestamp())
           .withColumn("record_source", F.lit("silver_web_events + silver_customers"))
    )