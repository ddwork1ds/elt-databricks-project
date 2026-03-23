from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="mart_sales_marketing_360")
def mart_sales_marketing_360():

    spend = spark.read.table("workspace.03_gold.fact_marketing_spend_daily").alias("s")

    funnel = (
        spark.read.table("workspace.03_gold.fact_web_funnel_daily")
        .groupBy("date_key", "campaign_key")
        .agg(
            F.sum("view_product_count").alias("views"),
            F.sum("add_to_cart_count").alias("add_to_carts"),
            F.sum("purchase_event_count").alias("purchase_events"),
            F.sum("session_count").alias("sessions"),
            F.sum("user_count").alias("users")
        )
        .alias("f")
    )

    campaign_dim = spark.read.table("workspace.03_gold.dim_campaign").alias("dc")

    attributed_orders = (
        spark.read.table("workspace.02_silver.silver_web_events").alias("w")
        .filter((F.col("event_type") == "purchase") & F.col("order_id").isNotNull())
        .join(
            spark.read.table("workspace.03_gold.fact_orders").alias("fo"),
            on="order_id",
            how="left"
        )
        .join(campaign_dim, on="campaign_id", how="left")
        .groupBy(
            F.date_format("event_date", "yyyyMMdd").cast("int").alias("date_key"),
            F.col("campaign_key")
        )
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum("payment_total").alias("revenue")
        )
        .alias("ao")
    )

    return (
        spend.join(funnel, on=["date_key", "campaign_key"], how="full")
             .join(attributed_orders, on=["date_key", "campaign_key"], how="full")
             .join(campaign_dim, on="campaign_key", how="left")
             .select(
                 F.col("date_key"),
                 F.col("campaign_key"),
                 F.col("dc.campaign_id"),
                 F.col("ads_campaign_name").alias("campaign_name"),
                 F.col("dc.channel"),
                 F.col("dc.source"),
                 F.col("dc.medium"),
                 F.coalesce(F.col("impressions"), F.lit(0)).alias("impressions"),
                 F.coalesce(F.col("clicks"), F.lit(0)).alias("clicks"),
                 F.coalesce(F.col("conversions"), F.lit(0)).alias("conversions"),
                 F.coalesce(F.col("cost"), F.lit(0.0)).alias("cost"),
                 F.coalesce(F.col("sessions"), F.lit(0)).alias("sessions"),
                 F.coalesce(F.col("users"), F.lit(0)).alias("users"),
                 F.coalesce(F.col("views"), F.lit(0)).alias("views"),
                 F.coalesce(F.col("add_to_carts"), F.lit(0)).alias("add_to_carts"),
                 F.coalesce(F.col("purchase_events"), F.lit(0)).alias("purchase_events"),
                 F.coalesce(F.col("orders"), F.lit(0)).alias("orders"),
                 F.coalesce(F.col("revenue"), F.lit(0.0)).alias("revenue"),
                 F.when(F.coalesce(F.col("clicks"), F.lit(0)) > 0, F.col("cost") / F.col("clicks")).alias("cpc"),
                 F.when(F.coalesce(F.col("impressions"), F.lit(0)) > 0, F.col("clicks") / F.col("impressions")).alias("ctr"),
                 F.when(F.coalesce(F.col("sessions"), F.lit(0)) > 0, F.col("orders") / F.col("sessions")).alias("session_to_order_rate"),
                 F.when(F.coalesce(F.col("views"), F.lit(0)) > 0, F.col("purchase_events") / F.col("views")).alias("view_to_purchase_rate"),
                 F.when(F.coalesce(F.col("cost"), F.lit(0.0)) > 0, F.col("revenue") / F.col("cost")).alias("roas")
             )
    )