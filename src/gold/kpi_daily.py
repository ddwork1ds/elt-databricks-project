from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="mart_kpi_daily")
def mart_kpi_daily():

    base = spark.read.table("workspace.03_gold.mart_sales_marketing_360").alias("m")

    return (
        base
        .select(
            # grain
            F.col("date_key"),
            F.col("campaign_key"),
            F.col("campaign_id"),
            F.col("channel"),

            # core raw metrics for BI
            F.coalesce(F.col("impressions"), F.lit(0)).cast("bigint").alias("impressions"),
            F.coalesce(F.col("clicks"), F.lit(0)).cast("bigint").alias("clicks"),
            F.coalesce(F.col("cost"), F.lit(0.0)).cast("double").alias("ad_spend"),
            F.coalesce(F.col("sessions"), F.lit(0)).cast("bigint").alias("sessions"),
            F.coalesce(F.col("add_to_carts"), F.lit(0)).cast("bigint").alias("add_to_carts"),
            F.coalesce(F.col("purchase_events"), F.lit(0)).cast("bigint").alias("purchase_events"),
            F.coalesce(F.col("orders"), F.lit(0)).cast("bigint").alias("orders"),
            F.coalesce(F.col("revenue"), F.lit(0.0)).cast("double").alias("revenue"),

            # marketing KPIs
            F.round(
                F.when(F.col("impressions") > 0, F.col("clicks") / F.col("impressions")).otherwise(F.lit(0.0)),
                4
            ).alias("ctr"),

            F.round(
                F.when(F.col("clicks") > 0, F.col("cost") / F.col("clicks")).otherwise(F.lit(0.0)),
                2
            ).alias("cpc"),

            F.round(
                F.when(F.col("impressions") > 0, (F.col("cost") / F.col("impressions")) * 1000).otherwise(F.lit(0.0)),
                2
            ).alias("cpm"),

            # funnel KPIs
            F.round(
                F.when(F.col("sessions") > 0, F.col("add_to_carts") / F.col("sessions")).otherwise(F.lit(0.0)),
                4
            ).alias("session_to_cart_rate"),

            F.round(
                F.when(F.col("add_to_carts") > 0, F.col("purchase_events") / F.col("add_to_carts")).otherwise(F.lit(0.0)),
                4
            ).alias("cart_to_purchase_rate"),

            F.round(
                F.when(F.col("sessions") > 0, F.col("purchase_events") / F.col("sessions")).otherwise(F.lit(0.0)),
                4
            ).alias("session_to_purchase_rate"),

            # business KPIs
            F.round(
                F.when(F.col("orders") > 0, F.col("revenue") / F.col("orders")).otherwise(F.lit(0.0)),
                2
            ).alias("aov"),

            F.round(
                F.when(F.col("cost") > 0, F.col("revenue") / F.col("cost")).otherwise(F.lit(0.0)),
                4
            ).alias("roas"),

            F.round(
                F.when(F.col("orders") > 0, F.col("cost") / F.col("orders")).otherwise(F.lit(0.0)),
                2
            ).alias("cost_per_order")
        )
    )