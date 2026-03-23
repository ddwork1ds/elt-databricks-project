from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="dim_date")
def dim_date():

    orders = spark.read.table("workspace.02_silver.silver_orders").select(
        F.col("order_purchase_date").alias("dt")
    )

    web = spark.read.table("workspace.02_silver.silver_web_events").select(
        F.col("event_date").alias("dt")
    )

    ads = spark.read.table("workspace.02_silver.silver_ads_campaigns").select(
        F.col("campaign_date").alias("dt")
    )

    dates = (
        orders.unionByName(web)
              .unionByName(ads)
              .filter(F.col("dt").isNotNull())
              .dropDuplicates(["dt"])
    )

    return (
        dates.select(
            F.date_format("dt", "yyyyMMdd").cast("int").alias("date_key"),
            F.col("dt").alias("full_date"),
            F.year("dt").alias("year"),
            F.quarter("dt").alias("quarter"),
            F.month("dt").alias("month"),
            F.date_format("dt", "MMMM").alias("month_name"),
            F.weekofyear("dt").alias("week_of_year"),
            F.dayofmonth("dt").alias("day_of_month"),
            F.dayofweek("dt").alias("day_of_week"),
            F.date_format("dt", "EEEE").alias("day_name")
        )
    )