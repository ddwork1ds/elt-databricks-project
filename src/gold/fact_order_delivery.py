from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="fact_order_logistics")
def fact_order_logistics():

    orders = (
        spark.read.table("workspace.02_silver.silver_orders")
        .alias("o")
    )

    customers = (
        spark.read.table("workspace.03_gold.dim_customer")
        .select(
            "customer_key",
            "customer_id",
            "customer_zip_code_prefix"
        )
        .alias("c")
    )

    geo = (
        spark.read.table("workspace.03_gold.dim_geolocation")
        .select(
            "geolocation_key",
            "zip_code_prefix"
        )
        .alias("g")
    )

    seller_counts = (
        spark.read.table("workspace.02_silver.silver_order_items")
        .groupBy("order_id")
        .agg(
            F.countDistinct("seller_id").alias("seller_count"),
            F.countDistinct("product_id").alias("product_count")
        )
        .alias("sc")
    )

    return (
        orders
        .join(customers, on="customer_id", how="left")
        .join(
            geo,
            F.col("c.customer_zip_code_prefix") == F.col("g.zip_code_prefix"),
            how="left"
        )
        .join(seller_counts, on="order_id", how="left")
        .select(
            F.col("order_id"),
            F.date_format("order_purchase_date", "yyyyMMdd").cast("int").alias("order_date_key"),
            F.col("c.customer_key"),
            F.col("customer_id"),
            F.col("g.geolocation_key").alias("customer_geolocation_key"),
            F.col("order_status"),
            F.col("order_purchase_ts"),
            F.col("order_approved_at"),
            F.col("order_delivered_carrier_ts"),
            F.col("order_delivered_customer_ts"),
            F.col("order_estimated_delivery_ts"),
            F.coalesce(F.col("sc.seller_count"), F.lit(0)).alias("seller_count"),
            F.coalesce(F.col("sc.product_count"), F.lit(0)).alias("product_count"),

            # Purchase -> Approval
            F.when(
                F.col("order_approved_at").isNotNull(),
                F.round(
                    (F.unix_timestamp("order_approved_at") - F.unix_timestamp("order_purchase_ts")) / 3600.0,
                    2
                )
            ).alias("approval_lead_time_hours"),

            # Approval -> Carrier handoff
            F.when(
                F.col("order_approved_at").isNotNull() & F.col("order_delivered_carrier_ts").isNotNull(),
                F.datediff(
                    F.to_date("order_delivered_carrier_ts"),
                    F.to_date("order_approved_at")
                )
            ).alias("handover_lead_time_days"),

            # Carrier handoff -> Customer delivery
            F.when(
                F.col("order_delivered_carrier_ts").isNotNull() & F.col("order_delivered_customer_ts").isNotNull(),
                F.datediff(
                    F.to_date("order_delivered_customer_ts"),
                    F.to_date("order_delivered_carrier_ts")
                )
            ).alias("last_mile_lead_time_days"),

            # Purchase -> Customer delivery
            F.when(
                F.col("order_purchase_ts").isNotNull() & F.col("order_delivered_customer_ts").isNotNull(),
                F.datediff(
                    F.to_date("order_delivered_customer_ts"),
                    F.to_date("order_purchase_ts")
                )
            ).alias("delivery_lead_time_days"),

            # Estimated vs actual delivery
            F.when(
                F.col("order_estimated_delivery_ts").isNotNull() & F.col("order_delivered_customer_ts").isNotNull(),
                F.datediff(
                    F.to_date("order_delivered_customer_ts"),
                    F.to_date("order_estimated_delivery_ts")
                )
            ).alias("delivery_delay_days"),

            # Flags
            F.when(F.col("order_status") == "delivered", F.lit(1)).otherwise(F.lit(0)).alias("is_delivered"),
            F.when(F.col("order_status") == "canceled", F.lit(1)).otherwise(F.lit(0)).alias("is_canceled"),
            F.when(
                F.col("order_delivered_customer_ts").isNull() &
                F.col("order_estimated_delivery_ts").isNotNull() &
                (F.to_date(F.current_timestamp()) > F.to_date("order_estimated_delivery_ts")),
                F.lit(1)
            ).otherwise(F.lit(0)).alias("is_open_late"),

            F.when(
                F.col("order_delivered_customer_ts").isNotNull() &
                F.col("order_estimated_delivery_ts").isNotNull() &
                (
                    F.datediff(
                        F.to_date("order_delivered_customer_ts"),
                        F.to_date("order_estimated_delivery_ts")
                    ) > 0
                ),
                F.lit(1)
            ).otherwise(F.lit(0)).alias("is_late_delivery")
        )
    )