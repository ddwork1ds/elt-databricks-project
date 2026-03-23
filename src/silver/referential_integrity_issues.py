from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="referential_integrity_issues")
def referential_integrity_issues():

    orders = spark.read.table("silver_orders_dev").alias("o")
    customers = spark.read.table("customers_dev").alias("c")
    order_items = spark.read.table("silver_order_items_dev").alias("oi")
    products = spark.read.table("silver_products_dev").alias("p")
    sellers = spark.read.table("silver_sellers_dev").alias("s")
    payments = spark.read.table("silver_order_payments_dev").alias("op")
    reviews = spark.read.table("silver_order_reviews_dev").alias("r")
    web = spark.read.table("silver_web_events_dev").alias("w")

    issues_orders_customers = (
        orders.join(customers, F.col("o.customer_id") == F.col("c.customer_id"), "left")
              .filter(F.col("c.customer_id").isNull())
              .select(
                  F.lit("orders_to_customers").alias("check_name"),
                  F.lit("silver_orders").alias("source_table"),
                  F.col("o.order_id").alias("record_key"),
                  F.col("o.customer_id").alias("missing_reference_key"),
                  F.lit("customer_id_not_found").alias("issue_type")
              )
    )

    issues_items_orders = (
        order_items.join(orders, F.col("oi.order_id") == F.col("o.order_id"), "left")
                   .filter(F.col("o.order_id").isNull())
                   .select(
                       F.lit("order_items_to_orders").alias("check_name"),
                       F.lit("silver_order_items").alias("source_table"),
                       F.concat_ws("-", F.col("oi.order_id"), F.col("oi.order_item_id")).alias("record_key"),
                       F.col("oi.order_id").alias("missing_reference_key"),
                       F.lit("order_id_not_found").alias("issue_type")
                   )
    )

    issues_items_products = (
        order_items.join(products, F.col("oi.product_id") == F.col("p.product_id"), "left")
                   .filter(F.col("p.product_id").isNull())
                   .select(
                       F.lit("order_items_to_products").alias("check_name"),
                       F.lit("silver_order_items").alias("source_table"),
                       F.concat_ws("-", F.col("oi.order_id"), F.col("oi.order_item_id")).alias("record_key"),
                       F.col("oi.product_id").alias("missing_reference_key"),
                       F.lit("product_id_not_found").alias("issue_type")
                   )
    )

    issues_items_sellers = (
        order_items.join(sellers, F.col("oi.seller_id") == F.col("s.seller_id"), "left")
                   .filter(F.col("s.seller_id").isNull())
                   .select(
                       F.lit("order_items_to_sellers").alias("check_name"),
                       F.lit("silver_order_items").alias("source_table"),
                       F.concat_ws("-", F.col("oi.order_id"), F.col("oi.order_item_id")).alias("record_key"),
                       F.col("oi.seller_id").alias("missing_reference_key"),
                       F.lit("seller_id_not_found").alias("issue_type")
                   )
    )

    issues_payments_orders = (
        payments.join(orders, F.col("op.order_id") == F.col("o.order_id"), "left")
                .filter(F.col("o.order_id").isNull())
                .select(
                    F.lit("payments_to_orders").alias("check_name"),
                    F.lit("silver_order_payments").alias("source_table"),
                    F.concat_ws("-", F.col("op.order_id"), F.col("op.payment_sequential")).alias("record_key"),
                    F.col("op.order_id").alias("missing_reference_key"),
                    F.lit("order_id_not_found").alias("issue_type")
                )
    )

    issues_reviews_orders = (
        reviews.join(orders, F.col("r.order_id") == F.col("o.order_id"), "left")
               .filter(F.col("o.order_id").isNull())
               .select(
                   F.lit("reviews_to_orders").alias("check_name"),
                   F.lit("silver_order_reviews").alias("source_table"),
                   F.col("r.review_id").alias("record_key"),
                   F.col("r.order_id").alias("missing_reference_key"),
                   F.lit("order_id_not_found").alias("issue_type")
               )
    )

    issues_web_orders = (
        web.filter(F.col("w.order_id").isNotNull())
           .join(orders, F.col("w.order_id") == F.col("o.order_id"), "left")
           .filter(F.col("o.order_id").isNull())
           .select(
               F.lit("web_events_to_orders").alias("check_name"),
               F.lit("silver_web_events").alias("source_table"),
               F.col("w.event_id").alias("record_key"),
               F.col("w.order_id").alias("missing_reference_key"),
               F.lit("order_id_not_found").alias("issue_type")
           )
    )

    issues_web_products = (
        web.filter(F.col("w.product_id").isNotNull())
           .join(products, F.col("w.product_id") == F.col("p.product_id"), "left")
           .filter(F.col("p.product_id").isNull())
           .select(
               F.lit("web_events_to_products").alias("check_name"),
               F.lit("silver_web_events").alias("source_table"),
               F.col("w.event_id").alias("record_key"),
               F.col("w.product_id").alias("missing_reference_key"),
               F.lit("product_id_not_found").alias("issue_type")
           )
    )

    return (
        issues_orders_customers
        .unionByName(issues_items_orders)
        .unionByName(issues_items_products)
        .unionByName(issues_items_sellers)
        .unionByName(issues_payments_orders)
        .unionByName(issues_reviews_orders)
        .unionByName(issues_web_orders)
        .unionByName(issues_web_products)
        .withColumn("detected_at", F.current_timestamp())
    )