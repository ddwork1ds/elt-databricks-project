from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="silver_products_enriched")
def silver_products_enriched():

    products = spark.read.table("silver_products").alias("p")
    translation = spark.read.table("silver_category_translation").alias("t")

    translation = translation.drop("ingestion_time", "source_file", "silver_processed_at", "record_source")

    return (
        products.join(
            translation,
            on="product_category_name",
            how="left"
        )
        .withColumn(
            "product_category_name_english",
            F.coalesce(F.col("product_category_name_english"), F.lit("unknown"))
        )
        .withColumn("silver_processed_at", F.current_timestamp())
        .withColumn("record_source", F.lit("silver_products + silver_category_translation"))
    )
