from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(name="silver_products")
def silver_products():

    df = spark.read.table("workspace.01_bronze.bronze_products")

    df = (
        df.select(
            F.trim(F.col("product_id")).alias("product_id"),
            F.trim(F.col("product_category_name")).alias("product_category_name"),
            F.col("product_name_lenght").cast("int").alias("product_name_length"),
            F.col("product_description_lenght").cast("int").alias("product_description_length"),
            F.col("product_photos_qty").cast("int").alias("product_photos_qty"),
            F.col("product_weight_g").cast("double").alias("product_weight_g"),
            F.col("product_length_cm").cast("double").alias("product_length_cm"),
            F.col("product_height_cm").cast("double").alias("product_height_cm"),
            F.col("product_width_cm").cast("double").alias("product_width_cm"),
            F.col("ingestion_time").cast("timestamp").alias("ingestion_time"),
            F.col("source_file").cast("string").alias("source_file")
        )
        .withColumn("product_id", F.when(F.col("product_id") == "", None).otherwise(F.col("product_id")))
        .withColumn("product_category_name", F.when(F.col("product_category_name") == "", None).otherwise(F.col("product_category_name")))
        .filter(F.col("product_id").isNotNull())
        .filter((F.col("product_weight_g").isNull()) | (F.col("product_weight_g") >= 0))
        .filter((F.col("product_length_cm").isNull()) | (F.col("product_length_cm") >= 0))
        .filter((F.col("product_height_cm").isNull()) | (F.col("product_height_cm") >= 0))
        .filter((F.col("product_width_cm").isNull()) | (F.col("product_width_cm") >= 0))
        .filter((F.col("product_photos_qty").isNull()) | (F.col("product_photos_qty") >= 0))
    )

    w = Window.partitionBy("product_id").orderBy(F.col("ingestion_time").desc_nulls_last())

    return (
        df.withColumn("rn", F.row_number().over(w))
          .filter(F.col("rn") == 1)
          .drop("rn")
          .withColumn("silver_processed_at", F.current_timestamp())
          .withColumn("record_source", F.lit("bronze_products"))
    )