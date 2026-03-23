from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(name="silver_category_translation")
def silver_category_translation():

    df = spark.read.table("workspace.01_bronze.bronze_category_translation")

    df = (
        df.select(
            F.trim(F.col("product_category_name")).alias("product_category_name"),
            F.trim(F.col("product_category_name_english")).alias("product_category_name_english"),
            F.col("ingestion_time").cast("timestamp").alias("ingestion_time"),
            F.col("source_file").cast("string").alias("source_file")
        )
        .withColumn("product_category_name", F.when(F.col("product_category_name") == "", None).otherwise(F.col("product_category_name")))
        .withColumn("product_category_name_english", F.when(F.col("product_category_name_english") == "", None).otherwise(F.col("product_category_name_english")))
        .filter(F.col("product_category_name").isNotNull())
        .filter(F.col("product_category_name_english").isNotNull())
    )

    w = Window.partitionBy("product_category_name").orderBy(F.col("ingestion_time").desc_nulls_last())

    return (
        df.withColumn("rn", F.row_number().over(w))
          .filter(F.col("rn") == 1)
          .drop("rn")
          .withColumn("silver_processed_at", F.current_timestamp())
          .withColumn("record_source", F.lit("bronze_category_translation"))
    )