from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(name="silver_order_reviews")
def silver_order_reviews():

    df = spark.read.table("workspace.01_bronze.bronze_reviews")

    df = (
        df.select(
            F.trim(F.col("review_id")).alias("review_id"),
            F.trim(F.col("order_id")).alias("order_id"),
            F.col("review_score").cast("int").alias("review_score"),
            F.trim(F.col("review_comment_title")).alias("review_comment_title"),
            F.trim(F.col("review_comment_message")).alias("review_comment_message"),
            F.to_timestamp("review_creation_date").alias("review_creation_ts"),
            F.to_timestamp("review_answer_timestamp").alias("review_answer_ts"),
            F.col("ingestion_time").cast("timestamp").alias("ingestion_time"),
            F.col("source_file").cast("string").alias("source_file")
        )
        .withColumn("review_id", F.when(F.col("review_id") == "", None).otherwise(F.col("review_id")))
        .withColumn("order_id", F.when(F.col("order_id") == "", None).otherwise(F.col("order_id")))
        .withColumn("review_comment_title", F.when(F.col("review_comment_title") == "", None).otherwise(F.col("review_comment_title")))
        .withColumn("review_comment_message", F.when(F.col("review_comment_message") == "", None).otherwise(F.col("review_comment_message")))
        .filter(F.col("review_id").isNotNull())
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("review_score").isNotNull())
        .filter((F.col("review_score") >= 1) & (F.col("review_score") <= 5))
    )

    w = Window.partitionBy("review_id").orderBy(F.col("ingestion_time").desc_nulls_last())

    return (
        df.withColumn("rn", F.row_number().over(w))
          .filter(F.col("rn") == 1)
          .drop("rn")
          .withColumn("silver_processed_at", F.current_timestamp())
          .withColumn("record_source", F.lit("bronze_order_reviews"))
    )
