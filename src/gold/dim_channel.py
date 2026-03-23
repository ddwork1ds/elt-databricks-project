from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="dim_channel")
def dim_channel():
    mapping = spark.read.table("workspace.02_silver.bridge_campaign_mapping")

    return (
        mapping.select(
            F.pmod(
                F.xxhash64(
                    F.coalesce(F.col("channel"), F.lit("unknown")),
                    F.coalesce(F.col("source"), F.lit("unknown")),
                    F.coalesce(F.col("medium"), F.lit("unknown"))
                ),
                F.lit(9223372036854775807)
            ).alias("channel_key"),
            F.coalesce(F.col("channel"), F.lit("unknown")).alias("channel_name"),
            F.coalesce(F.col("source"), F.lit("unknown")).alias("source"),
            F.coalesce(F.col("medium"), F.lit("unknown")).alias("medium")
        )
        .dropDuplicates(["channel_name", "source", "medium"])
    )