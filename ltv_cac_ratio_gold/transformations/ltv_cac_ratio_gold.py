import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
@dlt.table(
    name="ltv_cac_ratio",
    comment="""
        LTV:CAC ratio by acquisition channel.
        Healthy benchmark = ratio >= 3.0. NULL ratio = organic/no spend data.
    """
)
def ltv_cac_ratio():
    # Average LTV per channel
    ltv = (
        dlt.read("assignment_gold.ltv_per_user")
        .filter(F.col("acquisition_channel").isNotNull())
        .groupBy("acquisition_channel")
        .agg(
            F.avg("ltv").alias("avg_ltv"),
            F.count("user_id").alias("total_customers"),
            F.sum("ltv").alias("total_ltv")
        )
    )

    # Total spend per channel (all time)
    spend = (
        dlt.read("assignment_silver.marketing_spend_silver")
        .groupBy("channel")
        .agg(F.sum("spend").alias("total_spend"))
    )

    # Total customers acquired per channel (for avg CAC denominator)
    cac = (
        dlt.read("assignment_gold.cac_by_channel")
        .filter(F.col("new_customers") > 0)
        .groupBy("channel")
        .agg(F.sum("new_customers").alias("total_acquired"))
        .join(spend, on="channel", how="left")
        .withColumn("avg_cac",
            F.when(
                F.col("total_acquired") > 0,
                F.round(F.col("total_spend") / F.col("total_acquired"), 2)
            ).otherwise(F.lit(None).cast("double"))
        )
    )

    return (
        ltv
        .join(cac, ltv["acquisition_channel"] == cac["channel"], how="left")
        .withColumn("ltv_cac_ratio",
            F.when(
                F.col("avg_cac").isNotNull() & (F.col("avg_cac") > 0),
                F.round(F.col("avg_ltv") / F.col("avg_cac"), 2)
            ).otherwise(F.lit(None).cast("double"))
        )
        .withColumn("is_healthy",
            F.when(F.col("ltv_cac_ratio") >= 3.0, True)
            .when(F.col("ltv_cac_ratio").isNotNull(), False)
            .otherwise(F.lit(None).cast("boolean"))   # NULL = can't assess (organic)
        )
        .select(
            "acquisition_channel",
            "total_customers",
            "avg_ltv",
            "total_ltv",
            "avg_cac",
            "total_spend",
            "ltv_cac_ratio",
            "is_healthy"
        )
        .orderBy(F.col("ltv_cac_ratio").desc_nulls_last())
    )
