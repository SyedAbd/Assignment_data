import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
@dlt.table(
    name="cac_by_channel",
    comment="""
        Daily CAC per channel = spend / new signups attributed to that channel.
        NULL CAC when spend=0 (organic/missing) or conversions=0.
    """
)
def cac_by_channel():
    # Signups with channel attribution from events
    signups = (
        dlt.read("assignment_silver.events_silver")
        .filter(
            (F.col("event_type") == "signup") &
            F.col("acquisition_channel").isNotNull() &
            (F.col("_user_id_missing") == False)
        )
        .groupBy("event_date", "acquisition_channel")
        .agg(F.countDistinct("user_id").alias("new_customers"))
    )

    # Marketing spend per channel per day
    spend = (
        dlt.read("assignment_silver.marketing_spend_silver")
        .groupBy("date", "channel")
        .agg(F.sum("spend").alias("total_spend"))
    )

    return (
        spend
        .join(signups,
            (spend["date"] == signups["event_date"]) &
            (spend["channel"] == signups["acquisition_channel"]),
            how="full"
        )
        .withColumn("date", F.coalesce(spend["date"], signups["event_date"]))
        .withColumn("channel", F.coalesce(spend["channel"], signups["acquisition_channel"]))
        .withColumn("total_spend",    F.coalesce(F.col("total_spend"),  F.lit(0.0)))
        .withColumn("new_customers",  F.coalesce(F.col("new_customers"), F.lit(0)))
        .withColumn("cac",
            F.when(
                (F.col("total_spend") > 0) & (F.col("new_customers") > 0),
                F.round(F.col("total_spend") / F.col("new_customers"), 2)
            ).otherwise(F.lit(None).cast("double"))
        )
        .select("date", "channel", "total_spend", "new_customers", "cac")
        .orderBy("date", "channel")
    )


