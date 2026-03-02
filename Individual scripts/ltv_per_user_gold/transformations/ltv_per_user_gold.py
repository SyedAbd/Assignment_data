import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dlt.table(
    name="ltv_per_user",
    comment="Per-user LTV = total purchases minus total refunds, all time"
)
def ltv_per_user():
    events = dlt.read("assignment_silver.events_silver").filter(F.col("_user_id_missing") == False)

    purchases = (
        events
        .filter((F.col("event_type") == "purchase") & (F.col("amount") > 0))
        .groupBy("user_id")
        .agg(
            F.sum("amount").alias("total_purchases"),
            F.count("event_id").alias("purchase_count")
        )
    )

    refunds = (
        events
        .filter(F.col("event_type") == "refund")
        .groupBy("user_id")
        .agg(F.sum(F.abs("amount")).alias("total_refunds"))
    )

    # User metadata — first seen, last seen, acquisition channel
    user_meta = (
        events
        .groupBy("user_id")
        .agg(
            F.min("_timestamp_utc").alias("first_seen"),
            F.max("_timestamp_utc").alias("last_seen"),
            # acquisition_channel only on signup events — take first non-null
            F.first(
                F.when(F.col("event_type") == "signup", F.col("acquisition_channel")),
                ignorenulls=True
            ).alias("acquisition_channel")
        )
    )

    return (
        user_meta
        .join(purchases, on="user_id", how="left")
        .join(refunds,   on="user_id", how="left")
        .withColumn("total_purchases", F.coalesce(F.col("total_purchases"), F.lit(0.0)))
        .withColumn("total_refunds",   F.coalesce(F.col("total_refunds"),   F.lit(0.0)))
        .withColumn("ltv", F.round(F.col("total_purchases") - F.col("total_refunds"), 2))
        .withColumn("customer_lifetime_days",
            F.datediff(F.col("last_seen"), F.col("first_seen"))
        )
        .select(
            "user_id",
            "acquisition_channel",
            "first_seen",
            "last_seen",
            "customer_lifetime_days",
            "purchase_count",
            "total_purchases",
            "total_refunds",
            "ltv"
        )
        .orderBy(F.col("ltv").desc())
    )


