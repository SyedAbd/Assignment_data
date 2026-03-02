import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
@dlt.table(
    name="daily_revenue_net",
    comment="Net daily revenue = gross purchases minus refunds, refund-date attributed"
)
def daily_revenue_net():
    events = dlt.read("assignment_silver.events_silver")

    purchases = (
        events
        .filter((F.col("event_type") == "purchase") & (F.col("amount") > 0))
        .groupBy("event_date")
        .agg(F.sum("amount").alias("gross_revenue"))
    )

    refunds = (
        events
        .filter(F.col("event_type") == "refund")
        .withColumn("refund_amount",
            # Refund amounts may be stored as positive or negative in source
            # Normalise to positive so we can subtract cleanly
            F.abs(F.coalesce(F.col("amount"), F.lit(0)))
        )
        .groupBy("event_date")
        .agg(F.sum("refund_amount").alias("total_refunds"))
    )

    return (
        purchases
        .join(refunds, on="event_date", how="left")
        .withColumn("total_refunds", F.coalesce(F.col("total_refunds"), F.lit(0)))
        .withColumn("net_revenue", F.col("gross_revenue") - F.col("total_refunds"))
        .orderBy("event_date")
    )

