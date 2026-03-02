import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
@dlt.table(
    name="daily_revenue_gross",
    comment="Gross daily revenue from purchases only, before refund adjustment"
)
def daily_revenue_gross():
    return (
        dlt.read("assignment_silver.events_silver")
        .filter(
            (F.col("event_type") == "purchase") &
            F.col("amount").isNotNull() &
            (F.col("amount") > 0)    # exclude negative amount rows
        )
        .groupBy("event_date")
        .agg(
            F.sum("amount").alias("gross_revenue"),
            F.count("event_id").alias("purchase_count"),
            F.countDistinct("user_id").alias("paying_users")
        )
        .orderBy("event_date")
    )