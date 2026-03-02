import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
@dlt.table(
    name="mrr_daily",
    comment="""
        Daily MRR snapshot: sum of active subscription monthly prices per day.
        Use last day of month to get monthly MRR. Overlapping subs both counted.
    """
)
def mrr_daily():
    subs = dlt.read("assignment_silver.subscriptions_silver").filter(F.col("status") == "active")

    # Build date spine from earliest subscription start to today
    date_spine = spark.sql("""
        SELECT explode(sequence(
            DATE '2020-01-01',
            CURRENT_DATE(),
            INTERVAL 1 DAY
        )) AS spine_date
    """)

    # For each day in the spine, find all active subscriptions
    return (
        date_spine
        .join(subs,
            (subs["start_date"] <= F.col("spine_date")) &
            (
                subs["end_date"].isNull() |
                (subs["end_date"] >= F.col("spine_date"))
            ),
            how="left"
        )
        .groupBy("spine_date")
        .agg(
            F.sum("price").alias("mrr"),
            F.countDistinct("subscription_id").alias("active_subscriptions"),
            F.countDistinct("user_id").alias("subscribed_users")
        )
        .withColumnRenamed("spine_date", "date")
        .orderBy("date")
    )

