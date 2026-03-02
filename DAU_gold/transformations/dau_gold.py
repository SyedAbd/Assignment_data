import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dlt.table(
    name="daily_active_users",
    comment="DAU: distinct active users per day, null user_id excluded"
)
def daily_active_users():
    return (
        dlt.read("assignment_silver.events_silver")
        .filter(F.col("_user_id_missing") == False)
        .groupBy("event_date")
        .agg(
            F.countDistinct("user_id").alias("dau"),
            F.count("event_id").alias("total_events")
        )
        .orderBy("event_date")
    )
