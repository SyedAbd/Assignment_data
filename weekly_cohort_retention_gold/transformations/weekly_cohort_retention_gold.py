import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window
@dlt.table(
    name="weekly_cohort_retention",
    comment="""
        Cohort retention by signup week. Week 0 = signup week.
        retention_rate = active_users / cohort_size.
    """
)
def weekly_cohort_retention():
    events = dlt.read("assignment_silver.events_silver").filter(F.col("_user_id_missing") == False)

    # Get each user's signup date and cohort week
    signups = (
        events
        .filter(F.col("event_type") == "signup")
        .groupBy("user_id")
        .agg(F.min("_timestamp_utc").alias("signup_ts"))   # earliest signup if duped
        .withColumn("cohort_week", F.date_trunc("week", F.col("signup_ts")).cast("date"))
    )

    # Get all activity per user per week
    activity = (
        events
        .withColumn("activity_week", F.date_trunc("week", F.col("_timestamp_utc")).cast("date"))
        .select("user_id", "activity_week")
        .distinct()
    )

    # Join: for each user, how many weeks after their cohort week were they active?
    return (
        signups
        .join(activity, on="user_id", how="inner")
        .withColumn("weeks_since_signup",
            (F.datediff(F.col("activity_week"), F.col("cohort_week")) / 7).cast("int")
        )
        .filter(F.col("weeks_since_signup") >= 0)   # exclude pre-signup activity
        .groupBy("cohort_week", "weeks_since_signup")
        .agg(F.countDistinct("user_id").alias("active_users"))
        .join(
            # cohort sizes
            signups.groupBy("cohort_week").agg(F.countDistinct("user_id").alias("cohort_size")),
            on="cohort_week"
        )
        .withColumn("retention_rate",
            F.round(F.col("active_users") / F.col("cohort_size"), 4)
        )
        .orderBy("cohort_week", "weeks_since_signup")
    )

