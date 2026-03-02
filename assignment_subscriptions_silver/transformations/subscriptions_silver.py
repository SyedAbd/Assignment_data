import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dlt.table(
    name="subscriptions_silver",
    comment="""
        Cleaned subscriptions: deduplicated on subscription_id (latest created_at wins),
        dates cast, overlapping subscriptions and reactivations flagged.
    """
)
@dlt.expect("valid_status", "status IN ('active', 'canceled')")
@dlt.expect("valid_plan", "plan_id IN ('basic', 'pro', 'team')")
@dlt.expect("valid_price", "price > 0")
@dlt.expect("user_id_not_null", "user_id IS NOT NULL")
@dlt.expect_or_drop("valid_start_date", "start_date IS NOT NULL")
@dlt.expect("end_after_start", "end_date IS NULL OR end_date >= start_date")
def subscriptions_silver():
    df = dlt.read("assigment_bronze.subscriptions_bronze")

  
    df = (
        df.withColumn("start_date", F.to_date(F.col("start_date"), "yyyy-MM-dd"))
          .withColumn("end_date",   F.to_date(F.col("end_date"),   "yyyy-MM-dd"))
          .withColumn("created_at", F.to_timestamp(F.col("created_at")))
          .withColumn("price",      F.col("price").cast("double"))
    )

    # ── Deduplicate conflicting subscription_id ───────────────────────────────
    # Same subscription_id with different status is the intentional trap.
    # Strategy: keep most recently created record (latest created_at).
    # This correctly reflects the final known state of the subscription.
    dedup_window = Window.partitionBy("subscription_id").orderBy(
        F.col("created_at").desc()
    )

    df = (
        df.withColumn("_row_num", F.row_number().over(dedup_window))
          .filter(F.col("_row_num") == 1)
          .drop("_row_num")
    )

    # ── Flag overlapping subscriptions ───────────────────────────────────────
    # A subscription overlaps if it starts before the previous one ended
    # for the same user (ordered by start_date).
    overlap_window = Window.partitionBy("user_id").orderBy("start_date")

    df = df.withColumn(
        "_prev_end_date", F.lag("end_date").over(overlap_window)
    ).withColumn(
        "is_overlapping",
        F.when(
            F.col("_prev_end_date").isNotNull() &
            (F.col("start_date") <= F.col("_prev_end_date")),
            True
        ).otherwise(False)
    ).drop("_prev_end_date")

    # ── Flag reactivations ───────────────────────────────────────────────────
    # Reactivation = a user had a canceled subscription and then went active again
    df = df.withColumn(
        "_prev_status", F.lag("status").over(overlap_window)
    ).withColumn(
        "is_reactivation",
        F.when(
            (F.col("_prev_status") == "canceled") & (F.col("status") == "active"),
            True
        ).otherwise(False)
    ).drop("_prev_status")

    return df.select(
        "subscription_id",
        "user_id",
        "plan_id",
        "price",
        "currency",
        "start_date",
        "end_date",
        "status",
        "created_at",
        "is_overlapping",
        "is_reactivation",
        "_ingest_timestamp"
    )

