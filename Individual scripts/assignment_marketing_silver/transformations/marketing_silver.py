
import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dlt.table(
    name="marketing_spend_quarantine",
    comment="Invalid marketing spend rows: negative spend, nulls"
)
def marketing_spend_quarantine():
    df = dlt.read("assigment_bronze.marketing_spend_bronze")
    df = df.withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))

    return (
        df.filter(
            F.col("date").isNull() |
            F.col("channel").isNull() |
            (F.col("channel") == "") |
            F.col("spend").isNull() |
            (F.col("spend") < 0)
        )
        .withColumn("_quarantine_reason",
            F.when(F.col("date").isNull(),    F.lit("missing_date"))
            .when(F.col("channel").isNull() | (F.col("channel") == ""), F.lit("missing_channel"))
            .when(F.col("spend").isNull(),    F.lit("null_spend"))
            .when(F.col("spend") < 0,         F.lit("negative_spend"))
            .otherwise(F.lit("unknown"))
        )
    )


@dlt.table(
    name="marketing_spend_silver",
    comment="""
        Cleaned marketing spend: exact duplicates removed, negative spend dropped,
        missing date gaps flagged per channel.
    """
)
@dlt.expect_or_drop("valid_date",    "date IS NOT NULL")
@dlt.expect_or_drop("valid_channel", "channel IS NOT NULL AND channel != ''")
@dlt.expect_or_drop("non_negative_spend", "spend >= 0")
@dlt.expect("spend_not_null", "spend IS NOT NULL")
def marketing_spend_silver():
    df = dlt.read("assigment_bronze.marketing_spend_bronze")

    # ── Cast date ────────────────────────────────────────────────────────────
    df = df.withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))

    # ── Drop invalid rows ────────────────────────────────────────────────────
    df = df.filter(
        F.col("date").isNotNull() &
        F.col("channel").isNotNull() &
        (F.col("channel") != "") &
        F.col("spend").isNotNull() &
        (F.col("spend") >= 0)
    )

    # ── Deduplicate exact duplicate rows ─────────────────────────────────────
    # A duplicate is defined as same (date, channel, spend) — all three must match.
    # Using row_number so we keep exactly one copy per duplicate group.
    dedup_window = Window.partitionBy("date", "channel", "spend").orderBy(
        F.col("_ingest_timestamp").asc()
    )

    df = (
        df.withColumn("_row_num", F.row_number().over(dedup_window))
          .filter(F.col("_row_num") == 1)
          .drop("_row_num")
    )

    # ── Flag missing date gaps per channel ───────────────────────────────────
    # We can't generate the missing rows in silver (that's a gold concern),
    # but we flag the previous date gap so downstream knows the series is incomplete.
    gap_window = Window.partitionBy("channel").orderBy("date")

    df = df.withColumn(
        "_prev_date", F.lag("date").over(gap_window)
    ).withColumn(
        "_days_since_prev",
        F.datediff(F.col("date"), F.col("_prev_date"))
    ).withColumn(
        "has_gap_before",
        F.when(
            F.col("_days_since_prev").isNotNull() & (F.col("_days_since_prev") > 1),
            True
        ).otherwise(False)
    ).drop("_prev_date", "_days_since_prev")

    return df.select(
        "date",
        "channel",
        "spend",
        "has_gap_before",   # True if there are missing days before this record
        "_ingest_timestamp"
    )
