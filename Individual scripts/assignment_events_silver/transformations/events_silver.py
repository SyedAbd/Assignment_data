import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dlt.table(
    name="events_quarantine",
    comment="Corrupted or unparseable event rows excluded from silver"
)
def events_quarantine():
    return (
        dlt.read("assigment_bronze.events_bronze")
        .filter(
            # Corrupted rows: Auto Loader (UC mode) puts bad JSON in _rescued_data
            F.col("_rescued_data").isNotNull() |
            # Missing critical fields
            F.col("event_id").isNull() |
            F.col("event_type").isNull() |
            F.col("timestamp").isNull() |
            # Invalid event_type values
            ~F.col("event_type").isin(
                "signup", "login", "page_view", "purchase",
                "refund", "trial_start", "trial_convert", "cancel"
            )
        )
        .withColumn("_quarantine_reason",
            F.when(F.col("_rescued_data").isNotNull(), F.lit("corrupted_json"))
            .when(F.col("event_id").isNull(), F.lit("null_event_id"))
            .when(F.col("event_type").isNull(), F.lit("null_event_type"))
            .when(F.col("timestamp").isNull(), F.lit("null_timestamp"))
            .otherwise(F.lit("invalid_event_type"))
        )
    )


@dlt.table(
    name="events_silver",
    comment="""
        Cleaned events: deduped, conflict-resolved, timestamps normalized to UTC,
        schema v1/v2 unified, null user_id flagged but retained.
    """
)
# ── Data quality expectations (warn only — quarantine handles hard drops) ────
@dlt.expect("event_id_not_null", "event_id IS NOT NULL")
@dlt.expect("valid_event_type",
    "event_type IN ('signup','login','page_view','purchase','refund','trial_start','trial_convert','cancel')"
)
@dlt.expect("normalized_timestamp_not_null", "_timestamp_utc IS NOT NULL")
@dlt.expect("non_negative_amount", "amount IS NULL OR amount >= 0")
def events_silver():
    df = (
        dlt.read("assigment_bronze.events_bronze")
        # ── Drop corrupted rows (handled in quarantine table above) ──────────
        .filter(F.col("_rescued_data").isNull())
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("event_type").isNotNull())
        .filter(F.col("timestamp").isNotNull())
        .filter(F.col("event_type").isin(
            "signup", "login", "page_view", "purchase",
            "refund", "trial_start", "trial_convert", "cancel"
        ))
    )

    # ── Timestamp normalization ───────────────────────────────────────────────
    # Source has 3 formats: ISO Z, ISO with offset, YYYY-MM-DD HH:MM:SS
    # Try each in order; coalesce picks the first non-null result
    df = df.withColumn("_timestamp_utc",
        F.coalesce(
            # ISO 8601 with Z  e.g. 2024-01-15T10:30:00Z
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            # ISO 8601 with offset  e.g. 2024-01-15T10:30:00+05:30
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            # Plain datetime  e.g. 2024-01-15 10:30:00
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss"),
        )
    )

    # ── Schema evolution: unify v1 and v2 ────────────────────────────────────
    # v1 has no currency/tax fields — fill with nulls so schema is consistent
    if "currency" not in df.columns:
        df = df.withColumn("currency", F.lit(None).cast("string"))
    if "tax" not in df.columns:
        df = df.withColumn("tax", F.lit(None).cast("double"))

    # ── Flag null user_id (retain row but mark it) ───────────────────────────
    # Dropping these would lose revenue/refund events — flag instead
    df = df.withColumn("_user_id_missing", F.col("user_id").isNull())

    # ── Deduplication + conflict resolution ──────────────────────────────────
    # Same event_id can appear multiple times with:
    #   a) identical payload → exact duplicate, safe to drop
    #   b) different payload (e.g. amount/currency) → conflicting duplicate
    #
    # Strategy: for conflicting duplicates, keep the row with the LARGEST
    # amount (conservative revenue recognition — avoids over-counting refunds).
    # Secondary sort by _ingest_timestamp desc as tiebreaker.
    dedup_window = Window.partitionBy("event_id").orderBy(
        F.col("amount").desc_nulls_last(),
        F.col("_ingest_timestamp").desc()
    )

    df = (
        df.withColumn("_row_num", F.row_number().over(dedup_window))
          .filter(F.col("_row_num") == 1)
          .drop("_row_num")
    )

    # ── Add event_date for partitioning downstream ───────────────────────────
    df = df.withColumn("event_date", F.to_date(F.col("_timestamp_utc")))

    return df.select(
        "event_id",
        "user_id",
        "_user_id_missing",
        "event_type",
        "timestamp",           # raw — preserved for audit
        "_timestamp_utc",      # normalized
        "event_date",
        "schema_version",
        "amount",
        "currency",
        "tax",
        "refers_to_event_id",
        "acquisition_channel",
        "_ingest_timestamp"
    )
