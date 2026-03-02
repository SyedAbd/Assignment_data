import dlt
from pyspark.sql.functions import current_timestamp

@dlt.table(
    name="events_bronze",
    comment="Incremental ingestion of events.ndjson using Auto Loader",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def events_bronze():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/Volumes/workspace/assigment_bronze/assignment_data/")
    )
    
    return df.withColumn("_ingest_timestamp", current_timestamp())