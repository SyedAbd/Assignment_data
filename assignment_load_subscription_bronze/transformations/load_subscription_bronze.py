import dlt
from pyspark.sql.functions import (
    current_timestamp, col, count, row_number, to_date, to_timestamp
)

@dlt.table(
    name="subscriptions_bronze",
    comment="Raw ingest of subscriptions.json from UC Volume",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def subscriptions_bronze():
    return (
        spark.read
        .format("json")
        .option("multiline", "true")  
        .load("/Volumes/workspace/assigment_bronze/assignment_data/subscriptions.json")
        .withColumn("_ingest_timestamp", current_timestamp())
    )