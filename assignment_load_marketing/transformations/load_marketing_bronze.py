import dlt
from pyspark.sql.functions import (
    current_timestamp, col, row_number, to_date
)

@dlt.table(
    name="marketing_spend_bronze",
    comment="Raw ingest of marketing_spend.csv from UC Volume",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def marketing_spend_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("pathGlobFilter", "marketing_spend*.csv") 
        .load("/Volumes/workspace/assigment_bronze/assignment_data/") 
        .withColumn("_ingest_timestamp", current_timestamp())
    )
