# Bronze層：テレマティクスデータ取込
# Auto LoaderでVolumeからCSVデータを取り込み、全カラムをString型で保存します。

import dlt
from pyspark.sql.functions import current_timestamp, col

@dlt.table(
    name="lakeflow_training.bronze_schema.bronze_telematics",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_telematics_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "false")  # String型で取込
        .option("header", "true")
        .option("cloudFiles.schemaLocation", "/Volumes/lakeflow_training/bronze_schema/source_data/_schema")
        .load("/Volumes/lakeflow_training/bronze_schema/source_data/")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )