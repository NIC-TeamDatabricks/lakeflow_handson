# Silver層：テレマティクスデータ変換
# Bronze層からデータを読み取り、型変換・クレンジングを実施してAppendで蓄積

import dlt
from pyspark.sql.functions import col, to_timestamp

@dlt.table(
    name="lakeflow_training.silver_schema.silver_telematics",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_speed", "`検知時車両速度` IS NOT NULL")
def silver_telematics():

    return (
        spark.readStream.table("lakeflow_training.bronze_schema.bronze_telematics")
        .select(
            col("契約ID"),
            to_timestamp(col("検知日時"), "yyyy-MM-dd HH:mm:ss").alias("検知日時"),
            col("緯度").cast("double").alias("緯度"),
            col("経度").cast("double").alias("経度"),
            col("座標に基づく都道府県名"),
            col("座標に基づく市町村名"),
            col("検知時車両速度").cast("double").alias("検知時車両速度"),
            col("検知時運転時間").cast("int").alias("検知時運転時間"),
            col("衝突感知フラグ"),
            col("急停車フラグ"),
            col("急ハンドルフラグ"),
            col("SOS発信フラグ"),
            col("アルコール検知フラグ"),
            col("ingestion_timestamp"),
            col("source_file")
        )
    )