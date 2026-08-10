# Gold層：テレマティクスデータ集計
# Silver層のテレマティクスデータとマスタを結合し、契約者毎・都道府県毎に集計

import dlt
from pyspark.sql.functions import col, count, avg, when, sum as _sum

# ① 契約者毎の集計
@dlt.materialized_view(
    name="lakeflow_training.gold_schema.gold_telematics_by_contractor",
    comment="Gold層: 契約者毎の衝突検知・運転時間・速度の集計",
    table_properties={"quality": "gold"}
)
def gold_telematics_by_contractor():
    """
    契約者毎に以下を集計：
    - 衝突検知フラグのカウント
    - 平均運転時間
    - 平均速度
    """
    # テーブル読み込み
    telematics = spark.read.table("lakeflow_training.silver_schema.silver_telematics")
    contract = spark.read.table("lakeflow_training.silver_schema.contract_master")
    customer = spark.read.table("lakeflow_training.silver_schema.customer_master")
    
    # 結合：テレマティクス → 契約 → 顧客（緯度経度は除外）
    joined = (
        telematics
        .join(contract, "契約ID", "inner")
        .join(customer, "契約者ID", "inner")
        .select(
            col("契約者ID"),
            col("契約者氏名"),
            col("契約者都道府県名"),
            col("契約者市町村名"),
            col("契約部門"),
            col("契約車両名"),
            col("契約車両メーカー"),
            col("衝突感知フラグ"),
            col("検知時運転時間"),
            col("検知時車両速度")
        )
    )
    
    # 契約者毎に集計
    return (
        joined
        .groupBy(
            "契約者ID",
            "契約者氏名",
            "契約者都道府県名",
            "契約者市町村名",
            "契約部門"
        )
        .agg(
            _sum(when(col("衝突感知フラグ") == "1", 1).otherwise(0)).alias("衝突検知回数"),
            avg("検知時運転時間").alias("平均運転時間"),
            avg("検知時車両速度").alias("平均速度"),
            count("*").alias("総レコード数")
        )
        .orderBy(col("衝突検知回数").desc())
    )

# ② 都道府県ごとの集計
@dlt.materialized_view(
    name="lakeflow_training.gold_schema.gold_telematics_by_prefecture",
    comment="Gold層: 都道府県ごとの衝突検知・運転時間・速度の集計",
    table_properties={"quality": "gold"}
)
def gold_telematics_by_prefecture():
    """
    都道府県ごとに以下を集計：
    - 衝突検知フラグのカウント
    - 平均運転時間
    - 平均速度
    """
    # テーブル読み込み
    telematics = spark.read.table("lakeflow_training.silver_schema.silver_telematics")
    
    # 都道府県ごとに集計（座標に基づく都道府県名を使用）
    return (
        telematics
        .groupBy("座標に基づく都道府県名")
        .agg(
            _sum(when(col("衝突感知フラグ") == "1", 1).otherwise(0)).alias("衝突検知回数"),
            avg("検知時運転時間").alias("平均運転時間"),
            avg("検知時車両速度").alias("平均速度"),
            count("*").alias("総レコード数")
        )
        .orderBy(col("衝突検知回数").desc())
    )