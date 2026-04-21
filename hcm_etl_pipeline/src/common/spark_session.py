from typing import Dict
from pyspark.sql import SparkSession

def get_spark_session(app_name: str, spark_conf: Dict[str, str] = None) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .enableHiveSupport()
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.orc.compression.codec", "snappy")
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
    )
    if spark_conf:
        for key, val in spark_conf.items():
            builder = builder.config(key, str(val))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
