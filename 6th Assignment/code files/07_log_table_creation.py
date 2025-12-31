# Databricks notebook source
from pyspark.sql.types import *

log_schema = StructType([
    StructField("pipeline_name", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("records_processed", LongType(), True),
    StructField("records_rejected", LongType(), True),
    StructField("error_message", StringType(), True)
])

empty_log_df = spark.createDataFrame([], log_schema)

(
    empty_log_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.retail.pipeline_logs")
)
