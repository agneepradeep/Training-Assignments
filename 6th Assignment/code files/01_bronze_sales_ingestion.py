# Databricks notebook source
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)

bronze_sales_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("transaction_timestamp", TimestampType(), True),  
    StructField("store_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", IntegerType(), True),   
    StructField("discount", IntegerType(), True),
    StructField("total_amount", IntegerType(), True),
    StructField("currency", StringType(), True)
])


# COMMAND ----------

# ============================
# BRONZE SALES INGESTION
# ============================

from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType, LongType
)
from datetime import datetime

# ----------------------------
# CONFIG
# ----------------------------
raw_sales_path = "dbfs:/Volumes/workspace/retail/raw/bronze/sales/"
bronze_delta_path = "dbfs:/Volumes/workspace/retail/raw/bronze_delta/sales"
source_system = "retail_sales_csv"

pipeline_name = "bronze_sales_ingestion"
layer = "bronze"
start_time = datetime.now()

# ----------------------------
# LOG SCHEMA (REUSED)
# ----------------------------
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

def log_pipeline_event(
    pipeline_name,
    layer,
    start_time,
    end_time,
    status,
    records_processed,
    records_rejected,
    error_message=None
):
    log_data = [(
        pipeline_name,
        layer,
        start_time,
        end_time,
        status,
        int(records_processed),
        int(records_rejected),
        error_message if error_message else ""
    )]

    log_df = spark.createDataFrame(log_data, schema=log_schema)

    (
        log_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("workspace.retail.pipeline_logs")
    )

# ----------------------------
# INGESTION WITH LOGGING
# ----------------------------
try:
    # 1 Read raw CSV files
    raw_df = (
        spark.read
        .option("header", True)
        .schema(bronze_sales_schema)
        .csv(raw_sales_path)
    )

    # 2 Add ingestion metadata
    bronze_df = (
        raw_df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_system", lit(source_system))
        .withColumn("source_file", col("_metadata.file_path"))
    )

    record_count = bronze_df.count()

    # 3 Write to Bronze Delta (append-safe)
    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .save(bronze_delta_path)
    )

    end_time = datetime.now()

    # 4 SUCCESS LOG
    log_pipeline_event(
        pipeline_name,
        layer,
        start_time,
        end_time,
        status="SUCCESS",
        records_processed=record_count,
        records_rejected=0
    )

    print(f"Bronze ingestion completed successfully. Records ingested: {record_count}")

except Exception as e:
    end_time = datetime.now()

    # 5 FAILURE LOG
    log_pipeline_event(
        pipeline_name,
        layer,
        start_time,
        end_time,
        status="FAILED",
        records_processed=0,
        records_rejected=0,
        error_message=str(e)
    )

    print("Bronze ingestion failed")
    raise


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.retail.pipeline_logs
# MAGIC WHERE layer = 'bronze'
# MAGIC ORDER BY start_time DESC;
# MAGIC