# Databricks notebook source
# MAGIC %md
# MAGIC ## Imports + Logging Utilities (TOP OF NOTEBOOK)

# COMMAND ----------

from pyspark.sql.functions import (
    col, max as spark_max, row_number, round
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import timedelta, datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logging function

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType, LongType
)

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


# COMMAND ----------

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
        error_message if error_message is not None else ""
    )]

    log_df = spark.createDataFrame(log_data, schema=log_schema)

    (
        log_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable("workspace.retail.pipeline_logs")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline Metadata

# COMMAND ----------

pipeline_name = "silver_sales_incremental"
layer = "silver"
start_time = datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC ### TRY–EXCEPT WRAPPER

# COMMAND ----------

try:
    # read bronze delta
    bronze_path = "dbfs:/Volumes/workspace/retail/raw/bronze_delta/sales"
    bronze_df = spark.read.format("delta").load(bronze_path)

    print("Bronze count:", bronze_df.count())
    
    # incremental window logic
    last_ts = (
        spark.table("workspace.retail.silver_sales")
        .select(spark_max("transaction_timestamp"))
        .collect()[0][0]
    )

    if last_ts is None:
        incremental_df = bronze_df
    else:
        incremental_df = bronze_df.filter(
            col("transaction_timestamp") >= last_ts - timedelta(days=2)
        )

    print("Incremental count:", incremental_df.count())

    # Deduplication (Latest Ingestion Wins)
    w = Window.partitionBy("transaction_id") \
              .orderBy(col("ingestion_timestamp").desc())

    dedup_df = (
        incremental_df
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    print("After dedup:", dedup_df.count())

    # FK + Basic Validation
    products_df = spark.table("workspace.retail.silver_products")
    stores_df = spark.table("workspace.retail.silver_stores")

    valid_df = (
        dedup_df
        .join(products_df.select("product_id"), "product_id", "left_semi")
        .join(stores_df.select("store_id"), "store_id", "left_semi")
        .filter(col("quantity") > 0)
    )

    invalid_df = (
        dedup_df
        .join(
            valid_df.select("transaction_id"),
            "transaction_id",
            "left_anti"
        )
    )

    # Calibration (ONLY on valid data)
    calibrated_df = valid_df.withColumn(
        "total_amount",
        round(col("quantity") * col("unit_price") - col("discount"), 2)
    )

    # TYPE NORMALIZATION
    def normalize_silver_types(df):
        return (
            df
            .withColumn("quantity", col("quantity").cast("int"))
            .withColumn("unit_price", col("unit_price").cast("double"))
            .withColumn("discount", col("discount").cast("double"))
            .withColumn("total_amount", col("total_amount").cast("double"))
        )

    calibrated_df = normalize_silver_types(calibrated_df)
    invalid_df = normalize_silver_types(invalid_df)

    # Align Columns EXACTLY to Silver Schema
    silver_cols = spark.table("workspace.retail.silver_sales").columns

    calibrated_aligned_df = calibrated_df.select(*silver_cols)
    invalid_aligned_df = invalid_df.select(*silver_cols)

    #MERGE into Silver Sales (Idempotent)
    silver_tbl = DeltaTable.forName(spark, "workspace.retail.silver_sales")

    (
        silver_tbl.alias("t")
        .merge(
            calibrated_aligned_df.alias("s"),
            "t.transaction_id = s.transaction_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    # Write Quarantine (Append)
    (
        invalid_aligned_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("workspace.retail.silver_sales_quarantine")
    )

    # SUCCESS LOGGING (Phase 8)
    end_time = datetime.now()

    log_pipeline_event(
        pipeline_name,
        layer,
        start_time,
        end_time,
        status="SUCCESS",
        records_processed=calibrated_aligned_df.count(),
        records_rejected=invalid_aligned_df.count()
    )

except Exception as e:
    end_time = datetime.now()

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

    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Show ALL Pipeline Logs (Main Evidence)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pipeline_name,
# MAGIC     layer,
# MAGIC     start_time,
# MAGIC     end_time,
# MAGIC     status,
# MAGIC     records_processed,
# MAGIC     records_rejected,
# MAGIC     error_message
# MAGIC FROM workspace.retail.pipeline_logs
# MAGIC ORDER BY start_time DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Show Only Latest Run (Very Clean Output)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.retail.pipeline_logs
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 1;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Show Only Failed Runs

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.retail.pipeline_logs
# MAGIC WHERE status = 'FAILED'
# MAGIC ORDER BY start_time DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Records Processed vs Rejected (Data Quality Proof)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pipeline_name,
# MAGIC     SUM(records_processed) AS total_processed,
# MAGIC     SUM(records_rejected) AS total_rejected
# MAGIC FROM workspace.retail.pipeline_logs
# MAGIC GROUP BY pipeline_name;
# MAGIC