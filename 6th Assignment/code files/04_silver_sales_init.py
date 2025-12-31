# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

sales_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("transaction_timestamp", TimestampType(), True),
    StructField("store_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("source_system", StringType(), True),
    StructField("source_file", StringType(), True)
])

# COMMAND ----------

empty_df = spark.createDataFrame([], sales_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC # Trusted Silver table

# COMMAND ----------

(
    empty_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.retail.silver_sales")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Quarantine table

# COMMAND ----------

(
    empty_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.retail.silver_sales_quarantine")
)


# COMMAND ----------

print("Silver sales tables initialized")
