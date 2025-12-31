# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC # Paths

# COMMAND ----------

bronze_stores_path = "dbfs:/Volumes/workspace/retail/raw/bronze/stores/stores.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Bronze Stores (CSV)

# COMMAND ----------

stores_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(bronze_stores_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Basic cleansing

# COMMAND ----------

silver_stores_df = (
    stores_df
    .dropDuplicates(["store_id"])
    .filter(col("store_id").isNotNull())
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Silver Stores (overwrite snapshot)

# COMMAND ----------

(
    silver_stores_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.retail.silver_stores")
)

# COMMAND ----------

print("Silver Stores table created successfully")
