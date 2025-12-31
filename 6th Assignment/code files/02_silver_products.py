# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC # Paths

# COMMAND ----------

bronze_products_path = "dbfs:/Volumes/workspace/retail/raw/bronze/products/products_master.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Bronze Products (CSV)

# COMMAND ----------

products_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(bronze_products_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Basic cleansing

# COMMAND ----------

silver_products_df = (
    products_df
    .dropDuplicates(["product_id"])
    .filter(col("product_id").isNotNull())
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Silver Products (overwrite snapshot)

# COMMAND ----------

(
    silver_products_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.retail.silver_products")
)

# COMMAND ----------

print("Silver Products table created successfully")
