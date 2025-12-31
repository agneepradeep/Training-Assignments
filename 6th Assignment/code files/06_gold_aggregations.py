# Databricks notebook source
# MAGIC %md
# MAGIC ### Read Silver Tables (Once)

# COMMAND ----------

from pyspark.sql.functions import (
    col, to_date, year, month, sum, count, avg, round
)

silver_sales_df = spark.table("workspace.retail.silver_sales")
silver_products_df = spark.table("workspace.retail.silver_products")
silver_stores_df = spark.table("workspace.retail.silver_stores")


# COMMAND ----------

# MAGIC %md
# MAGIC ### GOLD TABLE 1 — Daily Sales Summary (Executive KPIs)

# COMMAND ----------

gold_daily_sales_df = (
    silver_sales_df
    .withColumn("sales_date", to_date(col("transaction_timestamp")))
    .groupBy("sales_date")
    .agg(
        round(sum("total_amount"), 2).alias("total_revenue"),
        sum("quantity").alias("total_quantity"),
        count("transaction_id").alias("total_transactions")
    )
)

(
    gold_daily_sales_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.retail.gold_daily_sales")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### GOLD TABLE 2 — Monthly Sales Trend (Time Intelligence)

# COMMAND ----------

gold_monthly_sales_df = (
    silver_sales_df
    .withColumn("year", year(col("transaction_timestamp")))
    .withColumn("month", month(col("transaction_timestamp")))
    .groupBy("year", "month")
    .agg(
        round(sum("total_amount"), 2).alias("total_revenue"),
        sum("quantity").alias("total_quantity"),
        count("transaction_id").alias("total_transactions")
    )
)

(
    gold_monthly_sales_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.retail.gold_monthly_sales")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### GOLD TABLE 3 — Monthly Revenue by Region

# COMMAND ----------

sales_with_region_df = (
    silver_sales_df
    .join(silver_stores_df, "store_id", "left")
)

gold_monthly_region_sales_df = (
    sales_with_region_df
    .withColumn("year", year(col("transaction_timestamp")))
    .withColumn("month", month(col("transaction_timestamp")))
    .groupBy("year", "month", "region", "country")
    .agg(
        round(sum("total_amount"), 2).alias("total_revenue"),
        sum("quantity").alias("total_quantity")
    )
)

(
    gold_monthly_region_sales_df.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable("workspace.retail.gold_monthly_region_sales")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### GOLD TABLE 4 — Product Performance (Core Business Table)

# COMMAND ----------

sales_with_products_df = (
    silver_sales_df
    .join(silver_products_df, "product_id", "left")
)

gold_product_performance_df = (
    sales_with_products_df
    .groupBy("product_id", "product_name", "category", "brand")
    .agg(
        sum("quantity").alias("total_quantity"),
        round(sum("total_amount"), 2).alias("total_revenue"),
        round(avg("unit_price"), 2).alias("avg_unit_price")
    )
)

(
    gold_product_performance_df.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable("workspace.retail.gold_product_performance")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### GOLD TABLE 5 — Category Performance (VERY IMPORTANT for Power BI)

# COMMAND ----------

gold_category_performance_df = (
    sales_with_products_df
    .groupBy("category")
    .agg(
        round(sum("total_amount"), 2).alias("total_revenue"),
        sum("quantity").alias("total_quantity"),
        count("transaction_id").alias("total_transactions")
    )
)

(
    gold_category_performance_df.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable("workspace.retail.gold_category_performance")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### GOLD TABLE 6 — Store Performance

# COMMAND ----------

gold_store_performance_df = (
    sales_with_region_df
    .groupBy("store_id", "store_name", "region", "country")
    .agg(
        round(sum("total_amount"), 2).alias("total_revenue"),
        sum("quantity").alias("total_quantity"),
        count("transaction_id").alias("total_transactions")
    )
)

(
    gold_store_performance_df.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable("workspace.retail.gold_store_performance")
)
