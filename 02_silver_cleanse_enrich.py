# Databricks notebook source
# Project     : E-Commerce Data Analytics Platform
# Layer       : Silver — Cleanse, Deduplicate, Enrich
# Description : Reads Bronze Delta, applies deduplication, standardisation,
#               schema validation, product catalogue enrichment, and writes
#               to ADLS Gen2 Silver partitioned by load_date.
# Author      : Abhishek Kamble
# Note        : Resource names are placeholders. Replace with actual values.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from delta.tables import DeltaTable
from datetime import date

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("bronze_path", "abfss://bronze@<storage-account-name>.dfs.core.windows.net/ecommerce/orders/")
dbutils.widgets.text("silver_path", "abfss://silver@<storage-account-name>.dfs.core.windows.net/ecommerce/orders/")
dbutils.widgets.text("product_cat_path", "abfss://silver@<storage-account-name>.dfs.core.windows.net/ecommerce/product_catalogue/")
dbutils.widgets.text("quarantine_path",  "abfss://silver@<storage-account-name>.dfs.core.windows.net/ecommerce/_quarantine/orders/")
dbutils.widgets.text("load_date", str(date.today()))

bronze_path      = dbutils.widgets.get("bronze_path")
silver_path      = dbutils.widgets.get("silver_path")
product_cat_path = dbutils.widgets.get("product_cat_path")
quarantine_path  = dbutils.widgets.get("quarantine_path")
load_date        = dbutils.widgets.get("load_date")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Read Bronze

# COMMAND ----------

df_bronze = spark.read.format("delta").load(bronze_path)
print(f"Bronze records: {df_bronze.count()}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Schema Validation — route bad records to quarantine

# COMMAND ----------

mandatory_cols = ["order_id", "customer_id", "order_date", "product_id", "quantity", "unit_price"]

# Flag records missing any mandatory field
condition_valid = " AND ".join([f"{col} IS NOT NULL" for col in mandatory_cols])

df_valid     = df_bronze.filter(condition_valid)
df_quarantine = df_bronze.filter(f"NOT ({condition_valid})")

invalid_count = df_quarantine.count()
print(f"Valid records   : {df_valid.count()}")
print(f"Quarantine rows : {invalid_count}")

if invalid_count > 0:
    df_quarantine.withColumn("quarantine_reason", F.lit("missing_mandatory_field")) \
                 .withColumn("quarantine_ts", F.current_timestamp()) \
                 .write.format("delta").mode("append").save(quarantine_path)
    # In production: trigger ADF alert / Teams notification here
    print("WARNING: Quarantine records written. Alert triggered.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Deduplication — ROW_NUMBER() on business key

# COMMAND ----------

# Orders API sends duplicate events — keep the latest per order_id
window_spec = Window.partitionBy("order_id").orderBy(F.col("order_updated_at").desc())

df_deduped = (
    df_valid
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

print(f"After dedup: {df_deduped.count()}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Standardisation — dates, currency, nulls

# COMMAND ----------

df_standardised = (
    df_deduped
    # Standardise date formats to yyyy-MM-dd
    .withColumn("order_date",    F.to_date(F.col("order_date"),    "yyyy-MM-dd"))
    .withColumn("delivery_date", F.to_date(F.col("delivery_date"), "yyyy-MM-dd"))
    # Currency: convert all to USD (example: GBP source)
    .withColumn("unit_price_usd", F.col("unit_price") * F.lit(1.27))
    # Null handling
    .withColumn("discount",       F.coalesce(F.col("discount"),       F.lit(0.0)))
    .withColumn("order_status",   F.coalesce(F.col("order_status"),   F.lit("UNKNOWN")))
    # Add load_date partition column
    .withColumn("load_date", F.lit(load_date))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Product Catalogue Enrichment
# MAGIC Join at Silver so Power BI never computes this join at query time

# COMMAND ----------

df_product_cat = spark.read.format("delta").load(product_cat_path) \
                      .select("product_id", "category", "sub_category", "unit_price_standard") \
                      .dropDuplicates(["product_id"])

df_enriched = df_standardised.join(df_product_cat, on="product_id", how="left")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Write to Silver — Delta, partitioned by load_date

# COMMAND ----------

(
    df_enriched
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"load_date = '{load_date}'")
    .partitionBy("load_date")
    .save(silver_path)
)

print(f"Silver write complete. load_date={load_date}, records={df_enriched.count()}")
