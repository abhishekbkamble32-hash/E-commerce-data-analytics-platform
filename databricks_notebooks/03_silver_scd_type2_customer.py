# Databricks notebook source
# Project     : E-Commerce Data Analytics Platform
# Layer       : Silver — SCD Type 2 on dim_customer (Delta MERGE)
# Description : Implements Slowly Changing Dimension Type 2 on the customer
#               dimension. When a customer changes city or loyalty_tier, the
#               old record is expired (end_date set, is_current=False) and a
#               new record is inserted with is_current=True.
# Author      : Abhishek Kamble
# Note        : Resource names are placeholders. Replace with actual values.

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import date

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("silver_customer_path",  "abfss://silver@<storage-account-name>.dfs.core.windows.net/ecommerce/dim_customer/")
dbutils.widgets.text("bronze_crm_path",       "abfss://bronze@<storage-account-name>.dfs.core.windows.net/ecommerce/crm/")
dbutils.widgets.text("load_date", str(date.today()))

silver_customer_path = dbutils.widgets.get("silver_customer_path")
bronze_crm_path      = dbutils.widgets.get("bronze_crm_path")
load_date            = dbutils.widgets.get("load_date")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Load latest CRM snapshot from Bronze (deduplicated)

# COMMAND ----------

from pyspark.sql.window import Window

df_crm_raw = spark.read.format("delta").load(bronze_crm_path)

window_spec = Window.partitionBy("customer_id").orderBy(F.col("ingestion_timestamp").desc())

df_source = (
    df_crm_raw
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
    .select("customer_id", "customer_name", "email", "city", "loyalty_tier", "ingestion_timestamp")
)

print(f"Source records (latest CRM snapshot): {df_source.count()}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Initialise dim_customer if it doesn't exist

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DateType, LongType

dim_schema = StructType([
    StructField("customer_sk",    LongType(),    False),   # surrogate key
    StructField("customer_id",    StringType(),  False),   # natural key
    StructField("customer_name",  StringType(),  True),
    StructField("email",          StringType(),  True),
    StructField("city",           StringType(),  True),
    StructField("loyalty_tier",   StringType(),  True),
    StructField("start_date",     DateType(),    False),
    StructField("end_date",       DateType(),    True),
    StructField("is_current",     BooleanType(), False),
])

try:
    DeltaTable.forPath(spark, silver_customer_path)
    print("dim_customer exists — running MERGE.")
except Exception:
    print("dim_customer does not exist — initialising.")
    df_init = (
        df_source
        .withColumn("customer_sk",  F.monotonically_increasing_id())
        .withColumn("start_date",   F.lit(load_date).cast("date"))
        .withColumn("end_date",     F.lit(None).cast("date"))
        .withColumn("is_current",   F.lit(True))
        .drop("ingestion_timestamp")
    )
    df_init.write.format("delta").mode("overwrite").save(silver_customer_path)
    print(f"dim_customer initialised with {df_init.count()} records.")
    dbutils.notebook.exit("INITIALISED")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. SCD Type 2 MERGE Logic
# MAGIC
# MAGIC Strategy (standard two-pass approach):
# MAGIC - Pass 1: Expire rows in target where business key matches but tracked attributes changed
# MAGIC - Pass 2: Insert new rows for changed/new records

# COMMAND ----------

delta_target = DeltaTable.forPath(spark, silver_customer_path)

# Tracked columns — changes to these trigger a Type 2 new record
tracked_cols = ["city", "loyalty_tier"]

# Build change detection condition
change_condition = " OR ".join([
    f"target.{col} <> source.{col}" for col in tracked_cols
])

# COMMAND ----------
# MAGIC %md
# MAGIC ### Pass 1 — Expire changed current records

# COMMAND ----------

(
    delta_target.alias("target")
    .merge(
        df_source.alias("source"),
        "target.customer_id = source.customer_id AND target.is_current = true"
    )
    .whenMatchedUpdate(
        condition=change_condition,
        set={
            "is_current": F.lit(False),
            "end_date":   F.lit(load_date).cast("date")
        }
    )
    .execute()
)

print("Pass 1 complete — expired changed records.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Pass 2 — Insert new records for changed + genuinely new customers

# COMMAND ----------

# Re-read target to see updated state
df_current_target = delta_target.toDF().filter(F.col("is_current") == True)

# New records = source rows where customer_id has no current match in target
#             + source rows where attributes changed (those were just expired above)
df_changed_or_new = (
    df_source.alias("src")
    .join(
        df_current_target.alias("tgt"),
        on="customer_id",
        how="left"
    )
    .filter(
        # Genuinely new customer
        F.col("tgt.customer_id").isNull()
        # OR attribute changed — target no longer has a current row for this customer
    )
    .select("src.*")
)

# Also catch customers whose current row was just expired in Pass 1
df_expired_customers = (
    delta_target.toDF()
    .filter((F.col("is_current") == False) & (F.col("end_date") == load_date))
    .select("customer_id")
)

df_to_insert = (
    df_source
    .join(df_expired_customers, on="customer_id", how="inner")
    .union(df_changed_or_new)
    .dropDuplicates(["customer_id"])
    .withColumn("customer_sk", F.monotonically_increasing_id())
    .withColumn("start_date",  F.lit(load_date).cast("date"))
    .withColumn("end_date",    F.lit(None).cast("date"))
    .withColumn("is_current",  F.lit(True))
    .drop("ingestion_timestamp")
)

if df_to_insert.count() > 0:
    df_to_insert.write.format("delta").mode("append").save(silver_customer_path)
    print(f"Pass 2 complete — inserted {df_to_insert.count()} new/changed records.")
else:
    print("Pass 2 — no new or changed records to insert.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Verify

# COMMAND ----------

df_final = delta_target.toDF()
print(f"Total dim_customer records : {df_final.count()}")
print(f"Current records            : {df_final.filter(F.col('is_current')==True).count()}")
print(f"Historical records         : {df_final.filter(F.col('is_current')==False).count()}")
