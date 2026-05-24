# Databricks notebook source
# Project     : E-Commerce Data Analytics Platform
# Layer       : Bronze — Raw Ingestion + Audit Columns
# Description : Adds standard audit columns to every ingested record before
#               writing to ADLS Gen2 Bronze as Delta (append-only).
#               This notebook is triggered by ADF Databricks Notebook Activity
#               after each Copy Activity completes.
# Author      : Abhishek Kamble
# Note        : Resource names are placeholders. Replace with actual values.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from datetime import datetime

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters (passed from ADF pipeline)

# COMMAND ----------

dbutils.widgets.text("source_system", "orders")          # orders | inventory | crm | product_catalogue
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("bronze_base_path", "abfss://bronze@<storage-account-name>.dfs.core.windows.net/ecommerce/")
dbutils.widgets.text("raw_base_path",    "abfss://raw@<storage-account-name>.dfs.core.windows.net/ecommerce/")

source_system   = dbutils.widgets.get("source_system")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
bronze_path     = dbutils.widgets.get("bronze_base_path") + source_system
raw_path        = dbutils.widgets.get("raw_base_path")    + source_system

ingestion_ts = datetime.utcnow().isoformat()

print(f"Source        : {source_system}")
print(f"Pipeline Run  : {pipeline_run_id}")
print(f"Bronze Path   : {bronze_path}")
print(f"Ingestion TS  : {ingestion_ts}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Read raw files landed by ADF Copy Activity

# COMMAND ----------

df_raw = spark.read.format("parquet").load(raw_path)

print(f"Records read from raw: {df_raw.count()}")
df_raw.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Add audit columns (standard across all sources)

# COMMAND ----------

df_bronze = (
    df_raw
    .withColumn("ingestion_timestamp", F.lit(ingestion_ts).cast("timestamp"))
    .withColumn("source_system",       F.lit(source_system))
    .withColumn("pipeline_run_id",     F.lit(pipeline_run_id))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write to Bronze — Delta, append-only

# COMMAND ----------

(
    df_bronze
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "false")   # Bronze schema must be stable
    .partitionBy("source_system")
    .save(bronze_path)
)

print(f"Bronze write complete for source: {source_system}")
print(f"Records written: {df_bronze.count()}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Log run summary

# COMMAND ----------

run_summary = spark.createDataFrame([{
    "source_system":       source_system,
    "pipeline_run_id":     pipeline_run_id,
    "ingestion_timestamp": ingestion_ts,
    "records_written":     df_bronze.count(),
    "status":              "SUCCESS"
}])

(
    run_summary
    .write
    .format("delta")
    .mode("append")
    .save("abfss://bronze@<storage-account-name>.dfs.core.windows.net/ecommerce/_audit_log/")
)

print("Audit log updated.")
