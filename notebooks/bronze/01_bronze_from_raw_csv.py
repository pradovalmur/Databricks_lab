# Databricks notebook source

import yaml
from pathlib import Path

from pyspark.sql.functions import (
    current_timestamp,
    current_date,
    input_file_name,
    lit
)

# COMMAND ----------

dbutils.widgets.text("config", "")
dbutils.widgets.text("source", "")

config_arg = dbutils.widgets.get("config")
source_name = dbutils.widgets.get("source")

print(f"Config: {config_arg}")
print(f"Source: {source_name}")

# COMMAND ----------

notebook_dir = Path.cwd()
config_path = (notebook_dir / config_arg).resolve()

print(f"Config path resolvido: {config_path}")

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

source = config["sources"][source_name]

# COMMAND ----------

catalog = source["catalog"]
schema = source["schema"]
volume = source["volume"]

file_name = source["file_name"]
delimiter = source.get("delimiter", ",")
header = str(source.get("header", True)).lower()

bronze_table = source.get("bronze_table", source_name)

raw_path = f"/Volumes/{catalog}/{schema}/{volume}/{file_name}"
target_table = f"{catalog}.{schema}.{bronze_table}"

print(f"Raw path: {raw_path}")
print(f"Target table: {target_table}")

# COMMAND ----------

df_raw = (
    spark.read
    .option("header", header)
    .option("delimiter", delimiter)
    .option("inferSchema", "true")
    .csv(raw_path)
)

display(df_raw.limit(10))

# COMMAND ----------

df_bronze = (
    df_raw
    .withColumn("_source_name", lit(source_name))
    .withColumn("_source_file", input_file_name())
    .withColumn("_raw_file_name", lit(file_name))
    .withColumn("_ingestion_ts", current_timestamp())
    .withColumn("_ingestion_date", current_date())
)

display(df_bronze.limit(10))

# COMMAND ----------

(
    df_bronze.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(target_table)
)

print(f"Dados carregados na Bronze: {target_table}")

# COMMAND ----------

spark.sql(f"""
SELECT 
  COUNT(*) AS total_rows,
  COUNT(DISTINCT _raw_file_name) AS total_files
FROM {target_table}
""").display()