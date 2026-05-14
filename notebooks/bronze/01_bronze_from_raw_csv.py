# Databricks notebook source

import re
import yaml
import unicodedata
from pathlib import Path

from pyspark.sql.functions import (
    current_timestamp,
    current_date,
    lit,
    col
)

# COMMAND ----------

dbutils.widgets.text("config", "")
dbutils.widgets.text("source", "")

config_arg = dbutils.widgets.get("config")
source_name = dbutils.widgets.get("source")

print(f"Config: {config_arg}")
print(f"Source: {source_name}")

# COMMAND ----------

def to_camel_case(column_name: str) -> str:
    normalized = unicodedata.normalize("NFKD", column_name)
    normalized = normalized.encode("ascii", "ignore").decode("utf-8")
    normalized = re.sub(r"[^a-zA-Z0-9 ]", " ", normalized)

    parts = normalized.strip().split()

    if not parts:
        return "column"

    return parts[0].lower() + "".join(part.capitalize() for part in parts[1:])


def normalize_columns(df):
    new_columns = [to_camel_case(c) for c in df.columns]
    return df.toDF(*new_columns)

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

df_raw = normalize_columns(df_raw)

display(df_raw.limit(10))

# COMMAND ----------

df_bronze = (
    df_raw
    .withColumn("_sourceName", lit(source_name))
    .withColumn("_sourceFile", col("_metadata.file_path"))
    .withColumn("_rawFileName", lit(file_name))
    .withColumn("_ingestionTs", current_timestamp())
    .withColumn("_ingestionDate", current_date())
)

display(df_bronze.limit(10))

# COMMAND ----------

if spark.catalog.tableExists(target_table):
    print(f"Tabela existe. Removendo dados antigos do arquivo: {file_name}")

    spark.sql(f"""
        DELETE FROM {target_table}
        WHERE _rawFileName = '{file_name}'
    """)
else:
    print(f"Tabela ainda não existe: {target_table}")

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
  COUNT(*) AS totalRows,
  COUNT(DISTINCT _rawFileName) AS totalFiles
FROM {target_table}
""").display()