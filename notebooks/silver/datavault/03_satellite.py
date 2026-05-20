# Databricks notebook source

import yaml
from pathlib import Path

from pyspark.sql.functions import (
    sha2,
    concat_ws,
    col,
    current_timestamp,
    current_date
)

# COMMAND ----------

dbutils.widgets.text("config", "")
dbutils.widgets.text("satellite_name", "")

config_arg = dbutils.widgets.get("config")
satellite_name = dbutils.widgets.get("satellite_name")

# COMMAND ----------

def load_yaml_config(config_arg: str) -> dict:
    config_path = (Path.cwd() / config_arg).resolve()

    print(f"Config path resolvido: {config_path}")

    with open(config_path, "r") as f:
        return yaml.safe_load(f)

# COMMAND ----------

config = load_yaml_config(config_arg)

sat_config = config["satellites"][satellite_name]

source_table = sat_config["source_table"]
target_table = sat_config["target_table"]
hash_key = sat_config["hash_key"]
parent_keys = sat_config["parent_keys"]
attributes = sat_config.get("attributes", [])

# COMMAND ----------

df = spark.table(source_table)

# COMMAND ----------

df_sat = (
    df
    .drop(hash_key)
    .withColumn(
        hash_key,
        sha2(
            concat_ws(
                "||",
                *[col(c).cast("string") for c in parent_keys]
            ),
            256
        )
    )
)

# COMMAND ----------

if attributes:
    columns_to_select = [hash_key] + attributes
else:
    columns_to_select = [
        hash_key
    ] + [
        c for c in df_sat.columns
        if c not in parent_keys
        and c != hash_key
    ]

columns_to_select = list(dict.fromkeys(columns_to_select))

# COMMAND ----------

df_sat = (
    df_sat
    .select(*columns_to_select)
    .dropDuplicates()
    .withColumn("loadTs", current_timestamp())
    .withColumn("loadDate", current_date())
)

display(df_sat.limit(10))

# COMMAND ----------

(
    df_sat.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Satellite criado: {target_table}")