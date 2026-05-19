# Databricks notebook source

import yaml
from pathlib import Path

from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    row_number,
    current_timestamp,
    current_date
)

# COMMAND ----------

dbutils.widgets.text("config", "")
dbutils.widgets.text("pit_name", "")

config_arg = dbutils.widgets.get("config")
pit_name = dbutils.widgets.get("pit_name")

# COMMAND ----------

def load_yaml_config(config_arg: str) -> dict:
    config_path = (Path.cwd() / config_arg).resolve()
    print(f"Config path resolvido: {config_path}")

    with open(config_path, "r") as f:
        return yaml.safe_load(f)

# COMMAND ----------

config = load_yaml_config(config_arg)
pit_config = config["gold"]["pits"][pit_name]

source_hub = pit_config["source_hub"]
source_satellite = pit_config["source_satellite"]
target_table = pit_config["target_table"]
hash_key = pit_config["hash_key"]
business_key = pit_config["business_key"]

# COMMAND ----------

df_hub = spark.table(source_hub)
df_sat = spark.table(source_satellite)

# COMMAND ----------

w = (
    Window
    .partitionBy(hash_key)
    .orderBy(col("loadTs").desc())
)

df_sat_latest = (
    df_sat
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

df_pit = (
    df_hub.alias("h")
    .join(
        df_sat_latest.alias("s"),
        on=hash_key,
        how="left"
    )
    .select(
        col(f"h.{hash_key}"),
        col(f"h.{business_key}"),
        *[
            col(f"s.{c}")
            for c in df_sat_latest.columns
            if c not in [hash_key, business_key]
        ]
    )
    .withColumn("_pitLoadTs", current_timestamp())
    .withColumn("_pitLoadDate", current_date())
)

# COMMAND ----------

(
    df_pit.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"PIT criada: {target_table}")

# COMMAND ----------

display(spark.table(target_table).limit(10))