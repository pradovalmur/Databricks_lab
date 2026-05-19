# Databricks notebook source

import yaml
from pathlib import Path

from pyspark.sql.functions import sha2, concat_ws, col, current_timestamp, current_date

# COMMAND ----------

dbutils.widgets.text("config", "")
dbutils.widgets.text("hub_name", "")

config_arg = dbutils.widgets.get("config")
hub_name = dbutils.widgets.get("hub_name")

# COMMAND ----------

def load_yaml_config(config_arg: str) -> dict:
    config_path = (Path.cwd() / config_arg).resolve()
    print(f"Config path resolvido: {config_path}")

    with open(config_path, "r") as f:
        return yaml.safe_load(f)

# COMMAND ----------

config = load_yaml_config(config_arg)
hub_config = config["hubs"][hub_name]

source_table = hub_config["source_table"]
target_table = hub_config["target_table"]
hash_key = hub_config["hash_key"]
business_keys = hub_config["business_key"]

# COMMAND ----------

df = spark.table(source_table)

df_hub = (
    df
    .select(*business_keys)
    .dropna()
    .dropDuplicates()
    .withColumn(
        hash_key,
        sha2(
            concat_ws("||", *[col(c).cast("string") for c in business_keys]),
            256
        )
    )
    .withColumn("loadTs", current_timestamp())
    .withColumn("loadDate", current_date())
)

display(df_hub.limit(10))

# COMMAND ----------

(
    df_hub.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Hub criado: {target_table}")