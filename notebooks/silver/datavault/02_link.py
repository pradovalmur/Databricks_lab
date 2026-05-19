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
dbutils.widgets.text("link_name", "")

config_arg = dbutils.widgets.get("config")
link_name = dbutils.widgets.get("link_name")

# COMMAND ----------

def load_yaml_config(config_arg: str) -> dict:
    config_path = (Path.cwd() / config_arg).resolve()

    print(f"Config path resolvido: {config_path}")

    with open(config_path, "r") as f:
        return yaml.safe_load(f)

# COMMAND ----------

config = load_yaml_config(config_arg)

link_config = config["links"][link_name]

source_table = link_config["source_table"]
target_table = link_config["target_table"]

hash_key = link_config["hash_key"]

business_keys_config = link_config["business_keys"]
link_keys = link_config["link_keys"]

# COMMAND ----------

df = spark.table(source_table)

# COMMAND ----------

hub_columns = []

for hub_name, hub_data in business_keys_config.items():
    business_keys = hub_data["keys"]
    hub_key = hub_data["hub_key"]

    df = df.withColumn(
        hub_key,
        sha2(
            concat_ws(
                "||",
                *[col(c).cast("string") for c in business_keys]
            ),
            256
        )
    )

    hub_columns.append(hub_key)

# COMMAND ----------

df_link = (
    df
    .select(*hub_columns, *link_keys)
    .dropna()
    .dropDuplicates()
    .withColumn(
        hash_key,
        sha2(
            concat_ws(
                "||",
                *[col(c).cast("string") for c in link_keys]
            ),
            256
        )
    )
    .withColumn("loadTs", current_timestamp())
    .withColumn("loadDate", current_date())
)

display(df_link.limit(10))

# COMMAND ----------

(
    df_link.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Link criado: {target_table}")