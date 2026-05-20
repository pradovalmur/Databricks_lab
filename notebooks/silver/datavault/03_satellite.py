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


def select_unique_columns_case_insensitive(df):
    seen = set()
    selected_columns = []

    for column_name in df.columns:
        normalized_name = column_name.lower()

        if normalized_name not in seen:
            seen.add(normalized_name)
            selected_columns.append(column_name)

    return df.select(*[col(c) for c in selected_columns])

# COMMAND ----------

config = load_yaml_config(config_arg)

sat_config = config["satellites"][satellite_name]

source_table = sat_config["source_table"]
target_table = sat_config["target_table"]
hash_key = sat_config["hash_key"]
parent_keys = sat_config["parent_keys"]
attributes = sat_config.get("attributes", [])

print(f"Satellite: {satellite_name}")
print(f"Source table: {source_table}")
print(f"Target table: {target_table}")
print(f"Hash key: {hash_key}")

# COMMAND ----------

df = spark.table(source_table)

df = select_unique_columns_case_insensitive(df)

# Remove qualquer coluna com mesmo nome da hash key, ignorando maiúscula/minúscula
columns_without_hash_key = [
    c for c in df.columns
    if c.lower() != hash_key.lower()
]

df = df.select(*[col(c) for c in columns_without_hash_key])

# COMMAND ----------

df_sat = df.withColumn(
    hash_key,
    sha2(
        concat_ws(
            "||",
            *[col(c).cast("string") for c in parent_keys]
        ),
        256
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
        and c.lower() != hash_key.lower()
    ]

# Deduplica seleção final ignorando maiúscula/minúscula
final_columns = []
seen = set()

for c in columns_to_select:
    normalized = c.lower()

    if normalized not in seen:
        seen.add(normalized)
        final_columns.append(c)

df_sat = df_sat.select(*[col(c) for c in final_columns])

# COMMAND ----------

df_sat = (
    df_sat
    .dropDuplicates()
    .withColumn("loadTs", current_timestamp())
    .withColumn("loadDate", current_date())
)

df_sat = select_unique_columns_case_insensitive(df_sat)

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