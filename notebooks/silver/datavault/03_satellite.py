# Databricks notebook source

import yaml
from pathlib import Path

from pyspark.sql.functions import sha2, concat_ws, col, current_timestamp, current_date

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

print(f"Satellite: {satellite_name}")
print(f"Source: {source_table}")
print(f"Target: {target_table}")
print(f"Hash key: {hash_key}")
print(f"Parent keys: {parent_keys}")
print(f"Attributes: {attributes}")

# COMMAND ----------

df = spark.table(source_table)

# COMMAND ----------

source_columns = df.columns
source_columns_lower = {c.lower(): c for c in source_columns}

# se attributes não vier definido, pega todas as colunas do source,
# exceto parent_keys, hash_key e colunas técnicas
if not attributes:
    excluded = set([c.lower() for c in parent_keys])
    excluded.add(hash_key.lower())

    attributes = [
        c for c in source_columns
        if c.lower() not in excluded
        and not c.startswith("_")
    ]

print(f"Final attributes: {attributes}")

# COMMAND ----------

select_exprs = []

for attr in attributes:
    if attr in source_columns:
        select_exprs.append(col(attr))

# COMMAND ----------

df_sat = (
    df
    .select(
        *[col(c) for c in parent_keys],
        *select_exprs
    )
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

final_columns = [hash_key] + attributes

# remove duplicados case-insensitive
seen = set()
final_columns_clean = []

for c in final_columns:
    if c.lower() not in seen:
        seen.add(c.lower())
        final_columns_clean.append(c)

df_sat = (
    df_sat
    .select(*[col(c) for c in final_columns_clean])
    .dropDuplicates()
    .withColumn("loadTs", current_timestamp())
    .withColumn("loadDate", current_date())
)

print(df_sat.columns)
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