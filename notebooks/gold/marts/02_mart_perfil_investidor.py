# Databricks notebook source

import yaml
from pathlib import Path

from pyspark.sql.functions import (
    countDistinct,
    count,
    current_timestamp,
    current_date
)

# COMMAND ----------

dbutils.widgets.text("config", "")
dbutils.widgets.text("mart_name", "martPerfilInvestidor")

config_arg = dbutils.widgets.get("config")
mart_name = dbutils.widgets.get("mart_name")

# COMMAND ----------

def load_yaml_config(config_arg: str) -> dict:
    config_path = (Path.cwd() / config_arg).resolve()
    print(f"Config path resolvido: {config_path}")

    with open(config_path, "r") as f:
        return yaml.safe_load(f)

# COMMAND ----------

config = load_yaml_config(config_arg)
mart_config = config["gold"]["marts"][mart_name]

source_table = mart_config["source_table"]
target_table = mart_config["target_table"]

# COMMAND ----------

df = spark.table(source_table)

# COMMAND ----------

profile_columns = [
    c for c in [
        "estadoCivil",
        "genero",
        "profissao",
        "uf",
        "cidade",
        "pais",
        "situacaoDaConta"
    ]
    if c in df.columns
]

# COMMAND ----------

if not profile_columns:
    df_mart = (
        df
        .agg(
            countDistinct("codigoDoInvestidor").alias("totalInvestidores")
        )
        .withColumn("_martLoadTs", current_timestamp())
        .withColumn("_martLoadDate", current_date())
    )
else:
    df_mart = (
        df
        .groupBy(*profile_columns)
        .agg(
            countDistinct("codigoDoInvestidor").alias("totalInvestidores"),
            count("*").alias("totalRegistros")
        )
        .withColumn("_martLoadTs", current_timestamp())
        .withColumn("_martLoadDate", current_date())
    )

# COMMAND ----------

(
    df_mart.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Mart criada: {target_table}")

# COMMAND ----------

display(spark.table(target_table).limit(100))