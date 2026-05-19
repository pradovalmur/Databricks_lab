# Databricks notebook source

import yaml
from pathlib import Path

from pyspark.sql.functions import (
    countDistinct,
    sum as spark_sum,
    min as spark_min,
    max as spark_max,
    current_timestamp,
    current_date
)

# COMMAND ----------

dbutils.widgets.text("config", "")
dbutils.widgets.text("mart_name", "martResumoTitulo")

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

df_mart = (
    df
    .groupBy("tipoTitulo")
    .agg(
        countDistinct("codigoDoInvestidor").alias("totalInvestidores"),
        spark_sum("totalOperacoes").alias("totalOperacoes"),
        spark_sum("valorTotalOperado").alias("valorTotalOperado"),
        spark_min("primeiraOperacao").alias("primeiraOperacao"),
        spark_max("ultimaOperacao").alias("ultimaOperacao")
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

display(spark.table(target_table))