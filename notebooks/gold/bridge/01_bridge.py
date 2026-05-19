# Databricks notebook source

import yaml
from pathlib import Path

from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
    min as spark_min,
    max as spark_max,
    current_timestamp,
    current_date,
    regexp_replace
)

# COMMAND ----------

dbutils.widgets.text("config", "")
dbutils.widgets.text("bridge_name", "")

config_arg = dbutils.widgets.get("config")
bridge_name = dbutils.widgets.get("bridge_name")

# COMMAND ----------

def load_yaml_config(config_arg: str) -> dict:
    config_path = (Path.cwd() / config_arg).resolve()
    print(f"Config path resolvido: {config_path}")

    with open(config_path, "r") as f:
        return yaml.safe_load(f)

# COMMAND ----------

config = load_yaml_config(config_arg)
bridge_config = config["gold"]["bridges"][bridge_name]

target_table = bridge_config["target_table"]
source_link = bridge_config["source_link"]
source_hubs = bridge_config["source_hubs"]
source_satellites = bridge_config["source_satellites"]

# COMMAND ----------

df_link = spark.table(source_link)
df_inv = spark.table(source_hubs["investidor"])
df_titulo = spark.table(source_hubs["titulo"])
df_sat_operacao = spark.table(source_satellites["operacao"])

# COMMAND ----------

df_sat_operacao = df_sat_operacao.withColumn(
    "valorDaOperacaoDouble",
    regexp_replace(col("valorDaOperacao"), ",", ".").cast("double")
)

# COMMAND ----------

df_bridge = (
    df_link.alias("l")
    .join(
        df_inv.alias("i"),
        col("l.hubInvestidorHk") == col("i.hubInvestidorHk"),
        "left"
    )
    .join(
        df_titulo.alias("t"),
        col("l.hubTituloHk") == col("t.hubTituloHk"),
        "left"
    )
    .join(
        df_sat_operacao.alias("s"),
        col("l.linkInvestidorTituloOperacaoHk") == col("s.linkInvestidorTituloOperacaoHk"),
        "left"
    )
    .groupBy(
        col("l.hubInvestidorHk"),
        col("l.hubTituloHk"),
        col("i.codigoDoInvestidor"),
        col("t.tipoTitulo")
    )
    .agg(
        count("*").alias("totalOperacoes"),
        countDistinct("s.dataDaOperacao").alias("totalDiasOperados"),
        spark_sum(col("s.valorDaOperacaoDouble")).alias("valorTotalOperado"),
        spark_min(col("s.dataDaOperacao")).alias("primeiraOperacao"),
        spark_max(col("s.dataDaOperacao")).alias("ultimaOperacao")
    )
    .withColumn("_bridgeLoadTs", current_timestamp())
    .withColumn("_bridgeLoadDate", current_date())
)

# COMMAND ----------

(
    df_bridge.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Bridge criada: {target_table}")

# COMMAND ----------

display(spark.table(target_table).limit(10))