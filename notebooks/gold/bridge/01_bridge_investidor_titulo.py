# Databricks notebook source

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

source_link = "lakehouse_lab.silver.linkInvestidorTituloOperacao"
source_hub_investidor = "lakehouse_lab.silver.hubInvestidor"
source_hub_titulo = "lakehouse_lab.silver.hubTitulo"
source_sat_operacao = "lakehouse_lab.silver.satOperacao"

target_table = "lakehouse_lab.gold.bridgeInvestidorTitulo"

# COMMAND ----------

df_link = spark.table(source_link)
df_inv = spark.table(source_hub_investidor)
df_titulo = spark.table(source_hub_titulo)
df_sat_operacao = spark.table(source_sat_operacao)

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