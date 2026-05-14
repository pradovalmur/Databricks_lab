# Databricks notebook source

from pyspark.sql.functions import (
    col,
    countDistinct,
    sum as spark_sum,
    min as spark_min,
    max as spark_max,
    current_timestamp,
    current_date
)

# COMMAND ----------

source_table = "lakehouse_lab.gold.bridgeInvestidorTitulo"
target_table = "lakehouse_lab.gold.martResumoTitulo"

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