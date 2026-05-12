# Databricks notebook source

from pyspark.sql.functions import (
    sha2,
    concat_ws,
    col,
    current_timestamp,
    current_date
)

# COMMAND ----------

source_table = "lakehouse_lab.bronze.operacoes"

target_table = (
    "lakehouse_lab.silver.linkInvestidorTituloOperacao"
)

# COMMAND ----------

df = spark.table(source_table)

# COMMAND ----------

df_link = (
    df
    .withColumn(
        "hubInvestidorHk",
        sha2(
            concat_ws(
                "||",
                col("codigoDoInvestidor").cast("string")
            ),
            256
        )
    )
    .withColumn(
        "hubTituloHk",
        sha2(
            concat_ws(
                "||",
                col("tipoTitulo").cast("string")
            ),
            256
        )
    )
    .withColumn(
        "linkInvestidorTituloOperacaoHk",
        sha2(
            concat_ws(
                "||",
                col("codigoDoInvestidor").cast("string"),
                col("tipoTitulo").cast("string"),
                col("dataDaOperacao").cast("string")
            ),
            256
        )
    )
    .select(
        "linkInvestidorTituloOperacaoHk",
        "hubInvestidorHk",
        "hubTituloHk",
        "dataDaOperacao"
    )
    .dropDuplicates()
    .withColumn("loadTs", current_timestamp())
    .withColumn("loadDate", current_date())
)

# COMMAND ----------

(
    df_link.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)

print(f"Link criado: {target_table}")