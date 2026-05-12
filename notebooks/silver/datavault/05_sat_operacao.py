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
target_table = "lakehouse_lab.silver.satOperacao"

# COMMAND ----------

df = spark.table(source_table)

# COMMAND ----------

df_sat = (
    df
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
    .withColumn("loadTs", current_timestamp())
    .withColumn("loadDate", current_date())
)

# COMMAND ----------

(
    df_sat.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(target_table)
)

print(f"Satellite criado: {target_table}")