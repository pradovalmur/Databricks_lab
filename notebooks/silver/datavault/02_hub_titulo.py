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
target_table = "lakehouse_lab.silver.hubTitulo"

business_key = "tipoTitulo"

# COMMAND ----------

df = spark.table(source_table)

# COMMAND ----------

df_hub = (
    df
    .select(business_key)
    .dropna()
    .dropDuplicates()
    .withColumn(
        "hubTituloHk",
        sha2(concat_ws("||", col(business_key).cast("string")), 256)
    )
    .withColumn("loadTs", current_timestamp())
    .withColumn("loadDate", current_date())
)

# COMMAND ----------

(
    df_hub.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)

print(f"Hub criado: {target_table}")