# Databricks notebook source

from pyspark.sql.functions import (
    col,
    countDistinct,
    count,
    current_timestamp,
    current_date
)

# COMMAND ----------

source_table = "lakehouse_lab.gold.pitInvestidor"
target_table = "lakehouse_lab.gold.martPerfilInvestidor"

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

print(f"Colunas de perfil encontradas: {profile_columns}")

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