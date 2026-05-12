# Databricks notebook source

from pyspark.sql.functions import (
    sha2,
    concat_ws,
    col,
    current_timestamp,
    current_date
)

# COMMAND ----------

source_table = "lakehouse_lab.bronze.investidores"
target_table = "lakehouse_lab.silver.hubInvestidor"

business_key = "codigoDoInvestidor"

# COMMAND ----------

df = spark.table(source_table)

display(df.limit(10))

# COMMAND ----------

df_hub = (
    df
    .select(business_key)
    .dropna()
    .dropDuplicates()
    .withColumn(
        "hubInvestidorHk",
        sha2(concat_ws("||", col(business_key).cast("string")), 256)
    )
    .withColumn("loadTs", current_timestamp())
    .withColumn("loadDate", current_date())
)

display(df_hub.limit(10))

# COMMAND ----------

(
    df_hub.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)

print(f"Hub criado: {target_table}")