# Databricks notebook source

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, current_timestamp, current_date

# COMMAND ----------

source_hub = "lakehouse_lab.silver.hubInvestidor"
source_sat = "lakehouse_lab.silver.satInvestidor"
target_table = "lakehouse_lab.gold.pitInvestidor"

# COMMAND ----------

df_hub = spark.table(source_hub)
df_sat = spark.table(source_sat)

# COMMAND ----------

w = (
    Window
    .partitionBy("hubInvestidorHk")
    .orderBy(col("loadTs").desc())
)

df_sat_latest = (
    df_sat
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

df_pit = (
    df_hub.alias("h")
    .join(
        df_sat_latest.alias("s"),
        on="hubInvestidorHk",
        how="left"
    )
    .select(
        col("h.hubInvestidorHk"),
        col("h.codigoDoInvestidor"),
        col("s.*")
    )
    .drop("hubInvestidorHk")
    .withColumnRenamed("h.hubInvestidorHk", "hubInvestidorHk")
    .withColumn("_pitLoadTs", current_timestamp())
    .withColumn("_pitLoadDate", current_date())
)

# COMMAND ----------

(
    df_pit.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"PIT criada: {target_table}")

# COMMAND ----------

display(spark.table(target_table).limit(10))