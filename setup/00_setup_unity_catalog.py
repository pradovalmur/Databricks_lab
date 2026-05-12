# Databricks notebook source

catalog_name = "lakehouse_lab"
schema_bronze = "bronze"
volume_name = "raw"

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_bronze}")

spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_bronze}.{volume_name}
""")

print("Unity Catalog configurado.")