# Databricks notebook source

catalog_name = "lakehouse_lab"

schemas = ["bronze", "silver", "gold"]

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")

for schema in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema}")

# COMMAND ----------

spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {catalog_name}.bronze.raw
""")

print("Unity Catalog configurado com bronze, silver, gold e volume raw.")