# Databricks notebook source

import yaml
from pathlib import Path
from pyspark.sql.functions import col, count, current_timestamp, lit

# COMMAND ----------

dbutils.widgets.text("config", "")
dbutils.widgets.text("source", "")

config_arg = dbutils.widgets.get("config")
source_name = dbutils.widgets.get("source")

print(f"Config: {config_arg}")
print(f"Source: {source_name}")

# COMMAND ----------

config_path = (Path.cwd() / config_arg).resolve()

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

source = config["sources"][source_name]

catalog = source["catalog"]
schema = source["schema"]
bronze_table = source.get("bronze_table", source_name)

target_table = f"{catalog}.{schema}.{bronze_table}"

dq_config = (
    source
    .get("data_quality", {})
    .get("bronze", {})
)

print(f"Target table: {target_table}")
print(f"Data quality config: {dq_config}")

# COMMAND ----------

df = spark.table(target_table)

results = []

def add_result(check_name, status, metric_value=None, threshold_value=None, message=None):
    results.append({
        "sourceName": source_name,
        "layer": "bronze",
        "tableName": target_table,
        "checkName": check_name,
        "checkStatus": status,
        "metricValue": str(metric_value) if metric_value is not None else None,
        "thresholdValue": str(threshold_value) if threshold_value is not None else None,
        "message": message
    })

# COMMAND ----------

# Check 1: min rows

row_count = df.count()
min_rows = dq_config.get("min_rows")

if min_rows is not None:
    if row_count >= min_rows:
        add_result("min_rows", "PASSED", row_count, min_rows)
    else:
        add_result(
            "min_rows",
            "FAILED",
            row_count,
            min_rows,
            f"Row count abaixo do mínimo esperado. Atual: {row_count}, esperado: {min_rows}"
        )

# COMMAND ----------

# Check 2: required columns

required_columns = dq_config.get("required_columns", [])
existing_columns = set(df.columns)

for required_column in required_columns:
    if required_column in existing_columns:
        add_result("required_column", "PASSED", required_column, None)
    else:
        add_result(
            "required_column",
            "FAILED",
            required_column,
            None,
            f"Coluna obrigatória ausente: {required_column}"
        )

# COMMAND ----------

# Check 3: not null

not_null_columns = dq_config.get("not_null", [])

for column_name in not_null_columns:
    if column_name not in existing_columns:
        add_result(
            "not_null",
            "FAILED",
            column_name,
            None,
            f"Coluna não encontrada para validação not_null: {column_name}"
        )
        continue

    null_count = df.filter(col(column_name).isNull()).count()

    if null_count == 0:
        add_result("not_null", "PASSED", 0, 0, f"{column_name} sem nulos")
    else:
        add_result(
            "not_null",
            "FAILED",
            null_count,
            0,
            f"Coluna {column_name} possui {null_count} valores nulos"
        )

# COMMAND ----------

# Check 4: unique key

unique_keys = dq_config.get("unique_keys", [])

if unique_keys:
    missing_keys = [c for c in unique_keys if c not in existing_columns]

    if missing_keys:
        add_result(
            "unique_keys",
            "FAILED",
            ",".join(missing_keys),
            None,
            f"Colunas de chave únicas ausentes: {missing_keys}"
        )
    else:
        duplicate_count = (
            df
            .groupBy(*unique_keys)
            .agg(count("*").alias("cnt"))
            .filter(col("cnt") > 1)
            .count()
        )

        if duplicate_count == 0:
            add_result("unique_keys", "PASSED", 0, 0)
        else:
            add_result(
                "unique_keys",
                "FAILED",
                duplicate_count,
                0,
                f"Foram encontradas {duplicate_count} chaves duplicadas"
            )

# COMMAND ----------

# Persistência dos resultados

audit_schema = f"{catalog}.audit"
audit_table = f"{audit_schema}.dataQualityResults"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {audit_schema}")

df_results = spark.createDataFrame(results).withColumn(
    "checkedAt",
    current_timestamp()
)

(
    df_results.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(audit_table)
)

display(df_results)

# COMMAND ----------

failed_checks = [r for r in results if r["checkStatus"] == "FAILED"]

if failed_checks:
    raise Exception(f"Data Quality FAILED para {source_name}: {failed_checks}")

print(f"Data Quality PASSED para {source_name}")