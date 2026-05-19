# Databricks notebook source

import yaml
from pathlib import Path

from pyspark.sql.functions import col, count, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

dbutils.widgets.text("config", "")
dbutils.widgets.text("link_name", "")
dbutils.widgets.text("pipeline_name", "tesouro_direto_pipeline")
dbutils.widgets.text("task_name", "quality_silver_link")
dbutils.widgets.text("severity_on_failure", "ERROR")

config_arg = dbutils.widgets.get("config")
link_name = dbutils.widgets.get("link_name")
pipeline_name = dbutils.widgets.get("pipeline_name")
task_name = dbutils.widgets.get("task_name")
severity_on_failure = dbutils.widgets.get("severity_on_failure")

# COMMAND ----------

config_path = (Path.cwd() / config_arg).resolve()

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

link_config = config["quality"]["links"][link_name]

target_table = link_config["table"]
link_hash_key = link_config["hash_key"]
hub_keys = link_config["hub_keys"]

catalog = target_table.split(".")[0]
audit_schema = f"{catalog}.audit"
audit_table = f"{audit_schema}.dataQualityResults"

# COMMAND ----------

df = spark.table(target_table)
existing_columns = set(df.columns)
results = []

def add_result(check_name, status, metric_value=None, threshold_value=None, message=None, severity="INFO"):
    results.append({
        "pipelineName": pipeline_name,
        "taskName": task_name,
        "sourceName": link_name,
        "layer": "silver",
        "tableName": target_table,
        "checkName": check_name,
        "checkStatus": status,
        "severity": severity,
        "metricValue": str(metric_value) if metric_value is not None else None,
        "thresholdValue": str(threshold_value) if threshold_value is not None else None,
        "message": message,
        "jobId": None,
        "runId": None
    })

# COMMAND ----------

row_count = df.count()

if row_count > 0:
    add_result("min_rows", "PASSED", row_count, 1)
else:
    add_result("min_rows", "FAILED", row_count, 1, "Link sem registros", severity_on_failure)

# COMMAND ----------

if link_hash_key not in existing_columns:
    add_result("link_hash_key_exists", "FAILED", link_hash_key, None, f"Hash key ausente: {link_hash_key}", severity_on_failure)
else:
    add_result("link_hash_key_exists", "PASSED", link_hash_key)

    null_count = df.filter(col(link_hash_key).isNull()).count()

    if null_count == 0:
        add_result("link_hash_key_not_null", "PASSED", 0, 0)
    else:
        add_result("link_hash_key_not_null", "FAILED", null_count, 0, f"{link_hash_key} possui nulos", severity_on_failure)

    duplicate_count = (
        df.groupBy(link_hash_key)
        .agg(count("*").alias("cnt"))
        .filter(col("cnt") > 1)
        .count()
    )

    if duplicate_count == 0:
        add_result("link_hash_key_unique", "PASSED", 0, 0)
    else:
        add_result("link_hash_key_unique", "FAILED", duplicate_count, 0, f"{link_hash_key} possui duplicados", severity_on_failure)

# COMMAND ----------

for hub_key in hub_keys:
    if hub_key not in existing_columns:
        add_result("hub_key_exists", "FAILED", hub_key, None, f"Hub key ausente: {hub_key}", severity_on_failure)
        continue

    add_result("hub_key_exists", "PASSED", hub_key)

    null_count = df.filter(col(hub_key).isNull()).count()

    if null_count == 0:
        add_result("hub_key_not_null", "PASSED", 0, 0, f"{hub_key} sem nulos")
    else:
        add_result("hub_key_not_null", "FAILED", null_count, 0, f"{hub_key} possui nulos", severity_on_failure)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {audit_schema}")

schema_results = StructType([
    StructField("pipelineName", StringType(), True),
    StructField("taskName", StringType(), True),
    StructField("sourceName", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("tableName", StringType(), True),
    StructField("checkName", StringType(), True),
    StructField("checkStatus", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("metricValue", StringType(), True),
    StructField("thresholdValue", StringType(), True),
    StructField("message", StringType(), True),
    StructField("jobId", StringType(), True),
    StructField("runId", StringType(), True)
])

df_results = spark.createDataFrame(results, schema=schema_results).withColumn("checkedAt", current_timestamp())

if spark.catalog.tableExists(audit_table):
    df_results.write.format("delta").mode("append").insertInto(audit_table)
else:
    df_results.write.format("delta").mode("overwrite").saveAsTable(audit_table)

display(df_results)

# COMMAND ----------

failed_checks = [r for r in results if r["checkStatus"] == "FAILED"]

if failed_checks:
    raise Exception(f"Data Quality FAILED para {link_name}: {failed_checks}")

print(f"Data Quality PASSED para {link_name}")