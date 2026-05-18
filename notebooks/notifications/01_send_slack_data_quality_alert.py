# Databricks notebook source

import yaml
import requests
from pathlib import Path

# COMMAND ----------

dbutils.widgets.text("alerts_config", "")
dbutils.widgets.text("catalog", "lakehouse_lab")
dbutils.widgets.text("pipeline_name", "")
dbutils.widgets.text("task_name", "")
dbutils.widgets.text("severity", "ERROR")

alerts_config_arg = dbutils.widgets.get("alerts_config")
catalog = dbutils.widgets.get("catalog")
pipeline_name = dbutils.widgets.get("pipeline_name")
task_name = dbutils.widgets.get("task_name")
severity = dbutils.widgets.get("severity")

# COMMAND ----------

config_path = (Path.cwd() / alerts_config_arg).resolve()

with open(config_path, "r") as f:
    alerts_config = yaml.safe_load(f)

slack_config = alerts_config["alerts"]["slack"]

if not slack_config.get("enabled", False):
    dbutils.notebook.exit("SLACK_ALERT_DISABLED")

webhook_url = dbutils.secrets.get(
    scope=slack_config["webhook_secret_scope"],
    key=slack_config["webhook_secret_key"]
)

channel = slack_config.get("channel", "data_quality_notification")

# COMMAND ----------

audit_table = f"{catalog}.audit.dataQualityResults"

df_alerts = spark.sql(f"""
SELECT *
FROM {audit_table}
WHERE pipelineName = '{pipeline_name}'
  AND taskName = '{task_name}'
  AND severity = '{severity}'
  AND checkStatus = 'FAILED'
ORDER BY checkedAt DESC
LIMIT 20
""")

alerts_count = df_alerts.count()

if alerts_count == 0:
    dbutils.notebook.exit("NO_ALERTS_FOUND")

alerts = df_alerts.collect()

# COMMAND ----------

alert_lines = []

for row in alerts:
    alert_lines.append(
        f"• *{row['checkName']}* | `{row['tableName']}` | {row['message']}"
    )

message = f"""
:rotating_light: *Data Quality Alert*

*Pipeline:* `{pipeline_name}`
*Task:* `{task_name}`
*Severity:* `{severity}`
*Total failures:* `{alerts_count}`

*Failures:*
{chr(10).join(alert_lines)}
"""

payload = {
    "channel": f"#{channel}",
    "text": message
}

response = requests.post(webhook_url, json=payload, timeout=30)
response.raise_for_status()

print("Alerta enviado para Slack com sucesso.")
dbutils.notebook.exit("SLACK_ALERT_SENT")