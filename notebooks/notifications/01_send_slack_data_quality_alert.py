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

def load_yaml_config(config_arg: str) -> dict:
    config_path = (Path.cwd() / config_arg).resolve()

    print(f"Config path resolvido: {config_path}")

    with open(config_path, "r") as f:
        return yaml.safe_load(f)

# COMMAND ----------

alerts_config = load_yaml_config(alerts_config_arg)

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

try:
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

except Exception as e:
    alerts_count = 0
    df_alerts = None
    audit_error = str(e)
else:
    audit_error = None

# COMMAND ----------

if alerts_count > 0:
    alerts = df_alerts.collect()

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
else:
    extra_info = ""

    if audit_error:
        extra_info = f"\n*Audit read error:* `{audit_error}`"

    message = f"""
:warning: *Pipeline Task Failed*

*Pipeline:* `{pipeline_name}`
*Task:* `{task_name}`
*Severity:* `{severity}`

A task falhou, mas nenhum registro de Data Quality foi encontrado em `{audit_table}`.

Provável erro técnico antes de gravar auditoria.{extra_info}
"""

# COMMAND ----------

payload = {
    "channel": f"#{channel}",
    "text": message
}

response = requests.post(webhook_url, json=payload, timeout=30)
response.raise_for_status()

print("Alerta enviado para Slack com sucesso.")
dbutils.notebook.exit("SLACK_ALERT_SENT")