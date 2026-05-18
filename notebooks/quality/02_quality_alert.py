# Databricks notebook source

import yaml
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

print(f"Alerts config: {alerts_config_arg}")
print(f"Pipeline: {pipeline_name}")
print(f"Task: {task_name}")
print(f"Severity: {severity}")

# COMMAND ----------

config_path = (Path.cwd() / alerts_config_arg).resolve()

with open(config_path, "r") as f:
    alerts_config = yaml.safe_load(f)

alerts = alerts_config.get("alerts", {})

# COMMAND ----------

def get_emails():
    # prioridade:
    # task -> pipeline -> default

    task_alerts = (
        alerts
        .get("tasks", {})
        .get(task_name, {})
        .get(severity, {})
        .get("emails")
    )

    if task_alerts:
        return task_alerts

    pipeline_alerts = (
        alerts
        .get("pipelines", {})
        .get(pipeline_name, {})
        .get(severity, {})
        .get("emails")
    )

    if pipeline_alerts:
        return pipeline_alerts

    default_alerts = (
        alerts
        .get("default", {})
        .get(severity, {})
        .get("emails", [])
    )

    return default_alerts

# COMMAND ----------

emails = get_emails()

print(f"Emails encontrados: {emails}")

# COMMAND ----------

audit_table = f"{catalog}.audit.dataQualityResults"

query = f"""
SELECT *
FROM {audit_table}
WHERE pipelineName = '{pipeline_name}'
  AND taskName = '{task_name}'
  AND severity = '{severity}'
  AND checkStatus = 'FAILED'
ORDER BY checkedAt DESC
LIMIT 100
"""

df_alerts = spark.sql(query)

display(df_alerts)

# COMMAND ----------

alerts_count = df_alerts.count()

if alerts_count == 0:
    print("Nenhum alerta encontrado.")
    dbutils.notebook.exit("NO_ALERTS")

# COMMAND ----------

alerts_rows = df_alerts.collect()

html_rows = ""

for row in alerts_rows:
    html_rows += f"""
    <tr>
        <td>{row['checkName']}</td>
        <td>{row['checkStatus']}</td>
        <td>{row['severity']}</td>
        <td>{row['message']}</td>
        <td>{row['tableName']}</td>
        <td>{row['checkedAt']}</td>
    </tr>
    """

# COMMAND ----------

html_body = f"""
<h2>Data Quality Alert</h2>

<p>
Pipeline: <b>{pipeline_name}</b><br>
Task: <b>{task_name}</b><br>
Severity: <b>{severity}</b><br>
Total alerts: <b>{alerts_count}</b>
</p>

<table border="1" cellpadding="5" cellspacing="0">
    <tr>
        <th>Check</th>
        <th>Status</th>
        <th>Severity</th>
        <th>Message</th>
        <th>Table</th>
        <th>Checked At</th>
    </tr>

    {html_rows}

</table>
"""

print(html_body)

# COMMAND ----------

# Aqui você poderá integrar:
#
# - SMTP
# - SendGrid
# - AWS SES
# - Teams
# - Slack
# - Webhook
#
# Exemplo futuro:
#
# send_email(
#     to=emails,
#     subject=f"[{severity}] Data Quality Alert - {pipeline_name}",
#     html=html_body
# )

print("Alerta preparado com sucesso.")
print(f"Destinatários: {emails}")

dbutils.notebook.exit("ALERT_CREATED")