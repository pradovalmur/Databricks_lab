# Databricks notebook source

csv_url = "https://www.tesourotransparente.gov.br/ckan/dataset/78739a33-4d2f-4e35-88fd-65f1ccbe81c4/resource/6908b59b-d9da-4b33-b15a-a89c2b71dc50/download/operacoestesourodireto2025.csv"

catalog_name = "lakehouse_lab"
schema_name = "bronze"
table_name = "dados_bronze"

volume_path = f"/Volumes/{catalog_name}/{schema_name}/raw"
raw_file_name = "arquivo_origem.csv"

raw_file_path = f"{volume_path}/{raw_file_name}"
bronze_table = f"{catalog_name}.{schema_name}.{table_name}"

# COMMAND ----------

dbutils.fs.mkdirs(volume_path)

# COMMAND ----------

# COMMAND ----------

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

session = requests.Session()

retry_strategy = Retry(
    total=5,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)

adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("http://", adapter)
session.mount("https://", adapter)

local_tmp_file = f"/tmp/{raw_file_name}"

with session.get(csv_url, stream=True, timeout=(30, 600)) as response:
    response.raise_for_status()

    with open(local_tmp_file, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

dbutils.fs.cp(f"file:{local_tmp_file}", raw_file_path, True)

print(f"Arquivo CSV bruto salvo em: {raw_file_path}")

# COMMAND ----------

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .csv(raw_file_path)
)

display(df.limit(10))

# COMMAND ----------

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(bronze_table)
)

print(f"Tabela Bronze criada: {bronze_table}")

# COMMAND ----------

spark.sql(f"""
SELECT COUNT(*) AS total_registros
FROM {bronze_table}
""").display()