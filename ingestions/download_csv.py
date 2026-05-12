import argparse
import yaml
import requests
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

parser = argparse.ArgumentParser()
parser.add_argument("--config", required=True)
parser.add_argument("--source", required=True)
args = parser.parse_args()

with open(args.config, "r") as f:
    config = yaml.safe_load(f)

source = config["sources"][args.source]

if not source.get("enabled", True):
    raise Exception(f"Source disabled: {args.source}")

url = source["url"]
catalog = source["catalog"]
schema = source["schema"]
volume = source["volume"]
file_name = source["file_name"]

volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
target_path = f"{volume_path}/{file_name}"
tmp_file = f"/tmp/{file_name}"

response = requests.get(url, stream=True, timeout=(30, 600))
response.raise_for_status()

with open(tmp_file, "wb") as f:
    for chunk in response.iter_content(chunk_size=1024 * 1024):
        if chunk:
            f.write(chunk)

dbutils.fs.cp(f"file:{tmp_file}", target_path, True)

print(f"Arquivo salvo em: {target_path}")