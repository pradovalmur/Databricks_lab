# ingestion/download_csv.py

import argparse
from pathlib import Path

import requests
import yaml


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def download_file(url: str, target_path: str) -> None:
    Path(target_path).parent.mkdir(parents=True, exist_ok=True)

    with requests.get(url, stream=True, timeout=(30, 600)) as response:
        response.raise_for_status()

        with open(target_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--source", required=True)
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    config_path = (script_dir / args.config).resolve()

    print(f"Config path: {config_path}")
    print(f"Source: {args.source}")

    config = load_config(config_path)

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

    print(f"URL: {url}")
    print(f"Target path: {target_path}")

    download_file(url, target_path)

    print(f"Arquivo salvo com sucesso em: {target_path}")


if __name__ == "__main__":
    main()