import argparse
from pathlib import Path

import requests
import yaml


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def download_file(url: str, target_path: str, overwrite: bool = False) -> None:
    target = Path(target_path)
    target.parent.mkdir(parents=True, exist_ok=True)

    if target.exists() and not overwrite:
        print(f"Arquivo já existe. Skip download: {target_path}")
        return

    if target.exists() and overwrite:
        print(f"Arquivo já existe, mas overwrite=true. Sobrescrevendo: {target_path}")

    with requests.get(url, stream=True, timeout=(30, 600)) as response:
        response.raise_for_status()

        with open(target_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    print(f"Download finalizado: {target_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--source", required=True)

    args = parser.parse_args()

    config_path = Path(args.config).resolve()

    print(f"Config path: {config_path}")
    print(f"Source: {args.source}")

    config = load_config(config_path)
    source = config["sources"][args.source]

    if not source.get("enabled", True):
        print(f"Source desativada. Skip: {args.source}")
        return

    url = source["url"]
    catalog = source["catalog"]
    schema = source["schema"]
    volume = source["volume"]
    file_name = source["file_name"]
    overwrite = source.get("overwrite", False)

    volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
    target_path = f"{volume_path}/{file_name}"

    print(f"URL: {url}")
    print(f"Target path: {target_path}")
    print(f"Overwrite: {overwrite}")

    download_file(url, target_path, overwrite)


if __name__ == "__main__":
    main()