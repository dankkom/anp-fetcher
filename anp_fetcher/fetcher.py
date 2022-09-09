import json
from pathlib import Path

import httpx
from tqdm import tqdm

from .config import CKAN_PACKAGE_SHOW_FMT, DEST_DATA_DIR
from .metadata import datasets


def fetch_file(url: str, dest_filepath: Path) -> bytes:
    print(url, "->", dest_filepath)
    args = {
        "method": "GET",
        "url": url,
        "timeout": 30,
        "follow_redirects": True,
    }
    dest_filepath.parent.mkdir(parents=True, exist_ok=True)
    while True:
        try:
            with httpx.stream(**args) as r:
                if "Content-Length" not in r.headers:
                    raise Exception("No Content-Length")
                total = int(r.headers["Content-Length"])
                progress = tqdm(
                    total=total,
                    unit="B",
                    unit_scale=True,
                )
                with dest_filepath.open("ab") as f:
                    for chunk in r.iter_bytes():
                        f.write(chunk)
                        progress.update(len(chunk))
                progress.close()
                break
        except httpx.ProtocolError as e:
            print("\nProtocol error", e)
        except httpx.ConnectTimeout as e:
            print("\nConnection timeout", e)
            dest_filepath.unlink(missing_ok=True)
            break


def get_ckan_package_metadata(dataset_id: str) -> dict:
    url = CKAN_PACKAGE_SHOW_FMT.format(dataset_id=dataset_id)
    r = httpx.get(url)
    metadata = r.json()
    return metadata


def download_shpc_metadata():
    dest_filepath = DEST_DATA_DIR / "shpc" / "metadata.html"
    if dest_filepath.exists():
        return
    dest_filepath.parent.mkdir(parents=True, exist_ok=True)
    url = datasets["shpc"]["index-page-url"]
    fetch_file(url, dest_filepath)
    return {
        "filepath": dest_filepath,
        "url": url,
    }


def download_shlp_metadata():
    dest_filepath = DEST_DATA_DIR / "shlp" / "metadata.html"
    if dest_filepath.exists():
        return
    dest_filepath.parent.mkdir(parents=True, exist_ok=True)
    url = datasets["shlp"]["index-page-url"]
    fetch_file(url, dest_filepath)
    return {
        "filepath": dest_filepath,
        "url": url,
    }
