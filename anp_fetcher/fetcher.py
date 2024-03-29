from pathlib import Path
from typing import Any

import httpx
from tqdm import tqdm

from .metadata_gather import gather_dados_estatisticos_metadata, gather_shpc_resources_metadata
from .storage import get_shpc_filepath


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
                    desc=dest_filepath.name,
                )
                with dest_filepath.open("wb") as f:
                    for chunk in r.iter_bytes():
                        f.write(chunk)
                        progress.update(len(chunk))
                progress.close()
                break
        except httpx.ProtocolError as e:
            print("\nProtocol error", e)
        except (httpx.ConnectTimeout, httpx.ConnectError) as e:
            print("\nConnection timeout", e)
            dest_filepath.unlink(missing_ok=True)
            break


def fetch_shlp(resource: dict[str, Any], dest_dir: Path):
    url = resource["url"]
    dest_filepath = dest_dir / "shlp" / resource["name"]
    if dest_filepath.exists():
        return {"filepath": dest_filepath}
    fetch_file(url, dest_filepath)
    return {
        "filepath": dest_filepath,
    }


def fetch_shpc(resource: dict[str, Any], data_dir: Path) -> dict:
    url = resource["url"]
    dest_filepath = get_shpc_filepath(data_dir, resource)
    if dest_filepath.exists():
        return resource | {"dest_filepath": dest_filepath}
    fetch_file(url, dest_filepath)
    return resource | {"dest_filepath": dest_filepath}


def fetch_shpc_doc(data_dir: Path):
    shpc_resources = gather_shpc_resources_metadata()
    url = shpc_resources["doc"]["url"]
    filename = shpc_resources["doc"]["filename"]
    dest_filepath = data_dir / "shpc" / "[doc]" / filename
    fetch_file(url, dest_filepath)


def dados_estatisticos(dest_dir: Path):
    links = gather_dados_estatisticos_metadata()
    for link in links:
        url = link["href"]
        dest_filepath = dest_dir / "dados-estatisticos" / link["filename"]
        if dest_filepath.exists():
            continue
        print(link)
        fetch_file(url, dest_filepath)
