import re
from pathlib import Path

import httpx
from bs4 import BeautifulSoup
from tqdm import tqdm

from . import metadata
from .metadata_gather import gather_shpc_resources_metadata
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


def fetch_shlp(dest_dir: Path):
    for resource in metadata.shlp:
        url = resource["url"]
        dest_filepath = dest_dir / "shlp" / resource["name"]
        if dest_filepath.exists():
            continue
        fetch_file(url, dest_filepath)
        yield {
            "filepath": dest_filepath,
        }


def fetch_shpc(data_dir: Path) -> dict:
    shpc_resources = gather_shpc_resources_metadata()
    for resource in shpc_resources["datasets"]:
        url = resource["url"]
        dest_filepath = get_shpc_filepath(data_dir, resource)
        if dest_filepath.exists() and not resource["dynamic"]:
            continue
        fetch_file(url, dest_filepath)
        yield resource | {"dest_filepath": dest_filepath}


def fetch_shpc_doc(data_dir: Path):
    shpc_resources = gather_shpc_resources_metadata()
    url = shpc_resources["doc"]["url"]
    filename = shpc_resources["doc"]["filename"]
    dest_filepath = data_dir / "shpc" / "[doc]" / filename
    fetch_file(url, dest_filepath)


def dados_estatisticos(dest_dir: Path):
    html_page_url = (
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-estatisticos"
    )
    r = httpx.get(html_page_url)
    soup = BeautifulSoup(r.text, "html.parser")

    def is_file(href: str) -> bool:
        m = re.match(r".*\.(pdf|xls|xlsx|zip)$", href)
        return bool(m)

    links = [
        {
            "href": a["href"],
            "text": a.text.strip(),
            "filename": a["href"].rsplit("/", 1)[1],
        }
        for a in soup.select("a") if is_file(a["href"])
    ]

    for link in links:
        url = link["href"]
        dest_filepath = dest_dir / "dados-estatisticos" / link["filename"]
        if dest_filepath.exists():
            continue
        print(link)
        fetch_file(url, dest_filepath)
