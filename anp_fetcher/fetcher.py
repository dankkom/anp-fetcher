from pathlib import Path
import re

import httpx
from tqdm import tqdm

from .metadata import datasets
from .config import CKAN_PACKAGE_SHOW_FMT
from . import utils


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


def fetch_shpc(dest_dir: Path):
    dataset_id = "serie-historica-de-precos-de-combustiveis-por-revenda"
    url = CKAN_PACKAGE_SHOW_FMT.format(dataset_id=dataset_id)
    r = httpx.get(url)
    metadata = r.json()
    resources = metadata["result"]["resources"]
    for resource in resources:
        name = resource["name"]
        if name.startswith("GLP P13"):
            dataset = "glp-p13"
            m = re.match(r"^GLP P13 \- (\w+\/\d{4})$", name)
            monthyear, = m.groups()
            period = utils.decode_monthyear(monthyear)
        elif name.startswith("Etanol + Gasolina Comum"):
            dataset = "etanol-gasolina-comum"
            m = re.match(r"^Etanol \+ Gasolina Comum \- (\w+\/\d{4})$", name)
            monthyear, = m.groups()
            period = utils.decode_monthyear(monthyear)
        elif name.startswith("Óleo Diesel S-500 e S-10 + GNV"):
            dataset = "oleo-diesel-gnv"
            m = re.match(r"^Óleo Diesel S\-500 e S\-10 \+ GNV \- (\w+\/\d{4})$", name)
            monthyear, = m.groups()
            period = utils.decode_monthyear(monthyear)
        elif name.endswith("GLP"):
            dataset = "glp"
            m = re.match(r"^([12]o(\.|) Sem (\d{4})) - GLP$", name)
            semesteryear, *_ = m.groups()
            period = utils.decode_semesteryear(semesteryear)
        elif name.endswith("Combustíveis Automotivos"):
            dataset = "combustiveis-automotivos"
            m = re.match(r"^([12]o(\.|) Sem (\d{4})) - Combustíveis Automotivos$", name)
            semesteryear, *_ = m.groups()
            period = utils.decode_semesteryear(semesteryear)
        else:
            continue
        url = resource["url"]
        filename = url.rsplit("/", maxsplit=1)[1]
        dest_filepath = dest_dir / dataset / filename
        if dest_filepath.exists():
            continue
        fetch_file(url, dest_filepath)


def fetch_shlp(dest_dir: Path):
    for resource in datasets["shlp"]["resources"]:
        url = resource["url"]
        dest_filepath = dest_dir / resource["name"]
        fetch_file(url, dest_filepath)
