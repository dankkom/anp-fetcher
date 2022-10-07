from pathlib import Path

import httpx
from tqdm import tqdm

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
                print(r.headers)
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


def fetch_shpc_dsas_ca(year, month, dest_filepath):
    url = (
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos"
        "/arquivos/shpc/dsas/ca/ca-{year}-{semester:02}.csv"
    ).format(year=year, month=month)
    fetch_file(url, dest_filepath)


def fetch_shpc_dsas_glp(year, month, dest_filepath):
    url = (
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos"
        "/arquivos/shpc/dsas/glp/glp-{year}-{semester:02}.csv"
    ).format(year=year, month=month)
    fetch_file(url, dest_filepath)


def fetch_shpc_dsan_precos_diesel_gnv(year, month, dest_filepath):
    url = (
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos"
        "/arquivos/shpc/dsan/{year}/precos-diesel-gnv-{month:02}.csv"
    ).format(year=year, month=month)
    fetch_file(url, dest_filepath)


def fetch_shpc_dsan_gasolina_etanol(year, month, dest_filepath):
    url = (
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos"
        "/arquivos/shpc/dsan/{year}/{year}-{month:02}-gasolina-etanol.csv"
    ).format(year=year, month=month)
    fetch_file(url, dest_filepath)


def fetch_shpc_dsan_precos_glp(year, month, dest_filepath):
    url = (
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos"
        "/arquivos/shpc/dsan/{year}/precos-glp-{month:02}.csv"
    ).format(year=year, month=month)
    fetch_file(url, dest_filepath)


def fetch_shlp(dest_dir):
    for resource in datasets["shlp"]["resources"]:
        url = resource["url"]
        dest_filepath = dest_dir / resource["name"]
        fetch_file(url, dest_filepath)
