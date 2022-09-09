import re

from bs4 import BeautifulSoup


def scrape_shlp_index(html):
    soup = BeautifulSoup(html, "lxml")
    content_core = soup.select_one("#content-core")
    links = map(
        lambda a: {"url": a["href"], "name": a.text},
        filter(
            lambda a: a["href"].endswith(".xlsx") or a["href"].endswith(".xlsb"),
            content_core.select("a"),
        ),
    )
    for link in links:
        filename = link["url"].rsplit("/", maxsplit=1)[1]
        stem, extension = filename.split(".")
        frequency, region, period = stem.split("-", maxsplit=2)
        match region:
            case "regioes":
                region = "regiao"
            case "estados":
                region = "estado"
            case "municipios":
                region = "municipio"
        link["frequency"] = frequency
        link["region"] = region
        link["period"] = period
        link["extension"] = extension
        yield link


def scrape_shpc_index(html):
    soup = BeautifulSoup(html, "lxml")
    content_core = soup.select_one("#content-core")
    links = map(
        lambda a: {"url": a["href"], "name": a.text},
        filter(
            lambda a: a["href"].endswith(".csv"),
            content_core.select("a"),
        ),
    )
    for link in links:
        filename = link["url"].rsplit("/", maxsplit=1)[1]
        stem, extension = filename.split(".")
        if m := re.match(r"^(ca)-(\d{4})-(0[12])$", stem):
            _, year, semester = m.groups()
            subset = "combustiveis-automotivos"
            date = (year, semester)
        elif m := re.match(r"^(glp)-(\d{4})-(0[12])$", stem):
            _, year, semester = m.groups()
            subset = "glp"
            date = (year, semester)
        elif m := re.match(r"^(precos-glp)-([01]\d)$", stem):
            _, month = m.groups()
            subset = "glp-p13"
            year = link["url"].rsplit("/", maxsplit=2)[1]
            date = (year, month)
        elif m := re.match(r"^(precos-diesel-gnv)-([01]\d)$", stem):
            _, month = m.groups()
            subset = "oleo-diesel-s-500-s-10-gnv"
            year = link["url"].rsplit("/", maxsplit=2)[1]
            date = (year, month)
        elif m := re.match(r"^(precos-gasolina-etanol)-([01]\d)$", stem):
            _, month = m.groups()
            subset = "etanol-gasolina-comum"
            year = link["url"].rsplit("/", maxsplit=2)[1]
            date = (year, month)
        else:
            continue
        link["subset"] = subset
        link["date"] = date
        link["extension"] = extension
        yield link
