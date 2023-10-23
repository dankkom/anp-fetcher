import re
from typing import Any

import httpx
from bs4 import BeautifulSoup


def parse_date_semester(txt: str) -> tuple[int]:
    m = re.match(r"(1|2)º semestre de (\d{4})", txt)
    semester, year = m.groups()
    return int(year), int(semester)


def parse_date_monthly(txt: str) -> tuple[int]:
    months = {
        "janeiro": 1,
        "fevereiro": 2,
        "março": 3,
        "abril": 4,
        "maio": 5,
        "junho": 6,
        "julho": 7,
        "agosto": 8,
        "setembro": 9,
        "outubro": 10,
        "novembro": 11,
        "dezembro": 12,
    }
    pattern = f"({'|'.join(months.keys())})" + r" de (\d{4})"
    m = re.match(pattern, txt.lower())
    month, year = m.groups()
    return int(year), months[month]


def parse_url(url: str) -> dict:
    filename = url.rsplit("/", maxsplit=1)[1]
    stem, extension = filename.split(".")
    return {
        "filename": filename,
        "file_stem": stem,
        "file_extension": extension,
    }


def gather_shpc_resources_metadata() -> dict[str, str | list[dict[str, Any]]]:
    """Gathers metadata links of SHPC datasets. Returns a list of dictionaries
    with informations of each data file link.

    Example dataset:

    {
        'dataset': 'glp-p13',
        'date': (None, None),
        'extension': 'csv',
        'filename': 'ultimas-4-semanas-glp.csv',
        'grouping': 'weekly',
        'link_text': 'GLP P13',
        'stem': 'ultimas-4-semanas-glp',
        'subset': None,
        'url': 'https://www.gov.br/anp/pt-br/.../ultimas-4-semanas-glp.csv',
    }
    """
    url = (
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos"
        "/serie-historica-de-precos-de-combustiveis"
    )
    r = httpx.get(url)
    soup = BeautifulSoup(r.text, "lxml")
    content_core = soup.select_one("#content-core")
    link_lists = content_core.select("ul")
    doc_link = link_lists[0].select_one("a")
    doc = {
        "link_text": doc_link.text.strip(),
        "url": doc_link["href"],
        **parse_url(doc_link["href"]),
    }

    # Dados semanais agrupados por semestre
    combustiveis_automotivos_list = link_lists[1].select("a")
    combustiveis_automotivos = [
        {
            "dataset": "combustiveis-automotivos",
            "subset": None,
            "grouping": "semester",
            "dynamic": False,
            "url": a["href"],
            "link_text": a.text.strip(),
            "date": parse_date_semester(a.text.strip()),
            **parse_url(a["href"]),
        }
        for a in combustiveis_automotivos_list
    ]

    glp_p13_list = link_lists[2].select("a")
    glp_p13 = [
        {
            "dataset": "glp-p13",
            "subset": None,
            "grouping": "semester",
            "dynamic": False,
            "url": a["href"],
            "link_text": a.text.strip(),
            "date": parse_date_semester(a.text.strip()),
            **parse_url(a["href"]),
        }
        for a in glp_p13_list
    ]

    # Dados semanais agrupados mensalmente
    diesel_gnv_list = link_lists[3].select("a")
    diesel_gnv = [
        {
            "dataset": "combustiveis-automotivos",
            "subset": "diesel-gnv",
            "grouping": "monthly",
            "dynamic": False,
            "url": a["href"],
            "link_text": a.text.strip(),
            "date": parse_date_monthly(a.text.strip()),
            **parse_url(a["href"]),
        }
        for a in diesel_gnv_list
    ]

    gasolina_etanol_list = link_lists[4].select("a")
    gasolina_etanol = [
        {
            "dataset": "combustiveis-automotivos",
            "subset": "gasolina-etanol",
            "grouping": "monthly",
            "dynamic": False,
            "url": a["href"],
            "link_text": a.text.strip(),
            "date": parse_date_monthly(a.text.strip()),
            **parse_url(a["href"]),
        }
        for a in gasolina_etanol_list
    ]

    glp_list = link_lists[5].select("a")
    glp = [
        {
            "dataset": "glp-p13",
            "subset": None,
            "grouping": "monthly",
            "dynamic": False,
            "url": a["href"],
            "link_text": a.text.strip(),
            "date": parse_date_monthly(a.text.strip()),
            **parse_url(a["href"]),
        }
        for a in glp_list
    ]

    names = {
        "Óleo Diesel (S-500 e S-10) + GNV": (
            "combustiveis-automotivos",
            "diesel-gnv",
        ),
        "Etanol Hidratado + Gasolina C": (
            "combustiveis-automotivos",
            "gasolina-etanol",
        ),
        "GLP P13": ("glp-p13", None),
    }
    ultimas_4_semanas_list = link_lists[6].select("a")
    ultimas_4_semanas = [
        {
            "dataset": names[a.text.strip()][0],
            "subset": names[a.text.strip()][1],
            "grouping": "weekly",
            "dynamic": True,
            "url": a["href"],
            "link_text": a.text.strip(),
            "date": (None, None),
            **parse_url(a["href"]),
        }
        for a in ultimas_4_semanas_list
    ]
    return {
        "doc": doc,
        "datasets": (
            combustiveis_automotivos
            + glp_p13
            + diesel_gnv
            + gasolina_etanol
            + glp
            + ultimas_4_semanas
        ),
    }


def gather_dados_estatisticos_metadata() -> list[dict[str, Any]]:
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

    return links
