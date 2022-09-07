import re

from .utils import clean_resource_name

MONTHS = {
    "janeiro": 1,
    "fevereiro": 2,
    "marco": 3,
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

datasets = {
    # Série Histórica de Preços de Combustíveis
    "shpc": {
        "index-page-url": "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis",
        "subsets": {
            "glp p13": {
                "name": "glp-p13",
            },
            "etanol + gasolina comum": {
                "name": "etanol-gasolina-comum",
            },
            "oleo diesel s-500 e s-10 + gnv": {
                "name": "oleo-diesel-s-500-s-10-gnv",
            },
            "glp": {
                "name": "glp",
            },
            "combustiveis automotivos": {
                "name": "combustiveis-automotivos",
            },
        },
    },
    "shlp": {
        "index-page-url": "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/serie-historica-do-levantamento-de-precos",
    },
}


def parse_shpc_metadata(shpc_metadata):
    subsets_shpc = datasets["shpc"]["subsets"]
    months = "|".join(MONTHS.keys())
    for resource in shpc_metadata["result"]["resources"]:
        url = resource["url"]
        name = clean_resource_name(resource["name"])
        extension = resource["format"].lower()
        if extension == "csv":
            info = {
                "url": url,
                "extension": extension,
                "resource-name": name,
            }
            if m := re.match(r"([12])o(|\.) sem (\d{4}) - ([\w ]+)", name):
                semester, _, year, subset_name = m.groups()
                info.update(
                    {
                        "subset": subsets_shpc[subset_name]["name"],
                        "subset-name": subset_name,
                        "date": (int(year), int(semester)),
                        "frequency": "semester",
                    },
                )
            elif m := re.match(r"(.+) - (" + months + r")/(\d{4})", name):
                subset_name, month, year = m.groups()
                info.update(
                    {
                        "subset": subsets_shpc[subset_name]["name"],
                        "subset-name": subset_name,
                        "date": (int(year), MONTHS[month]),
                        "frequency": "month",
                    },
                )
            yield info
