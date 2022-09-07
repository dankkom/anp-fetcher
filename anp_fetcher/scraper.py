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
