import datetime as dt
import re
import unicodedata

months = {
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


def strip_accents(s):
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )


def clean_resource_name(resource_name: str) -> str:
    return (
        strip_accents(resource_name)
        .strip()
        .lower()
    )


def decode_monthyear(monthyear: str) -> dt.date:
    month, year = clean_resource_name(monthyear).split("/")
    return dt.date(int(year), months[month], 1)


def decode_semesteryear(semesteryear: str) -> dt.date:
    semesteryear = clean_resource_name(semesteryear).replace(".", "")
    m = re.match(r"([12])o sem (\d{4})", semesteryear)
    semester, year = m.groups()
    if semester == "1":
        return dt.date(int(year), 1, 1)
    elif semester == "2":
        return dt.date(int(year), 6, 1)
    else:
        raise ValueError(f"Invalid {semesteryear}")
