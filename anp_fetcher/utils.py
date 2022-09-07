import re
import unicodedata


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
