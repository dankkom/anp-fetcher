import os
from pathlib import Path

CKAN_PACKAGE_SHOW_FMT = "https://dados.gov.br/api/3/action/package_show?id={dataset_id}"
DATA_DIR = Path(os.getenv("DATA_DIR"))
DEST_DATA_DIR = DATA_DIR / "raw" / "anp"
DEST_DATA_DIR.mkdir(parents=True, exist_ok=True)
