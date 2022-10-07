from pathlib import Path


def get_shpc_filepath(dest_data_dir: Path, info: dict) -> Path:
    subset = info["subset"]
    year, subyear = info["date"]
    partition = f"{year:04}{subyear:02}"
    extension = info["extension"]
    dest_filename = f"{subset}_{partition}.{extension}"
    dest_filepath = dest_data_dir / "shpc" / subset / dest_filename
    return dest_filepath


def get_shlp_filepath(dest_data_dir: Path, info: dict) -> Path:
    frequency = info["frequency"]
    region = info["region"]
    period = info["period"]
    extension = info["extension"]
    dest_filename = f"{region}-{frequency}-{period}.{extension}"
    dest_filepath = dest_data_dir / "shlp" / f"{region}-{frequency}" / dest_filename
    return dest_filepath


def write_data(data: bytes, filepath: Path):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with filepath.open("wb") as f:
        f.write(data)
