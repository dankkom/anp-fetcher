from pathlib import Path


def get_shpc_filepath(dest_data_dir: Path, info: dict) -> Path:
    subset = info["subset"]
    year, subyear = info["date"]
    partition = f"{year:04}{subyear:02}"
    extension = info["extension"]
    dest_filename = f"{subset}_{partition}.{extension}"
    dest_filepath = dest_data_dir / "shpc" / subset / dest_filename
    return dest_filepath
