import datetime as dt
from pathlib import Path


def get_shpc_filepath(data_dir: Path, dataset_info: dict) -> Path:
    dataset = dataset_info["dataset"]
    if subset := dataset_info["subset"]:
        dataset += "-" + subset
    if dataset_info["grouping"] == "weekly":
        date_partition = f"@{dt.datetime.utcnow():%Y%mW%U}"
    else:
        year, subyear = dataset_info["date"]
        date_partition = f"{year:04}{subyear:02}"
    extension = dataset_info["file_extension"]
    dest_filename = f"{dataset}_{date_partition}.{extension}"
    dest_filepath = data_dir / "shpc" / dataset / dest_filename
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
