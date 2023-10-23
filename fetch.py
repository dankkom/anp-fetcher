import argparse
from pathlib import Path

from anp_fetcher import fetcher, metadata
from anp_fetcher.metadata_gather import gather_shpc_resources_metadata


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output", required=True, type=Path)
    args = parser.parse_args()
    dest_dir = args.output
    for resource in metadata.shlp:
        fetcher.fetch_shlp(resource, dest_dir)
    shpc_resources = gather_shpc_resources_metadata()
    for resource in shpc_resources["datasets"]:
        fetcher.fetch_shpc(resource, dest_dir)
    fetcher.dados_estatisticos(dest_dir)


if __name__ == "__main__":
    main()
