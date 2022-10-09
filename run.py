import argparse
from pathlib import Path

from anp_fetcher import fetcher


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output", required=True, type=Path)
    args = parser.parse_args()
    dest_dir = args.output
    for file in fetcher.fetch_shlp(dest_dir):
        pass
    for file in fetcher.fetch_shpc(dest_dir):
        pass


if __name__ == "__main__":
    main()
