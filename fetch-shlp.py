from pathlib import Path

from anp_fetcher import fetcher


def main():
    fetcher.fetch_shlp(Path("/home/dk/data/raw/anp"))


if __name__ == "__main__":
    main()
