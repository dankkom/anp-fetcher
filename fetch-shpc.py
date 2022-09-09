from anp_fetcher import config, fetcher, scraper, storage


def main():
    r = fetcher.download_shpc_metadata()
    with open(config.DEST_DATA_DIR / "shpc" / "metadata.html") as f:
        html = f.read()
    for link in scraper.scrape_shpc_index(html):
        dest_filepath = storage.get_shpc_filepath(config.DEST_DATA_DIR, link)
        print(link["subset"], link["date"])


if __name__ == "__main__":
    main()
