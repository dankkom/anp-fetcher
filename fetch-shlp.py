from anp_fetcher import config, fetcher, scraper


def main():
    r = fetcher.download_shlp_metadata()
    with open(config.DEST_DATA_DIR / "shlp" / "metadata.html") as f:
        html = f.read()
    for link in scraper.scrape_shlp_index(html):
        print(link)


if __name__ == "__main__":
    main()
