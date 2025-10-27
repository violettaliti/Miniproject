import os
import requests
from bs4 import BeautifulSoup
import time

# my web crawler identity
headers = {
    "User-Agent": (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:143.0) "
        "Gecko/20100101 Firefox/143.0 "
    "(compatible; violettalitiScraper/1.0; +https://github.com/violettaliti)"
    )
}

# website: Transparency International
# data: Corruption Perceptions Index

# URLs of all countries' 2024 corruption perceptions indices
sitemap_url = "https://www.transparency.org/en/sitemaps-1-section-country-1-sitemap.xml"

def country_cpi_2024(url = sitemap_url, headers=headers):
    try:
        # make an http request
        response = requests.get(url, headers=headers, timeout=5)
        print("\nQuerries URL for scraping:", response.url, "\n")

        if response.status_code == 200:
            print("Connected to the website. Web scraping begins...\n")
            # print(response.content)

            # raw content to html format
            soup = BeautifulSoup(response.content, "html.parser")
            # print(soup)

            # get html table
            table = soup.find_all("table")
            print(table)

            # get categories / column names
            # use .strip() to remove extra whitespaces and specified characters like \n
            categories = [val.text.strip() for val in table.find_all("th")]
            print(categories)

        else:
            print(f"Something went wrong, I couldn't fetch the requested data /ᐠ-˕-マ. Error status code: {response.status_code}.")
            print(response.content)

    except requests.exceptions.RequestException as e:
        print(f"Something went wrong ૮₍•᷄  ༝ •᷅₎ა --> Error message: {type(e).__name__} - {e}.")


if __name__ == "__main__":
    print("Hello from web_logger!")
    country_cpi_2024("https://en.wikipedia.org/wiki/Corruption_Perceptions_Index#2024_scores")
