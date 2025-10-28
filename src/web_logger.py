# imports

import os # part of python standard library
import requests
from bs4 import BeautifulSoup
import time # part of python standard library
import pandas as pd
import re # part of python standard library
from io import StringIO # part of python standard library

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

# official URL of all countries' 2024 corruption perceptions indices on the Transparency International website
# sitemap_url = "https://www.transparency.org/en/sitemaps-1-section-country-1-sitemap.xml"
# This site blocks scraping so I've switched to Wikipedia instead:
wiki_url = "https://en.wikipedia.org/wiki/List_of_countries_by_Corruption_Perceptions_Index"

def scrape_country_cpi_tables(url = wiki_url, headers = headers):
    """
    this function scrapes the wiki page 'List of countries by Corruption Perceptions Index'.
    :param url: https://en.wikipedia.org/wiki/List_of_countries_by_Corruption_Perceptions_Index
    :param headers: thi's browser cookies' identity using codersbay laptop
    :return: a pandas dataframe with the country cpi scores over the years
    """
    try:
        # make an http request
        response = requests.get(url, headers=headers, timeout=5)
        print("\nQuerries URL for scraping:", response.url, "\n")

        if response.status_code == 200:
            print("Connected to the website. Web scraping begins ... ₍^. .^₎⟆ ...\n")
            # print(response.content)

            # raw content to html format
            soup = BeautifulSoup(response.content, "html.parser")
            # print(soup)

            # get all tables on the page (with the wikitable class)
            all_tables = soup.find_all("table", {"class": "wikitable"})
            print(f"Yay ฅ^>⩊<^ฅ found {len(all_tables)} tables in total on this page!")

            valid_tables = []

            for table in all_tables:
                ths = [th.get_text(strip=True) for th in table.find_all("th")]
                # print(ths)
                if any("Nation" in h for h in ths): # only take relevant tables which contains 'Nation'
                    valid_tables.append(table)

            if not valid_tables:
                raise Exception("No valid tables found! /ᐠ-˕-マⳊ")
            else:
                print(f"Among the {len(all_tables)} tables on the site, there are {len(valid_tables)} relevant tables! 𖹭")

            for idx, table in enumerate(valid_tables, start = 1):
                categories = [th.get_text(strip=True) for th in table.find_all("th")] # get column names
                print(f"Table #{idx} - original categories: {categories}.")

                # create dataframe for each valid table
                df = pd.DataFrame(columns=categories)

                table_content = table.find_all("tr")

                countries_list = []
                year_2024_scores = []
                year_2023_scores = []
                year_2022_scores = []
                year_2021_scores = []
                year_2020_scores = []

                for row in table_content[2:]:
                    row_data = row.find_all("td")
                    countries_list.append(row_data[1].text)
                    year_2024_scores.append(row_data[2].text)
                    year_2023_scores.append(row_data[4].text)
                    year_2022_scores.append(row_data[6].text)
                    #print(row_data[6].text)

                    year_2021_scores.append(row_data[8].text)
                    print(row_data[8].text)

                    year_2020_scores.append(row_data[10].text)

                print(df.to_string())

                    # individual_data = [data.text.strip() for data in row_data]
                    # print(individual_data)

                    # append datarow at the end
                    # len_df = len(df)
                    # df.loc[len_df] = individual_data

                # print(df.to_string())

                """
                for row in table.find_all("tr")[1:]:  # skip header row
                    # get all header/data cells in this row
                    cells = row.find_all(["th", "td"])

                    # extract text from each cell (THIS replaces row_data.text)
                    values = [c.get_text(" ", strip=True) for c in cells]

                    # pad/trim to header length so assignment doesn’t crash
                    if len(values) < len(categories):
                        values += [""] * (len(categories) - len(values))
                    elif len(values) > len(categories):
                        values = values[:len(categories)]

                    df.loc[len(df)] = values

                print(df.head(), "\n")
                
                """

                """
                # turn the html table into a pandas dataframe
                df = pd.read_html(StringIO(str(table)))[0]

                # rename the 'nation or territory' column to 'country'
                # for col in df.columns:
                #    if 'Nation' in col or 'Territory' in col:
                #        df = df.rename(columns = {col: "Country"})
                #        break # no need to check other cols after having found the 'nation' col

                year_cols = [col for col in df.columns if re.fullmatch(r"(19|20)\d{2}", str(col))] # only take cols with valid years

                cols_kept = ["Nation\xa0or\xa0Territory"] + year_cols
                df = df[cols_kept]
                print(f"Columns kept: {cols_kept}")

                # drop unwanted 'Score', 'Δ[i]' or reference-number-like cols
                df = df.loc[:, ~df.columns.str.contains(r"Score|Δ|\[|\]", regex=True)]

                print(f"Table #{idx} -  Cleaned columns: {list(df.columns)}.")

                print(df.head(), "\n")

                """







            # first table - CPI scores for 2020-2024: tables[2]
            # print(tables)

            # second table - CPI scores for 2012-2019: tables[3]

            # third table - CPI scores for 2003-2011: tables[5]

            # fourth table - CPI scores for 1998-2002: tables[6]

            # fifth table - CPI scores for 1995-1997: tables[8]

            # get categories / column names
            # use .strip() to remove extra whitespaces and specified characters like \n
            # categories = [val.text.strip() for val in tables[0].find("th")]
            # print(categories)

        else:
            print(f"Something went wrong, I couldn't fetch the requested data /ᐠ-˕-マ. Error status code: {response.status_code}.")
            print(response.content)

    except requests.exceptions.RequestException as e:
        print(f"Something went wrong ૮₍•᷄  ༝ •᷅₎ა --> Error message: {type(e).__name__} - {e}.")

def normalise_cpi_data():
    pass

if __name__ == "__main__":
    print("Hello from web_logger!")
    scrape_country_cpi_tables()
