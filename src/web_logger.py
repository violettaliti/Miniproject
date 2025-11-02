# imports
import requests
from bs4 import BeautifulSoup
import pandas as pd
from functools import reduce # part of python standard library

# my web crawler identity
headers_default = {
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

def avoid_index_error(row_data, index):
    """
    this function helps safely return the text of a cell and avoid index error
    if the index doesn't exist or the cell is empty, return None
    :return: None or text
    """
    if index < len(row_data):
        txt = row_data[index].get_text(strip = True)
        if txt in {"-", ""}:
            return None
        else:
            return txt
    return None

def scrape_country_cpi_tables(url = wiki_url, headers = headers_default):
    """
    this function scrapes the wiki page 'List of countries by Corruption Perceptions Index'.
    :param url: https://en.wikipedia.org/wiki/List_of_countries_by_Corruption_Perceptions_Index
    :param headers: thi's browser cookies' identity using codersbay laptop
    :return: a pandas dataframe with the country cpi scores over the years
    """
    try:
        # make an http request
        response = requests.get(url, headers = headers, timeout = 5)
        print("\nQuerries URL for scraping:", response.url, "\n")

        if response.status_code == 200:
            print("Connected to the website. Web scraping begins ... ₍^. .^₎⟆ ...\n")
            # print(response.content)

            # raw content to html format
            soup = BeautifulSoup(response.content, "html.parser")
            # print(soup)

            # get all tables on the page (with the wikitable class)
            all_tables = soup.find_all("table", {"class": "wikitable"})
            print(f"Yay ฅ^>⩊<^ฅ found {len(all_tables)} tables in total on this page!\n")

            valid_tables = []

            for table in all_tables:
                ths = [th.get_text(strip = True) for th in table.find_all("th")]
                # print(ths)
                if any("Nation" in h for h in ths): # only take relevant tables which contains 'Nation'
                    valid_tables.append(table)

            if not valid_tables:
                raise Exception("No valid tables found! /ᐠ-˕-マⳊ")
            else:
                print(f"Among the {len(all_tables)} tables on the site, there are {len(valid_tables)} relevant tables!")

            dfs = []

            for idx, table in enumerate(valid_tables, start = 1):
                print(f"\n-------------------------- Scraping table #{idx} -----------------------------\n")

                headers_list = [th.get_text(strip = True) for th in table.find_all("th")]

                # only get relevant column names (rank, country, and years)
                valid_categories = []

                for element in headers_list:
                    if element == "Nation\xa0or\xa0Territory":
                        valid_categories.append("Country")
                    elif element[0] in {"1", "2"}: # if it's a year column header starting with 1 (e.g. 1995) or 2 (2001)
                        valid_categories.append(element[:4]) # take only the first 4 digits belonging to the year
                print(f"Table #{idx} - original categories: {valid_categories}.")

                years = valid_categories[1:]
                years_count = len(years)
                print(f"Table #{idx} - year range: {years[-1]}-{years[0]}, number of year columns: {years_count}.")

                # create dataframe for each valid table
                df = pd.DataFrame(columns = valid_categories)

                table_content = table.find_all("tr")
                for row in table_content[2:]:
                    row_data = row.find_all("td")
                    if not row_data:
                        continue

                    if years_count < 1:
                        print("There is no year data on this table!")

                    # use the avoid_index_error method to avoid rows with fewer data than available columns
                    try:
                        data = {"Country": avoid_index_error(row_data, 1)}
                        for year_index, year in enumerate(years):
                            cell_index = 2 + (year_index * 2)
                            data[year] = avoid_index_error(row_data, cell_index) # this is needed because tables have different numbers of year columns

                        df.loc[len(df)] = data # each iteration adds one row in the df --> Append a new row at the next index position, using the dictionary data to fill columns, and assign NaN to any columns that aren’t specified.
                    except IndexError:
                        continue # just skip rows with no data

                print(f"\nTable #{idx} scraped successfully with {len(df)} rows!\n")
                print(f"The first 5 rows of table #{idx}:\n", df.head(5), "\n")

                dfs.append(df)

            print("----------- Finished scraping all valid tables! ₍^. .^₎⟆ -------------\n")
            return dfs

        else:
            print(f"Something went wrong, I couldn't fetch the requested data /ᐠ-˕-マ. Error status code: {response.status_code}.")
            print(response.content)

    except requests.exceptions.RequestException as e:
        print(f"Something went wrong ૮₍•᷄  ༝ •᷅₎ა --> Error message: {type(e).__name__} - {e}.")

def normalise_cpi_data(tables_to_normalise):
    """
    this function normalises the cpi scores data:
     (i) convert all scores to numeric in df (currently they're str / object)
     (ii) normalise scores for the years before 2012 (when the scores were 0-10)
    :return: scores in numeric datatype, normalised cpi scores (0-100) for scores before 2012
    """
    print("----------- Normalising the data (scores after 2012 is based on 0-100 scale, before 2012 based on 0-10 scale) -----------\n")
    for i, df in enumerate(tables_to_normalise, start = 1):
        year_cols = [col for col in df.columns if str(col).isdigit() and 1995 <= int(col) <= 2024] # identify which columns are year columns

        # convert all years to numeric in df
        df[year_cols] = df[year_cols].apply(pd.to_numeric, errors = "coerce") # errors="coerce": if it cannot be converted, make it NaN

        # normalise values for years before 2012 (table 3, 4, 5)
        if i in {3, 4, 5}:
            df[year_cols] = df[year_cols] * 10 # multiply numeric values by 10 to scale the old CPI scores from 0-10 to 0-100
    print("--------------- The data has been normalised! ₍^. .^₎Ⳋ ---------------\n")
    return tables_to_normalise

def merge_tables_by_country(normalised_tables):
    """
    this function merges the tables by country
    :param normalised_tables
    :return: one merged table
    """
    print(f"----------- Merging the {len(normalised_tables)} tables into one table (on the column 'country') -----------\n")
    # remove duplicate country rows
    cleaned_tables = [df.drop_duplicates(subset = ["Country"]) for df in normalised_tables]

    # merge one after another by Country, keeping all countries
    merged_table = reduce(lambda left, right: pd.merge(left, right, on = "Country", how = "outer"), cleaned_tables)

    # identify year columns and sort by year
    year_cols = [col for col in merged_table.columns if col != "Country"]
    sorted_year_cols = sorted(year_cols, key = int) # key = int: use the integer value of each item as the sort key

    # reorder columns: country, then ASC years
    sorted_merged_table = merged_table[["Country"] + sorted_year_cols]

    print(f"The first 5 rows of the sorted, merged table:\n\n", sorted_merged_table.head(5), "\n")
    print("NaN_values count per year:\n", sorted_merged_table.isna().sum())
    print("\nThe merged table description:\n\n", sorted_merged_table.describe())
    print(f"\n--------------- Finished merging {len(normalised_tables)} tables! ₍^. .^₎Ⳋ -----------------\n")
    return sorted_merged_table

def transform_data(table_to_be_transformed):
    print(f"----------- Transforming the table into a 3NF-compliant dataset! -----------\n")
    transformed_df = table_to_be_transformed.melt(
        id_vars = ["Country"], # column "country" remains the same / fixed
        var_name = "Year", # name of the new column for the former headers (e.g. 1995, 2024)
        value_name = "CPI Score" # name of the new column for the old values (cpi scores)
    )
    transformed_df["Year"] = transformed_df["Year"].astype(int)
    transformed_df["CPI Score"] = transformed_df["CPI Score"].astype(float)

    print(f"The first 5 rows of the transformed, 3NF-compliant table:\n\n", transformed_df.head(5), "\n")
    print("The transformed table description:\n\n", transformed_df.describe())
    print(f"\n--------------- Finished transforming the table! ₍^. .^₎Ⳋ -----------------\n")
    return transformed_df

if __name__ == "__main__":
    print("Hello from web_logger!")
    dfs = scrape_country_cpi_tables()
    normalised_dfs = normalise_cpi_data(dfs)
    sorted_merged_table = merge_tables_by_country(normalised_dfs)
    transformed_table = transform_data(sorted_merged_table)

