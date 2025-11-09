# imports
import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
import pandas as pd
import numpy as np
from functools import reduce # part of python standard library
import os # part of python standard library
from save_data import DBPostgres, DatabaseError
import psycopg
from psycopg import sql

# my web crawler identity
headers_default = {
    "User-Agent": (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:143.0) "
        "Gecko/20100101 Firefox/143.0 "
    "(compatible; violettalitiScraper/1.0; +https://github.com/violettaliti)"
    )
}

#######################################
# website: Transparency International
# data: Corruption Perceptions Index
#######################################

# official URL of all countries' 2024 corruption perceptions indices on the Transparency International website
# sitemap_url = "https://www.transparency.org/en/sitemaps-1-section-country-1-sitemap.xml"
# This site blocks scraping, so I've switched to Wikipedia instead:
wiki_url = "https://en.wikipedia.org/wiki/List_of_countries_by_Corruption_Perceptions_Index"

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

        if response.status_code != 200:
            print(f"Something went wrong, I couldn't fetch the requested data /ᐠ-˕-マ. Error status code: {response.status_code}.")
            print(response.content)
            return []

        print("Connected to the website. Web scraping begins ... ₍^. .^₎⟆ ...\n")
        # raw content to html format
        soup = BeautifulSoup(response.content, "html.parser")

        # get all tables on the page (with the wikitable class)
        all_tables = soup.find_all("table", {"class": "wikitable"})
        print(f"Yay ฅ^>⩊<^ฅ found {len(all_tables)} tables in total on this page!\n")

        valid_tables = []

        for table in all_tables:
            ths = [th.get_text(strip = True) for th in table.find_all("th")]
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
            if years_count < 1:
                print("There is no year data on this table!")
            print(f"Table #{idx} - year range: {years[-1]}-{years[0]}, number of year columns: {years_count}.")

            # create dataframe for each valid table
            df = pd.DataFrame(columns = valid_categories)

            table_content = table.find_all("tr")
            for row in table_content[2:]:
                row_data = row.find_all("td")
                if not row_data:
                    continue

                try:
                    # country name as text
                    country_td = row_data[1] if len(row_data) > 1 else None
                    country_name = country_td.get_text(" ", strip = True) if isinstance(country_td, Tag) else None
                    data = {"Country": country_name}

                    # get cpi scores for each year
                    current_year_index = 0
                    skip_next = False
                    for col_count, col in enumerate(row_data):
                        if col_count < 2 or skip_next:
                            skip_next = False
                            continue
                        col_text = col.get_text()
                        if col_text != "—\n":
                            year = years[current_year_index]
                            current_year_index += 1
                            data[year] = col_text
                            skip_next = True
                        else:
                            year = years[current_year_index]
                            current_year_index += 1
                            data[year] = None

                    # only append if the row has some valid year data
                    if any(v for k, v in data.items() if k != "Country"):
                        df.loc[len(df)] = data # each iteration adds one row in the df --> Append a new row at the next index position, using the dictionary data to fill columns, and assign NaN to any columns that aren’t specified.
                except IndexError:
                    continue # just skip rows with no data

            print(f"\nTable #{idx} scraped successfully with {len(df)} rows!\n")
            print(f"The first 5 rows of table #{idx}:\n", df.head(5), "\n")

            dfs.append(df)

        print("----------- Finished scraping all valid tables! ₍^. .^₎⟆ -------------\n")
        return dfs

    except requests.exceptions.RequestException as e:
        print(f"Something went wrong ૮₍•᷄  ༝ •᷅₎ა --> Error message: {type(e).__name__} - {e}.")
        return []

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

def transform_and_clean_data(table_to_be_transformed):
    """
    this function transform and clean the scraped data
    :param table_to_be_transformed
    :return: transformed and cleaned rows
    """
    print(f"----------- Transforming and cleaning the table into a 3NF-compliant dataset! -----------\n")
    transformed_df = table_to_be_transformed.melt(
        id_vars = ["Country"], # column "country" remains the same / fixed
        var_name = "Year", # name of the new column for the former headers (e.g. 1995, 2024)
        value_name = "CPI Score" # name of the new column for the old values (cpi scores)
    )
    # enforce types
    transformed_df["Year"] = transformed_df["Year"].astype(int)
    transformed_df["Country"] = transformed_df["Country"].str.strip()

    # sanity check
    # os.makedirs("data", exist_ok = True)  # creates data/ if it doesn’t exist
    # csv_path = "data/cpi_fixed_transformed.csv"
    # transformed_df.to_csv(csv_path, index = False, encoding = "utf-8")
    # print(f"Saved transformed (not cleaned - so NaNs included) CPI data to {csv_path}.\n")

    # drop NaN scores (so that the data saved into db is cleaned)
    transformed_cleaned_df = transformed_df.dropna(subset = ["CPI Score"])

    print(f"The first 5 rows of the cleaned, transformed, 3NF-compliant table (NaNs removed):\n\n", transformed_cleaned_df.head(5), "\n")
    print("The cleaned, transformed table description:\n\n", transformed_cleaned_df.describe())
    print(f"\n--------------- Finished transforming and cleaning the table! ₍^. .^₎Ⳋ -----------------\n")

    # replace NaN with None for postgreSQL
    cpi_db = transformed_cleaned_df.replace({np.nan: None})
    # turn the df into a list of tuples for saving into the db later
    cpi_rows = list(cpi_db.itertuples(index = False, name = None))

    return cpi_rows

#######################################
# website:
# data: World Happiness Report
#######################################
def get_world_happiness_scores(url):
    """
    this function scrapes the world happiness report and get the world happiness scores
    :param url
    :return: world happiness scores as list of tuples
    """
    df = pd.read_excel(url)
    # columns include: year, rank, country name, score, etc.
    cols = [col for col in df.columns]
    rename_map = {}
    for col in cols:
        lowercase_col = col.lower()
        if "country" in lowercase_col: rename_map[col] = "country_name"
        elif lowercase_col.startswith("year"): rename_map[col] = "year"
        elif "life evaluation" in lowercase_col or "ladder score" in lowercase_col or "score" == lowercase_col:
            rename_map[col] = "happiness_score"
    world_happiness_df = df.rename(columns = rename_map)
    cleaned_world_happiness_df = world_happiness_df[["country_name","year","happiness_score"]].dropna(subset = ["country_name", "year"])

    print(f"The first 5 rows of the world happiness df:\n\n", cleaned_world_happiness_df.head(), "\n")
    print("NaN_values count:\n", cleaned_world_happiness_df.isna().sum())
    print("\nThe world happiness df's description:\n\n", cleaned_world_happiness_df.describe())

    # replace NaN with None for postgreSQL
    world_happiness_db = cleaned_world_happiness_df.replace({np.nan: None})
    # turn the df into a list of tuples for saving into the db later
    world_happiness_rows = list(world_happiness_db.itertuples(index = False, name = None))

    return world_happiness_rows

#######################################
# Save / persist to db
#######################################
class WebDB(DBPostgres):
    """child class of DBPostgres"""
    def add_data_to_staging_cpi(self, data: list, table_name: str = "staging_cpi_raw"):
        """persist acquired data into db"""
        if not data:
            print("There is no CPI data to add to the database. /ᐠ-˕-マ\n")
            return

        query = sql.SQL("""
                        INSERT INTO {} (country_name, year, cpi_score)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (country_name, year) DO NOTHING;
                        """).format(sql.Identifier(table_name))
        # on conflict do nothing to prevent throwing errors and creating duplicates

        try:
            self._executemany(query, data)
            print(f"Successfully added or updated {len(data)} raw rows into '{table_name}' ദ്ദി（•˕•マ.ᐟ")
        except (Exception, psycopg.DatabaseError) as e:
            self.connection.rollback()
            raise DatabaseError(f"Something went wrong with adding the CPI data to the table '{table_name}'. Error type: {type(e).__name__}, error message: '{e}'.")

    def get_cpi_country_info(self, country_names, start_year, end_year):
        """
        fetch and display info for one or more countries (case-insensitive)
        - country_names can be a single string: "Austria, germany" or a list: ["Austria", "gErManY"]
        - update 'docker compose', service 'app_base' environment var COUNTRIES_OF_INTEREST to include or remove any countries to be displayed
        - years period includes the start and end year
        - update 'docker compose', service 'app_base' environment vars START_YEAR_OF_INTEREST and END_YEAR_OF_INTEREST to change the year period to be displayed
        :param country_names, start_year, end_year
        :return: country CPI scores info
        """
        try:
            # turn input into a list of strings in case it is a string
            if isinstance(country_names, str):
                country_names = [name.strip() for name in country_names.split(",")]
            if not country_names:
                print("No countries provided.")
                return
            print(f"\n--- Printing Corruption Perception Index (CPI) scores from {start_year} to {end_year} for the following countries of interest: {', '.join(country_names)} ---")
            print("--- To update or change the countries and/or years of interest, please update 'docker compose' - service 'app_base' environment ---")

            for country_idx, country_name in enumerate(country_names, start = 1):
                print(f"\n{country_idx}. CPI scores of '{country_name}' from {start_year} to {end_year} is:")
                self.cursor.execute("SELECT * FROM staging_cpi_raw WHERE country_name ILIKE %s AND year BETWEEN %s AND %s ORDER BY year;", (f"%{country_name}%", start_year, end_year))
                country_rows = self.cursor.fetchall()
                if not country_rows:
                    print(f"No CPI info found for: {country_name}.")
                    continue

                previous_year_score = None
                for row in country_rows:
                    if previous_year_score is None:
                        print(f"- {row[1]}: {row[2]}")
                    else:
                        score_change = row[2] - previous_year_score
                        print(f"- {row[1]}: {row[2]} | Score change: {round(score_change, 1)}")
                    previous_year_score = row[2]
        except (Exception, psycopg.DatabaseError) as e:
            raise DatabaseError(f"Something went wrong with getting the CPI info of '{', '.join(country_names)}'. Error type: {type(e).__name__}, error message: '{e}'.")

    def add_data_to_staging_world_happiness_report(self, data: list, table_name: str = "staging_world_happiness_report"):
        """persist acquired data into db"""
        if not data:
            print("There is no world happiness data to add to the database. /ᐠ-˕-マ\n")
            return

        query = sql.SQL("""
                        INSERT INTO {} (country_name, year, happiness_score)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (country_name, year) DO NOTHING;
                        """).format(sql.Identifier(table_name))
        # on conflict do nothing to prevent throwing errors and creating duplicates

        try:
            self._executemany(query, data)
            print(f"Successfully added or updated {len(data)} raw rows into '{table_name}' ദ്ദി（•˕•マ.ᐟ")
        except (Exception, psycopg.DatabaseError) as e:
            self.connection.rollback()
            raise DatabaseError(f"Something went wrong with adding the world happiness data to the table '{table_name}'. Error type: {type(e).__name__}, error message: '{e}'.")

#######################################
# Run the web crawlers
#######################################
if __name__ == "__main__":
    print("Hello from web_logger!")
    # scraping CPI info
    dfs = scrape_country_cpi_tables()
    normalised_dfs = normalise_cpi_data(dfs)
    sorted_merged_table = merge_tables_by_country(normalised_dfs)
    cpi_data = transform_and_clean_data(sorted_merged_table)
    web_db = WebDB()
    web_db.add_data_to_staging_cpi(cpi_data)

    names = os.getenv("COUNTRIES_OF_INTEREST", "Austria, Germany").strip()
    start_year = os.getenv("START_YEAR_OF_INTEREST", "2000")
    end_year = os.getenv("END_YEAR_OF_INTEREST", "2024")

    if names and start_year and end_year:
        web_db.get_cpi_country_info(names, start_year, end_year)
    else:
        print("\n--- Printing CPI scores for the countries of interest: Not a single country of interest was given ^. .^₎⟆ ---")

    # scraping world happiness info
    # url found for WHR 2025 “Data for Figure 2.1” (https://www.worldhappiness.report/data-sharing/)
    xlsx_url = "https://files.worldhappiness.report/WHR25_Data_Figure_2.1v3.xlsx"

    world_happiness_rows = get_world_happiness_scores(xlsx_url)
    web_db.add_data_to_staging_world_happiness_report(world_happiness_rows)

    web_db.close_connection()