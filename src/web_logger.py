# imports
import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
import pandas as pd
import numpy as np
from functools import reduce # part of python standard library
import os # part of python standard library
import psycopg
from psycopg import sql
from dotenv import load_dotenv

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

                # use the avoid_index_error method to avoid rows with fewer data than available columns
                try:
                    # country name as text
                    country_td = row_data[1] if len(row_data) > 1 else None
                    country_name = country_td.get_text(" ", strip = True) if isinstance(country_td, Tag) else None
                    data = {"Country": country_name}

                    # year cells: take bold text only
                    for year_index, year in enumerate(years):
                        cell_index = 2 + (year_index * 2)
                        # get the <td> cell safely
                        td = row_data[cell_index] if cell_index < len(row_data) else None

                        # check that the cell contains a <b> tag (bold)
                        if isinstance(td, Tag):
                            bold = td.select_one("b, strong")
                            data[year] = bold.get_text(strip = True) if bold else None
                        else:
                            # no <b> tag -> skip or set as None
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
    print(f"----------- Transforming and cleaning the table into a 3NF-compliant dataset! -----------\n")
    transformed_df = table_to_be_transformed.melt(
        id_vars = ["Country"], # column "country" remains the same / fixed
        var_name = "Year", # name of the new column for the former headers (e.g. 1995, 2024)
        value_name = "CPI Score" # name of the new column for the old values (cpi scores)
    )
    transformed_df["Year"] = transformed_df["Year"].astype(int)
    transformed_df["CPI Score"] = transformed_df["CPI Score"].astype(float)

    # corrections (due to the wiki table formats, the following info were skipped):
    corrections = [
        {"Country": "Brunei Darussalam", "Year": 2022, "CPI Score": None},
        {"Country": "Brunei Darussalam", "Year": 2020, "CPI Score": 60.0},
        {"Country": "Seychelles", "Year": 2015, "CPI Score": 55.0},
        {"Country": "Seychelles", "Year": 2014, "CPI Score": 55.0},
        {"Country": "Seychelles", "Year": 2013, "CPI Score": 54.0},
        {"Country": "Seychelles", "Year": 2012, "CPI Score": 52.0},
        {"Country": "Bahamas", "Year": 2014, "CPI Score": 71.0},
        {"Country": "Bahamas", "Year": 2013, "CPI Score": 71.0},
        {"Country": "Bahamas", "Year": 2012, "CPI Score": 71.0},
        {"Country": "Barbados", "Year": 2014, "CPI Score": 74.0},
        {"Country": "Barbados", "Year": 2013, "CPI Score": 75.0},
        {"Country": "Barbados", "Year": 2012, "CPI Score": 76.0},
        {"Country": "Brunei Darussalam", "Year": 2013, "CPI Score": 60.0},
        {"Country": "Brunei Darussalam", "Year": 2012, "CPI Score": 55.0},
        {"Country": "Saint Vincent and the Grenadines", "Year": 2014, "CPI Score": 62.0},
        {"Country": "Saint Vincent and the Grenadines", "Year": 2013, "CPI Score": 62.0},
        {"Country": "Saint Vincent and the Grenadines", "Year": 2012, "CPI Score": 62.0},
        {"Country": "Saint Lucia", "Year": 2014, "CPI Score": 71.0},
        {"Country": "Saint Lucia", "Year": 2013, "CPI Score": 71.0},
        {"Country": "Saint Lucia", "Year": 2012, "CPI Score": 71.0},
        {"Country": "Dominica", "Year": 2014, "CPI Score": 58.0},
        {"Country": "Dominica", "Year": 2013, "CPI Score": 58.0},
        {"Country": "Dominica", "Year": 2012, "CPI Score": 58.0},
        {"Country": "Eswatini", "Year": 2014, "CPI Score": 43.0},
        {"Country": "Eswatini", "Year": 2013, "CPI Score": 39.0},
        {"Country": "Eswatini", "Year": 2012, "CPI Score": 37.0},
        {"Country": "Equatorial Guinea", "Year": 2013, "CPI Score": 19.0},
        {"Country": "Equatorial Guinea", "Year": 2012, "CPI Score": 20.0},
        {"Country": "Saint Lucia", "Year": 2009, "CPI Score": 70.0},
        {"Country": "Saint Lucia", "Year": 2008, "CPI Score": 71.0},
        {"Country": "Saint Lucia", "Year": 2007, "CPI Score": 68.0},
        {"Country": "Saint Vincent and the Grenadines", "Year": 2009, "CPI Score": 64.0},
        {"Country": "Saint Vincent and the Grenadines", "Year": 2008, "CPI Score": 65.0},
        {"Country": "Saint Vincent and the Grenadines", "Year": 2007, "CPI Score": 61.0},
        {"Country": "Fiji", "Year": 2005, "CPI Score": 40.0},
        {"Country": "Grenada", "Year": 2007, "CPI Score": 34.0},
        {"Country": "Grenada", "Year": 2006, "CPI Score": 35.0},
        {"Country": "Liberia", "Year": 2005, "CPI Score": 22.0},
        {"Country": "Suriname", "Year": 2009, "CPI Score": 37.0},
        {"Country": "Suriname", "Year": 2008, "CPI Score": 36.0},
        {"Country": "Suriname", "Year": 2007, "CPI Score": 35.0},
        {"Country": "Suriname", "Year": 2006, "CPI Score": 30.0},
        {"Country": "Suriname", "Year": 2005, "CPI Score": 32.0},
        {"Country": "Suriname", "Year": 2004, "CPI Score": 43.0},
        {"Country": "Belize", "Year": 2008, "CPI Score": 29.0},
        {"Country": "Belize", "Year": 2007, "CPI Score": 30.0},
        {"Country": "Belize", "Year": 2006, "CPI Score": 35.0},
        {"Country": "Belize", "Year": 2005, "CPI Score": 37.0},
        {"Country": "Belize", "Year": 2004, "CPI Score": 38.0},
        {"Country": "Belize", "Year": 2003, "CPI Score": 45.0},
        {"Country": "Serbia and Montenegro", "Year": 2005, "CPI Score": 28.0},
        {"Country": "Serbia and Montenegro", "Year": 2004, "CPI Score": 27.0},
        {"Country": "Serbia and Montenegro", "Year": 2003, "CPI Score": 23.0},
        {"Country": "Palestine", "Year": 2005, "CPI Score": 26.0},
        {"Country": "Palestine", "Year": 2004, "CPI Score": 25.0},
        {"Country": "Palestine", "Year": 2003, "CPI Score": 30.0},
        {"Country": "Afghanistan", "Year": 2005, "CPI Score": 25.0},
        {"Country": "Somalia", "Year": 2005, "CPI Score": 21.0},
        {"Country": "Uruguay", "Year": 1999, "CPI Score": 44.0},
        {"Country": "Uruguay", "Year": 1998, "CPI Score": 43.0},
        {"Country": "Hungary", "Year": 1999, "CPI Score": 52.0},
        {"Country": "Hungary", "Year": 1998, "CPI Score": 50.0},
        {"Country": "Belarus", "Year": 2000, "CPI Score": 41.0},
        {"Country": "Belarus", "Year": 1999, "CPI Score": 34.0},
        {"Country": "Belarus", "Year": 1998, "CPI Score": 39.0},
        {"Country": "Mongolia", "Year": 1999, "CPI Score": 43.0},
        {"Country": "Jamaica", "Year": 1999, "CPI Score": 38.0},
        {"Country": "Jamaica", "Year": 1998, "CPI Score": 38.0},
        {"Country": "Morocco", "Year": 2000, "CPI Score": 47.0},
        {"Country": "Morocco", "Year": 1999, "CPI Score": 41.0},
        {"Country": "Morocco", "Year": 1998, "CPI Score": 37.0},
        {"Country": "Ethiopia", "Year": 2000, "CPI Score": 32.0},
        {"Country": "FYR Macedonia", "Year": 1999, "CPI Score": 33.0},
        {"Country": "Honduras", "Year": 1999, "CPI Score": 18.0},
        {"Country": "Honduras", "Year": 1998, "CPI Score": 17.0},
        {"Country": "Burkina Faso", "Year": 2000, "CPI Score": 30.0},
        {"Country": "Pakistan", "Year": 1999, "CPI Score": 22.0},
        {"Country": "Pakistan", "Year": 1998, "CPI Score": 27.0},
        {"Country": "Nicaragua", "Year": 1999, "CPI Score": 31.0},
        {"Country": "Nicaragua", "Year": 1998, "CPI Score": 30.0},
        {"Country": "Guatemala", "Year": 1999, "CPI Score": 32.0},
        {"Country": "Guatemala", "Year": 1998, "CPI Score": 31.0},
        {"Country": "Albania", "Year": 1999, "CPI Score": 23.0},
        {"Country": "Georgia", "Year": 1999, "CPI Score": 23.0},
        {"Country": "Armenia", "Year": 2000, "CPI Score": 25.0},
        {"Country": "Armenia", "Year": 1999, "CPI Score": 25.0},
        {"Country": "Kyrgyz Republic", "Year": 1999, "CPI Score": 22.0},
        {"Country": "Mozambique", "Year": 2000, "CPI Score": 22.0},
        {"Country": "Mozambique", "Year": 1999, "CPI Score": 35.0},
        {"Country": "Paraguay", "Year": 1999, "CPI Score": 20.0},
        {"Country": "Paraguay", "Year": 1998, "CPI Score": 15.0},
        {"Country": "Angola", "Year": 2000, "CPI Score": 17.0},
        {"Country": "FR Yugoslavia", "Year": 2000, "CPI Score": 13.0},
        {"Country": "FR Yugoslavia", "Year": 1999, "CPI Score": 20.0},
        {"Country": "FR Yugoslavia", "Year": 1998, "CPI Score": 30.0},
        {"Country": "Jordan", "Year": 1996, "CPI Score": 48.9},
        {"Country": "Ecuador", "Year": 1996, "CPI Score": 31.9},
        {"Country": "Egypt", "Year": 1996, "CPI Score": 28.4},
        {"Country": "Uganda", "Year": 1996, "CPI Score": 27.1},
        {"Country": "Cameroon", "Year": 1996, "CPI Score": 24.6},
        {"Country": "Bangladesh", "Year": 1996, "CPI Score": 22.9},
        {"Country": "Kenya", "Year": 1996, "CPI Score": 22.1}
    ]
    corrections_df = pd.DataFrame(corrections)

    # enforce types
    transformed_df["Country"] = transformed_df["Country"].str.strip()
    transformed_df["Year"] = transformed_df["Year"].astype(int)
    transformed_df["CPI Score"] = transformed_df["CPI Score"].astype(float)

    # left-join corrections and overwrite cpi scores where provided
    fixed_df = transformed_df.merge(corrections_df, on = ["Country", "Year"], how = "left", suffixes = ("", "_corr"))
    # overwrite scraped scores with correction values when available
    fixed_df["CPI Score"] = fixed_df["CPI Score_corr"].where(
        fixed_df["CPI Score_corr"].notna(), # take correction when it's not NaN
        fixed_df["CPI Score"] # otherwise keep the scraped one
    )
    fixed_df = fixed_df.drop(columns = ["CPI Score_corr"])

    # sanity check
    os.makedirs("data", exist_ok = True)  # creates data/ if it doesn’t exist
    csv_path = "data/cpi_fixed_transformed.csv"
    fixed_df.to_csv(csv_path, index = False, encoding = "utf-8")
    print(f"Saved fixed and transformed (not cleaned - so NaNs included) CPI data to {csv_path}.\n")

    # drop NaN scores (so that the data saved into db is cleaned)
    transformed_cleaned_df = fixed_df.dropna(subset = ["CPI Score"])

    print(f"The first 5 rows of the cleaned, transformed, 3NF-compliant table (NaNs removed):\n\n", transformed_cleaned_df.head(5), "\n")
    print("The cleaned, transformed table description:\n\n", transformed_cleaned_df.describe())
    print(f"\n--------------- Finished transforming and cleaning the table! ₍^. .^₎Ⳋ -----------------\n")

    # turn the df into a list of tuples for saving into the db later
    cpi_db = transformed_cleaned_df.replace({np.nan: None})  # replace NaN with None for postgreSQL
    cpi_rows = list(cpi_db.itertuples(index = False, name = None))  # convert to list of tuples in correct column order

    return cpi_rows

#######################################
# website:
# data: World Happiness
#######################################


#######################################
# Save / persist to db
#######################################
class DatabaseError(Exception):
    pass

class WorldBankDBPostgres:
    def __init__(self):
        """
        automatically connect to postgres database when a class object is instantiated.
        """
        load_dotenv()  # this reads .env locally, in docker env is already there / set
        dbname = os.getenv("DB_NAME", os.getenv("POSTGRES_DB", "worldbank"))  # double fallbacks: if there's no env var name 'DB_NAME', then check for 'POSTGRES_DB', if still fails, use the default 'worldbank'
        user = os.getenv("DB_USER", os.getenv("POSTGRES_USER", "user"))
        password = os.getenv("DB_PASSWORD", os.getenv("POSTGRES_PASSWORD", "katzi"))  # in my .env file, I have a different pw but since it's in .gitignore, we can just use the default pw 'katzi'
        host = os.getenv("DB_HOST", "localhost")  # use 'db' inside docker container, 'localhost' outside (e.g. in locally installed apps such as pgAdmin)
        port = int(os.getenv("DB_PORT", 5555))  # use 5432 for inside the docker container, 5555 for locally installed apps such as pgAdmin

        try:
            self.connection = self.connect_with_retry({
                "dbname": dbname,
                "user": user,
                "password": password,
                "host": host,
                "port": port,
                "options": "-c search_path=thi_miniproject" # applied for the entire session, so that I don't have to manually command 'SET search_path TO thi_miniproject;' for every SQL query
            })
            self.cursor = self.connection.cursor()
            self.connection.commit()
            print("\n- Connected to database (schema 'thi_miniproject' is set)! -\n")

        except (Exception, psycopg.DatabaseError) as e:
            self.connection = psycopg.connect(dbname = dbname, user = user, password = password, host = host, port = port)
            raise DatabaseError(f"Something went wrong with the connection ≽^- ˕ -^≼ Error type: {type(e).__name__}, error message: '{e}'.")

    @staticmethod
    def connect_with_retry(dsn_kwargs, retries = 5, delay = 3): # helper function
        """
        try to connect to postgres multiple times before giving up.
        useful when db starts slower than the app in docker compose.
        """
        last_err = None
        for attempt in range(1, retries + 1):
            try:
                conn = psycopg.connect(**dsn_kwargs)
                conn.autocommit = False
                print(f"Connected on attempt # {attempt} ₍^. .^₎⟆")
                return conn
            except psycopg.OperationalError as e:
                last_err = e
                print(f"Attempt {attempt}: Postgres not ready yet. Retrying in {delay}s...")
                time.sleep(delay)
        raise last_err

    def add_data_to_db(self, data: list = None, table_name: str = "staging_cpi_raw"):
        """persist acquired data into db"""
        if not data:
            print("There is no data to add to the database. /ᐠ-˕-マ")
            return

        query = sql.SQL("""
                        INSERT INTO {} (country_name, year, cpi_score)
                        VALUES (%s, %s, %s);
                        """).format(sql.Identifier(table_name))
        # on conflict do nothing to prevent throwing errors and creating duplicates

        try:
            self.cursor.executemany(query.as_string(self.connection), data)
            self.connection.commit()
            print(f"Successfully added or updated {len(data)} rows into '{table_name}' ദ്ദി（•˕•マ.ᐟ")
        except (Exception, psycopg.DatabaseError) as e:
            self.connection.rollback()
            raise DatabaseError(f"Something went wrong with adding the data to the table '{table_name}'. Error type: {type(e).__name__}, error message: '{e}'.")

    def __str__(self):
        """replace the string special method to automatically display the db name"""
        return f"WorldBank PostgreSQL database (schema: thi_miniproject)"

    def get_country_info(self, country_names):
        """
        fetch and display info for one or more countries (case-insensitive)
        - country_names can be a single string: "Austria, germany" or a list: ["Austria", "gErManY"]
        - update 'docker compose', service 'app_base' environment var COUNTRIES_OF_INTEREST to include or remove any countries to be displayed
        :param country_names
        :return: country info
        """
        try:
            # turn input into a list of strings in case it is a string
            if isinstance(country_names, str):
                country_names = [name.strip() for name in country_names.split(",")]

            print(f"\n--- Printing country info for the following countries of interest: {', '.join(country_names)} (to update or change this list, go to 'docker compose' - service 'app_base' environment) ---")

            self.cursor.execute("SELECT * FROM staging_cpi_raw WHERE country_name ILIKE ANY(%s) ORDER BY country_name;", (country_names,))
            country_rows = self.cursor.fetchall()
            if not country_rows:
                print(f"No country info found for: {', '.join(country_names)}.")
                return
            category = [row[0] for row in self.cursor.description]

            number = 1
            for row in country_rows:
                print(f"\n{number}. Country info of '{row[1]}' is:")
                number += 1
                country_info = []
                for col, val in zip(category, row):
                    # convert decimals and datetimes to readable formats:
                    if isinstance(val, Decimal):
                        val = float(val)
                    elif isinstance(val, datetime):
                        val = val.strftime("%Y-%m-%d %H:%M:%S %Z")  # strftime stands for 'string format time'
                    country_info.append(f"{col}: {val}")
                print(", ".join(country_info))
        except (Exception, psycopg.DatabaseError) as e:
            raise DatabaseError(f"Something went wrong with getting the country info of '{', '.join(country_names)}'. Error type: {type(e).__name__}, error message: '{e}'.")

    def close_connection(self):
        try:
            self.cursor.close()
        except (Exception, psycopg.DatabaseError) as e:
            raise DatabaseError(f"Something went wrong with closing the connection. Error type: {type(e).__name__}, error message: '{e}'.")


#######################################
# Run the web crawlers
#######################################
if __name__ == "__main__":
    print("Hello from web_logger!")
    dfs = scrape_country_cpi_tables()
    normalised_dfs = normalise_cpi_data(dfs)
    sorted_merged_table = merge_tables_by_country(normalised_dfs)
    cpi_data = transform_and_clean_data(sorted_merged_table)
    wb_db = WorldBankDBPostgres()
    wb_db.add_data_to_db(cpi_data)

    wb_db.close_connection()