# imports
import os # os is part of python standard library -> no need to add to requirements.txt
import requests
import psycopg
import time # time is part of python standard library -> no need to add to requirements.txt
from psycopg import sql
from dotenv import load_dotenv
from decimal import Decimal # part of python standard library
from datetime import datetime # part of python standard library
from pathlib import Path # part of python standard library
import pandas as pd

#### API requests
# Full list of World Bank ISO2 codes for countries in Europe
europe_countries_codes = [
    "AT","BE","BG","HR","CY","CZ","DK","EE","FI","FR","DE","GR","HU","IS","IE",
    "IT","LV","LT","LU","MT","NL","NO","PL","PT","RO","SK","SI","ES","SE","CH",
    "GB","AL","BA","RS","MK","ME","XK","UA","MD","BY"
]

def get_European_country_general_info():
    try:
        url = "https://api.worldbank.org/v2/country/AT;BE;BG;HR;CY;CZ;DK;EE;FI;FR;DE;GR;HU;IS;IE;IT;LV;LT;LU;MT;NL;NO;PL;PT;RO;SK;SI;ES;SE;CH;GB;AL;BA;RS;MK;ME;XK;UA;MD;BY?format=json"
        response = requests.get(url, timeout=5)
        print("\nQueried URL:", response.url, "\n")

        if response.status_code == 200:
            country_info = response.json()
            # print(country_info)

            country_iso3code_all = []
            country_iso2code_all = []
            country_name_all = []
            country_income_level_all = []
            country_capital_city_all = []
            country_longitude_all = []
            country_latitude_all = []

            for country_dict in country_info[1]:
                country_iso3code = country_dict["id"]
                country_iso3code_all.append(country_iso3code)

                country_iso2code = country_dict["iso2Code"]
                country_iso2code_all.append(country_iso2code)

                country_name = country_dict["name"]
                country_name_all.append(country_name)

                country_income_level = country_dict["incomeLevel"]["value"]
                country_income_level_all.append(country_income_level)

                country_capital_city = country_dict["capitalCity"]
                country_capital_city_all.append(country_capital_city)

                country_longitude = country_dict["longitude"]
                country_longitude_all.append(float(country_longitude))

                country_latitude = country_dict["latitude"]
                country_latitude_all.append(float(country_latitude))

            print("....Collecting data.... (•˕•マ.ᐟ \n")
            print(f"List of country iso3codes:\n{country_iso3code_all}\n")
            print(f"List of country iso2codes:\n{country_iso2code_all}\n")
            print(f"List of country names:\n{country_name_all}\n")
            print(f"List of country income levels:\n{country_income_level_all}\n")
            print(f"List of country capital city:\n{country_capital_city_all}\n")
            print(f"List of country longitude:\n{country_longitude_all}\n")
            print(f"List of country latitude:\n{country_latitude_all}\n")
            print("---- Finish collecting data! ᓚ₍⑅^..^₎♡ ----\n")

            rows = list(zip( # zip() transposes a list of columns into a list of rows, suitable for INSERT query later (to add the data to db)
                country_iso3code_all,
                country_iso2code_all,
                country_name_all,
                country_income_level_all,
                country_capital_city_all,
                country_longitude_all,
                country_latitude_all))

            return rows
        else:
            print(f"Something went wrong, I couldn't fetch the requested data /ᐠ-˕-マ. Error status code: {response.status_code}.")

    except requests.exceptions.RequestException as e:
        print(f"Something went wrong ૮₍•᷄  ༝ •᷅₎ა --> Error message: {type(e).__name__} - {e}.")

"""
# working in progress

def get_corruption_info():
    try:
        url = "https://api.worldbank.org/v2/country/AT;BE;BG;HR;CY;CZ;DK;EE;FI;FR;DE;GR;HU;IS;IE;IT;LV;LT;LU;MT;NL;NO;PL;PT;RO;SK;SI;ES;SE;CH;GB;AL;BA;RS;MK;ME;XK;UA;MD;BY/indicators/CC.EST?format=json"
        response = requests.get(url, timeout=5)
        print("\nQueried URL:", response.url, "\n")

        if response.status_code == 200:
            corruption_info = response.json()
            print(corruption_info)

            for country_dict in corruption_info[1]:
                country_id = country_dict["country"]["id"]
                country_name = country_dict["country"]["value"]

        else:
                print(f"Something went wrong, I couldn't fetch the requested data /ᐠ-˕-マ. Error status code: {response.status_code}.")

    except requests.exceptions.RequestException as e:
        print(f"Something went wrong ૮₍•᷄  ༝ •᷅₎ა --> Error message: {type(e).__name__} - {e}.")

get_corruption_info()
"""

indicators = [
    "NY.GDP.MKTP.KD.ZG",  # GDP growth (annual %)
    "FP.CPI.TOTL.ZG",     # Inflation, consumer prices (annual %)
    "SL.UEM.TOTL.ZS"      # Unemployment, total (% of labor force)
]

def test():
    try:
        url = "https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL;NY.GDP.MKTP.KD.ZG;FP.CPI.TOTL.ZG;SL.UEM.TOTL.ZS;CC.EST?per_page=20000&format=csv" # indicators:  | NY.GDP.MKTP.KD.ZG: GDP growth | CC.EST: corruption info
        response = requests.get(url, timeout=5)
        print("\nQueried URL:", response.url, "\n")

        # if running inside Docker -> keep this as /data
        # if running locally outside Docker -> set DATA_DIR="postgres_data/data"
        DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
        DATA_DIR.mkdir(parents=True, exist_ok=True)  # create the folder (and any missing parents) if it doesn’t exist; don’t error if it already exists

        out_path = DATA_DIR / "wb_all_countries_multi_indicators.csv"

        if response.status_code == 200:
            out_path.write_bytes(response.content)

            print(f"Saved → {out_path}")

            df = pd.read_csv("wb_all_countries_multi_indicators.csv")
            print(df.head())

        else:
            print(f"Something went wrong, I couldn't fetch the requested data /ᐠ-˕-マ. Error status code: {response.status_code}.")

    except requests.exceptions.RequestException as e:
        print(f"Something went wrong ૮₍•᷄  ༝ •᷅₎ა --> Error message: {type(e).__name__} - {e}.")


#### Saving / persisting to db
class DatabaseError(Exception):
    pass

class WorldBankDBPostgres:
    def __init__(self):
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

    def _drop_table(self, table_name):
        try:
            self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.connection.commit()
        except (Exception, psycopg.DatabaseError) as e:
            raise DatabaseError(f"Something went wrong with dropping the table '{table_name}'. Error type: {type(e).__name__}, error message: '{e}'.")

    def add_data_to_db(self, data: list = None, table_name: str = "european_country_general_info"):
        if not data:
            print("There is no data to add to the database. /ᐠ-˕-マ")
            return

        query = sql.SQL("""
                        INSERT INTO {} (country_iso3code, country_iso2code, country_name, country_income_level, country_capital_city,
                                        country_longitude, country_latitude)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (country_iso3code) 
                        DO UPDATE SET
                            country_iso2code = EXCLUDED.country_iso2code,
                            country_name = EXCLUDED.country_name,
                            country_income_level = EXCLUDED.country_income_level,
                            country_capital_city = EXCLUDED.country_capital_city,
                            country_longitude = EXCLUDED.country_longitude,
                            country_latitude = EXCLUDED.country_latitude,
                            data_source = 'WorldBank API',
                            update_count = european_country_general_info.update_count + 1,
                            last_updated = NOW()
                        WHERE
                            (european_country_general_info.country_iso2code,
                            european_country_general_info.country_name,
                            european_country_general_info.country_income_level,
                            european_country_general_info.country_capital_city,
                            european_country_general_info.country_longitude,
                            european_country_general_info.country_latitude)
                        IS DISTINCT FROM
                            (EXCLUDED.country_iso2code,
                            EXCLUDED.country_name,
                            EXCLUDED.country_income_level,
                            EXCLUDED.country_capital_city,
                            EXCLUDED.country_longitude,
                            EXCLUDED.country_latitude);
                        """).format(sql.Identifier(table_name))
        # upsert = insert + update -> if inserting a row which has an existing country code (our primary key), then update that row
        # and add to the update_count only if the new data is different from the old one (only meaningful updates count)

        try:
            self.cursor.executemany(query.as_string(self.connection), data)
            self.connection.commit()
            print(f"Successfully added or updated {len(data)} rows into '{table_name}' ദ്ദി（•˕•マ.ᐟ")
        except (Exception, psycopg.DatabaseError) as e:
            self.connection.rollback()
            raise DatabaseError(f"Something went wrong with adding the data to the table '{table_name}'. Error type: {type(e).__name__}, error message: '{e}'.")

    def __str__(self):
        return f"WorldBank PostgreSQL database (schema: thi_miniproject)"

    def get_all_countries_info(self):
        """
        fetch and display all countries' general info
        """
        try:
            self.cursor.execute("SELECT * FROM european_country_general_info ORDER BY country_iso3code")
            all_countries_rows = self.cursor.fetchall()
            if not all_countries_rows:
                print(f"No country info available for {self}.")
                return
            category = [row[0] for row in self.cursor.description]
            print(f"\n--- Printing {len(all_countries_rows)} countries' general info for {self}: ---")
            number = 1
            for row in all_countries_rows:
                print(f"\n{number}. Country info of '{row[1]}' is:")
                number += 1
                country_info = []
                for col, val in zip(category, row):
                    # convert decimals and datetimes to readable formats:
                    if isinstance(val, Decimal):
                        val = float(val)
                    elif isinstance(val, datetime):
                        val = val.strftime("%Y-%m-%d %H:%M:%S %Z") # strftime stands for 'string format time'
                    country_info.append(f"{col}: {val}")
                print(", ".join(country_info))
        except (Exception, psycopg.DatabaseError) as e:
            self.connection.rollback()
            raise DatabaseError(f"Something went wrong with getting all countries' info. Error type: {type(e).__name__}, error message: '{e}'.")

    def get_country_info(self, country_names):
        """
        fetch and display general info for one or more countries (case-insensitive)
        - country_names can be a single string: "Austria, germany" or a list: ["Austria", "gErManY"]
        - update 'docker compose', service 'app_base' environment var COUNTRIES_OF_INTEREST to include or remove any european countries to be displayed
        :param country_names
        :return: general country info
        """
        try:
            # turn input into a list of strings in case it is a string
            if isinstance(country_names, str):
                country_names = [name.strip() for name in country_names.split(",")]

            print(f"\n--- Printing general country info for the following countries of interest: {', '.join(country_names)} (to update or change this list, go to 'docker compose' - service 'app_base' environment) ---")

            self.cursor.execute("SELECT * FROM european_country_general_info WHERE country_name ILIKE ANY(%s) ORDER BY country_name;", (country_names,))
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

if __name__ == "__main__":
    print("Hello from api_logger!")
    api_data = get_European_country_general_info()
    # api_data is a list of the following lists: country_code, country_name, country_income_level, country_capital_city, country_longitude, country_latitude
    wb_db = WorldBankDBPostgres()
    wb_db.add_data_to_db(api_data)

    display_all = os.getenv("DISPLAY_ALL_COUNTRIES_INFO", "false").strip().lower() in ("1", "true", "yes")
    if display_all:
        wb_db.get_all_countries_info()
    else:
        print(f"\n--- The user does not wish to display all countries' general info (•́ ᴖ •̀) (if you changed your mind, change the var DISPLAY_ALL_COUNTRIES_INFO to 'true' in 'docker compose' - service 'app_base' environment.) ---")

    names = os.getenv("COUNTRIES_OF_INTEREST", "").strip()
    if names:
        wb_db.get_country_info(names)
    else:
        print("\n--- Printing general country info for the countries of interest: No info about countries of interest was given ^. .^₎⟆ ---")

    test()

    wb_db.close_connection()


