# imports
import os # part of python standard library -> no need to add to requirements.txt
import requests
from save_data import DBPostgres, DatabaseError
import psycopg
from psycopg import sql
import pandas as pd
import numpy as np

#######################################
# API: World Bank
#######################################
def get_country_general_info():
    """
    this function fetch data about all countries' general info through an API request to World Bank API
    :return: countries general info
    """
    try:
        url = "https://api.worldbank.org/v2/country/?per_page=20000&format=json"
        response = requests.get(url, timeout=5)
        print("\nQueried URL:", response.url, "\n")

        if response.status_code != 200:
            print(f"Something went wrong, I couldn't fetch the requested data /ᐠ-˕-マ. Error status code: {response.status_code}.")
            print(response.content)
            return []

        print("....API request approved! Collecting data.... (•˕•マ.ᐟ \n")
        country_info = response.json()[1] # exclude the metadata info
        filtered_country_info = [country for country in country_info if country["region"]["value"] != "Aggregates"]

        country_info_df = pd.DataFrame([
            {
                "country_iso3code": country_dict.get("id"),
                "country_iso2code": country_dict.get("iso2Code"),
                "country_name": country_dict.get("name"),
                "region_name": country_dict.get("region", {}).get("value"),
                "region_id": country_dict.get("region", {}).get("id"),
                "region_iso2code": country_dict.get("region", {}).get("iso2code"),
                "country_income_level": country_dict.get("incomeLevel", {}).get("value"),
                "country_capital_city": country_dict.get("capitalCity"),
                "country_longitude": float(country_dict.get("longitude")) if country_dict.get("longitude") else None,
                "country_latitude": float(country_dict.get("latitude")) if country_dict.get("latitude") else None
            }
            for country_dict in filtered_country_info
        ])

        print("Country info df shape:", country_info_df.shape)
        print("\nFirst five rows of the country info df:\n", country_info_df.head())
        print("\nDescription of the country info df:\n", country_info_df.describe(), "\n")
        print(country_info_df.info())
        print("\n---- Finish collecting data! ᓚ₍⑅^..^₎♡ ----\n")

        # turn the df into a list of tuples for saving into the db later
        country_info_db = country_info_df.replace({np.nan: None}) # replace NaN with None for postgreSQL
        country_rows = list(country_info_db.itertuples(index=False, name=None))

        return country_rows

    except requests.exceptions.RequestException as e:
        print(f"Something went wrong ૮₍•᷄  ༝ •᷅₎ა --> Error message: {type(e).__name__} - {e}.")

def get_all_wb_topics():
    """
    this function fetch all World Bank umbrella topics (21 in total)
    :return: World Bank topics
    """
    try:
        url = "https://api.worldbank.org/v2/topic?format=json"
        response = requests.get(url, timeout = 5)
        print("\nQueried URL (for getting all WB topics):", response.url, "\n")

        if response.status_code != 200:
            print(f"Something went wrong, I couldn't fetch the requested WB topics /ᐠ-˕-マ. Error status code: {response.status_code}.")
            print(response.content)
            return []

        print(".... API request approved! Collecting all WB umbrella topics .... (•˕•マ.ᐟ \n")
        wb_topics = response.json()[1]

        topics_df = pd.DataFrame([
            {
                "topic_id": topic["id"], # []: mandatory fields - strict dict access -> it doesn’t exist, Python raises a KeyError
                "topic_name": topic["value"],
                "description": topic["sourceNote"]
            } for topic in wb_topics
        ])
        # enforce types
        topics_df["topic_id"] = topics_df["topic_id"].astype(int)

        print("Topics df shape:", topics_df.shape)
        print("\nFirst five rows of the topics df:\n", topics_df.head())
        print(topics_df.info())
        print(f"\n--- {len(topics_df)} WB topics have been collected!  --- ദ്ദി（• ˕ •マ.ᐟ\n")

        # turn the df into a list of tuples for saving into the db later
        wb_topics_db = topics_df.replace({np.nan: None})  # replace NaN with None for postgreSQL
        wb_topics_rows = list(wb_topics_db.itertuples(index = False, name = None))

        return wb_topics_rows

    except requests.exceptions.RequestException as e:
        print(f"Something went wrong ૮₍•᷄  ༝ •᷅₎ა --> Error message: {type(e).__name__} - {e}.")

def yes_no_to_bool(x):
    if isinstance(x, str):
        x = x.strip().upper()
        if x == "Y":
            return True
        if x == "N":
            return False
    return None  # for None, "", or other unexpected cases

def get_all_wb_sources():
    """
    this function fetch all World Bank sources (71 in total)
    :return: World Bank sources
    """
    try:
        url = "https://api.worldbank.org/v2/source?format=json&per_page=500"
        response = requests.get(url, timeout = 5)
        print("\nQueried URL (for getting all WB sources):", response.url, "\n")

        if response.status_code != 200:
            print(f"Something went wrong, I couldn't fetch the requested WB sources /ᐠ-˕-マ. Error status code: {response.status_code}.")
            print(response.content)
            return []

        print(".... API request approved! Collecting all WB sources .... (•˕•マ.ᐟ \n")
        wb_sources = response.json()[1]

        sources_df = pd.DataFrame([
            {
                "source_id": int(source["id"]),
                "source_name": source["name"],
                "source_code": source.get("code"),
                "data_availability": source.get("dataavailability"),
                "metadata_availability": source.get("metadataavailability"),
                "concepts": int(source.get("concepts")),
                "last_updated": source.get("lastupdated")
            } for source in wb_sources
        ])

        for col in ["data_availability", "metadata_availability"]:
            sources_df[col] = sources_df[col].apply(yes_no_to_bool)

        print("Sources df shape:", sources_df.shape)
        print("\nFirst five rows of the sources df:\n", sources_df.head())
        print(sources_df.info())
        print(f"\n--- {len(sources_df)} WB sources have been collected!  --- ദ്ദി（• ˕ •マ.ᐟ\n")

        # turn the df into a list of tuples for saving into the db later
        wb_sources_db = sources_df.replace({np.nan: None})  # replace NaN with None for postgreSQL
        wb_sources_rows = list(wb_sources_db.itertuples(index = False, name = None))

        return wb_sources_rows

    except requests.exceptions.RequestException as e:
        print(f"Something went wrong ૮₍•᷄  ༝ •᷅₎ა --> Error message: {type(e).__name__} - {e}.")

def get_all_wb_indicators():
    """
    this function fetch all World Bank indicators
    :return: World Bank indicators
    """
    try:
        url = "https://api.worldbank.org/v2/source/2/indicators?format=json&per_page=20000"
        response = requests.get(url, timeout = 5)
        print("\nQueried URL (for getting all WB indicators):", response.url, "\n")

        if response.status_code != 200:
            print(f"Something went wrong, I couldn't fetch the requested WB indicators /ᐠ-˕-マ. Error status code: {response.status_code}.")
            print(response.content)
            return []

        print(".... API request approved! Collecting all WB indicators .... (•˕•マ.ᐟ \n")
        wb_indicators = response.json()[1]

        indicators_df = pd.DataFrame([
            {
                "indicator_id": indicator["id"], # []: mandatory fields - strict dict access -> it doesn’t exist, Python raises a KeyError
                "indicator_name": indicator["name"],
                "source_id": indicator["source"]["id"],
                "description": indicator.get("sourceNote"),
                "topics": indicator["topics"]
            } for indicator in wb_indicators
        ])
        # enforce types
        indicators_df["source_id"] = indicators_df["source_id"].astype(int)

        print("Indicator info df shape:", indicators_df.shape)
        print("\nFirst five rows of the indicators df:\n", indicators_df.head())
        print(indicators_df.info())
        print(f"\n--- {len(indicators_df)} WB indicators have been collected!  --- ദ്ദി（• ˕ •マ.ᐟ\n")

        # turn the df into a list of tuples and drop column 'topics' which violates 3NF for saving into the db later
        wb_indicators_db = indicators_df.replace({np.nan: None})  # replace NaN with None for postgreSQL
        wb_indicators_rows = list(wb_indicators_db.drop(columns = ["topics"]).itertuples(index = False, name = None))

        # list of all indicator ids for looping later
        indicator_ids = indicators_df["indicator_id"].unique().tolist()

        # get the indicator-topic list, and flatten the nested list of topics per indicator
        indicator_topics_rows = []
        for _, row in indicators_df.iterrows(): # _ ignore the numeric index, and iterrows() iterates over each row of the DataFrame
            indicator_id = row["indicator_id"]
            for topic in row["topics"]:
                topic_id = int(topic.get("id"))
                indicator_topics_rows.append([indicator_id, topic_id])

        return wb_indicators_rows, indicator_ids, indicator_topics_rows

    except requests.exceptions.RequestException as e:
        print(f"Something went wrong ૮₍•᷄  ༝ •᷅₎ა --> Error message: {type(e).__name__} - {e}.")

# csv data
#     try:
#         url = "https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL;NY.GDP.MKTP.KD.ZG;FP.CPI.TOTL.ZG;SL.UEM.TOTL.ZS;CC.EST?per_page=20000&format=csv" # indicators:  | NY.GDP.MKTP.KD.ZG: GDP growth | CC.EST: corruption info
#         response = requests.get(url, timeout=5)
#         print("\nQueried URL:", response.url, "\n")
#
#         # if running inside Docker -> keep this as /data
#         # if running locally outside Docker -> set DATA_DIR="postgres_data/data"
#         DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
#         DATA_DIR.mkdir(parents=True, exist_ok=True)  # create the folder (and any missing parents) if it doesn’t exist; don’t error if it already exists
#
#         out_path = DATA_DIR / "wb_all_countries_multi_indicators.csv"
#
#         if response.status_code == 200:
#             out_path.write_bytes(response.content)
#
#             print(f"Saved → {out_path}")
#
#             df = pd.read_csv("wb_all_countries_multi_indicators.csv")
#             print(df.head())
#
#         else:
#             print(f"Something went wrong, I couldn't fetch the requested data /ᐠ-˕-マ. Error status code: {response.status_code}.")
#
#     except requests.exceptions.RequestException as e:
#         print(f"Something went wrong ૮₍•᷄  ༝ •᷅₎ა --> Error message: {type(e).__name__} - {e}.")

#######################################
# Save / persist to db
#######################################
class ApiDB(DBPostgres):
    """child class of DBPostgres"""
    def add_data_to_staging_country_general_info_table(self, data: list, table_name: str = "staging_country_general_info"):
        """persist acquired raw data into staging_db"""
        if not data:
            print("There is no API data to add to the database. /ᐠ-˕-マ")
            return

        query = sql.SQL("""
                        INSERT INTO {} (country_iso3code, country_iso2code, country_name, region_name, region_id, region_iso2code, country_income_level, country_capital_city,
                                        country_longitude, country_latitude)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (country_iso3code) 
                        DO UPDATE SET
                            country_iso2code = EXCLUDED.country_iso2code,
                            country_name = EXCLUDED.country_name,
                            region_name = EXCLUDED.region_name,
                            region_id = EXCLUDED.region_id,
                            region_iso2code = EXCLUDED.region_iso2code,
                            country_income_level = EXCLUDED.country_income_level,
                            country_capital_city = EXCLUDED.country_capital_city,
                            country_longitude = EXCLUDED.country_longitude,
                            country_latitude = EXCLUDED.country_latitude,
                            data_source = 'WorldBank API',
                            update_count = staging_country_general_info.update_count + 1,
                            last_updated = NOW()
                        WHERE
                            (staging_country_general_info.country_iso2code,
                            staging_country_general_info.country_name,
                            staging_country_general_info.region_name,
                            staging_country_general_info.region_id,
                            staging_country_general_info.region_iso2code,
                            staging_country_general_info.country_income_level,
                            staging_country_general_info.country_capital_city,
                            staging_country_general_info.country_longitude,
                            staging_country_general_info.country_latitude)
                        IS DISTINCT FROM
                            (EXCLUDED.country_iso2code,
                            EXCLUDED.country_name,
                            EXCLUDED.region_name,
                            EXCLUDED.region_id,
                            EXCLUDED.region_iso2code,
                            EXCLUDED.country_income_level,
                            EXCLUDED.country_capital_city,
                            EXCLUDED.country_longitude,
                            EXCLUDED.country_latitude);
                        """).format(sql.Identifier(table_name))
        # upsert = insert + update -> if inserting a row which has an existing country code (our primary key), then update that row
        # and add to the update_count only if the new data is different from the old one (only meaningful updates count)

        try:
            self._executemany(query, data)
            self.connection.commit()
            print(f"Successfully added or updated {len(data)} rows into '{table_name}' ദ്ദി（•˕•マ.ᐟ\n")
        except (Exception, psycopg.DatabaseError) as e:
            self.connection.rollback()
            raise DatabaseError(f"Something went wrong with adding the API data to the table '{table_name}'. Error type: {type(e).__name__}, error message: '{e}'.")

    def add_data_to_country_general_info_table(self, data: list, table_name: str = "country_general_info"):
        """persist normalised acquired data into db"""
        if not data:
            print("There is no normalised API-data to add to the database. /ᐠ-˕-マ")
            return

        query = sql.SQL("""
                        INSERT INTO {} (country_iso3code, country_iso2code, country_name, region_id, country_income_level, country_capital_city,
                                        country_longitude, country_latitude)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (country_iso3code) DO NOTHING;
                        """).format(sql.Identifier(table_name))
        # on conflict do nothing to prevent throwing errors and creating duplicates

        try:
            self._executemany(query, data)
            self.connection.commit()
            print(f"Successfully added or updated {len(data)} normalised rows into '{table_name}' ദ്ദി（•˕•マ.ᐟ\n")
        except (Exception, psycopg.DatabaseError) as e:
            self.connection.rollback()
            raise DatabaseError(f"Something went wrong with adding the normalised API-data to the table '{table_name}'. Error type: {type(e).__name__}, error message: '{e}'.")

    def add_data_to_region_table(self, data: list, table_name: str = "region"):
        """persist normalised acquired data into db"""
        if not data:
            print("There is no region data to add to the database. /ᐠ-˕-マ")
            return

        query = sql.SQL("""
                        INSERT INTO {} (region_id, region_iso2code, region_name)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (region_id) DO NOTHING;
                        """).format(sql.Identifier(table_name))
        # on conflict do nothing to prevent throwing errors and creating duplicates

        try:
            self._executemany(query, data)
            self.connection.commit()
            print(f"Successfully added or updated {len(data)} normalised rows into '{table_name}' ദ്ദി（•˕•マ.ᐟ\n")
        except (Exception, psycopg.DatabaseError) as e:
            self.connection.rollback()
            raise DatabaseError(f"Something went wrong with adding the region data to the table '{table_name}'. Error type: {type(e).__name__}, error message: '{e}'.")

    def add_data_to_country_alias_table(self, data: list, table_name: str = "country_alias"):
        """persist staging data into db"""
        if not data:
            print("There is no country aliases to add to the database. /ᐠ-˕-マ")
            return

        query = sql.SQL("""
                        INSERT INTO {} (country_name_alias, country_iso3code)
                        VALUES (%s, %s)
                        ON CONFLICT (country_name_alias) DO NOTHING;
                        """).format(sql.Identifier(table_name))
        # on conflict do nothing to prevent throwing errors and creating duplicates

        try:
            self._executemany(query, data)
            self.connection.commit()
            print(f"Successfully added or updated {len(data)} country-alias rows into '{table_name}' ദ്ദി（•˕•マ.ᐟ\n")
        except (Exception, psycopg.DatabaseError) as e:
            self.connection.rollback()
            raise DatabaseError(f"Something went wrong with adding the country-alias data to the table '{table_name}'. Error type: {type(e).__name__}, error message: '{e}'.")

    def get_all_eu_countries_info(self):
        """
        fetch and display all European countries' general info
        """
        # Full list of World Bank ISO2 codes for countries in Europe
        europe_countries_iso2codes = [
            "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", "IS", "IE",
            "IT", "LV", "LT", "LU", "MT", "NL", "NO", "PL", "PT", "RO", "SK", "SI", "ES", "SE", "CH",
            "GB", "AL", "BA", "RS", "MK", "ME", "XK", "UA", "MD", "BY"
        ]

        # dynamically create %s placeholders for each country code
        eu_country_iso2codes = sql.SQL(", ").join(sql.Placeholder() * len(europe_countries_iso2codes))

        query = sql.SQL(
            "SELECT * FROM staging_country_general_info "
            "WHERE country_iso2code IN ({codes}) "
            "ORDER BY country_iso3code;"
        ).format(codes = eu_country_iso2codes)

        try:
            self.cursor.execute(query, europe_countries_iso2codes)
            all_countries_rows = self.cursor.fetchall()
            if not all_countries_rows:
                print(f"No European country info available for {self}.")
                return
            category = [row[0] for row in self.cursor.description]
            print(f"\n--- Printing {len(all_countries_rows)} European countries' general info for {self}: ---")
            for idx, row in enumerate(all_countries_rows, start = 1):
                print(f"\n{idx}. Country info of '{row[2]}' is:")
                print(self._pretty_row(category, row))
        except (Exception, psycopg.DatabaseError) as e:
            self.connection.rollback()
            raise DatabaseError(f"Something went wrong with getting all European countries' general info. Error type: {type(e).__name__}, error message: '{e}'.")

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

            self.cursor.execute("SELECT * FROM staging_country_general_info WHERE country_name ILIKE ANY(%s) ORDER BY country_name;", (country_names,))
            country_rows = self.cursor.fetchall()
            if not country_rows:
                print(f"No country info found for: {', '.join(country_names)}.")
                return
            category = [row[0] for row in self.cursor.description]

            for idx, row in enumerate(country_rows, start = 1):
                print(f"\n{idx}. Country info of '{row[2]}' is:")
                print(self._pretty_row(category, row))
            print("\n--- Finished printing country info! ---\n")

        except (Exception, psycopg.DatabaseError) as e:
            raise DatabaseError(f"Something went wrong with getting the country info of '{', '.join(country_names)}'. Error type: {type(e).__name__}, error message: '{e}'.")

    def add_data_to_wb_topics_table(self, data: list, table_name: str = "wb_topics"):
        """persist normalised acquired data into db"""
        if not data:
            print("There is no normalised API-data to add to the database. /ᐠ-˕-マ")
            return

        query = sql.SQL("""
                        INSERT INTO {} (topic_id, topic_name, description)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (topic_id) DO NOTHING;
                        """).format(sql.Identifier(table_name))
        # on conflict do nothing to prevent throwing errors and creating duplicates

        try:
            self._executemany(query, data)
            self.connection.commit()
            print(f"Successfully added or updated {len(data)} normalised rows into '{table_name}' ദ്ദി（•˕•マ.ᐟ\n")
        except (Exception, psycopg.DatabaseError) as e:
            self.connection.rollback()
            raise DatabaseError(f"Something went wrong with adding the normalised API-data to the table '{table_name}'. Error type: {type(e).__name__}, error message: '{e}'.")

    def add_data_to_wb_source_table(self, data: list, table_name: str = "wb_source"):
        """persist normalised acquired data into db"""
        if not data:
            print("There is no normalised API-data to add to the database. /ᐠ-˕-マ")
            return

        query = sql.SQL("""
                        INSERT INTO {} (source_id, source_name, source_code, data_availability, metadata_availability, concepts, last_updated)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (source_id) DO NOTHING;
                        """).format(sql.Identifier(table_name))
        # on conflict do nothing to prevent throwing errors and creating duplicates

        try:
            self._executemany(query, data)
            self.connection.commit()
            print(f"Successfully added or updated {len(data)} normalised rows into '{table_name}' ദ്ദി（•˕•マ.ᐟ\n")
        except (Exception, psycopg.DatabaseError) as e:
            self.connection.rollback()
            raise DatabaseError(f"Something went wrong with adding the normalised API-data to the table '{table_name}'. Error type: {type(e).__name__}, error message: '{e}'.")

    def add_data_to_wb_indicators_table(self, data: list, table_name: str = "wb_indicators"):
        """persist normalised acquired data into db"""
        if not data:
            print("There is no normalised API-data to add to the database. /ᐠ-˕-マ")
            return

        query = sql.SQL("""
                        INSERT INTO {} (indicator_id, indicator_name, source_id, description)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (indicator_id) DO NOTHING;
                        """).format(sql.Identifier(table_name))
        # on conflict do nothing to prevent throwing errors and creating duplicates

        try:
            self._executemany(query, data)
            self.connection.commit()
            print(f"Successfully added or updated {len(data)} normalised rows into '{table_name}' ദ്ദി（•˕•マ.ᐟ\n")
        except (Exception, psycopg.DatabaseError) as e:
            self.connection.rollback()
            raise DatabaseError(f"Something went wrong with adding the normalised API-data to the table '{table_name}'. Error type: {type(e).__name__}, error message: '{e}'.")

    def add_data_to_wb_indicator_topics_table(self, data: list, table_name: str = "wb_indicator_topics"):
        """persist normalised acquired data into db"""
        if not data:
            print("There is no normalised API-data to add to the database. /ᐠ-˕-マ")
            return

        query = sql.SQL("""
                        INSERT INTO {} (indicator_id, topic_id)
                        VALUES (%s, %s)
                        ON CONFLICT (indicator_id, topic_id) DO NOTHING;
                        """).format(sql.Identifier(table_name))
        # on conflict do nothing to prevent throwing errors and creating duplicates

        try:
            self._executemany(query, data)
            self.connection.commit()
            print(f"Successfully added or updated {len(data)} normalised rows into '{table_name}' ദ്ദി（•˕•マ.ᐟ\n")
        except (Exception, psycopg.DatabaseError) as e:
            self.connection.rollback()
            raise DatabaseError(f"Something went wrong with adding the normalised API-data to the table '{table_name}'. Error type: {type(e).__name__}, error message: '{e}'.")

#######################################
# Run the API requests
#######################################
if __name__ == "__main__":
    print("Hello from api_logger!")
    api_data = get_country_general_info()
    wb_api_db = ApiDB()
    wb_api_db.add_data_to_staging_country_general_info_table(api_data)

    display_all = os.getenv("DISPLAY_ALL_EU_COUNTRIES_INFO", "false").strip().lower() in ("1", "true", "yes")
    if display_all:
        wb_api_db.get_all_eu_countries_info()
    else:
        print(f"\n--- The user does not wish to display all European countries' general info (•́ ᴖ •̀) ---")
        print("--- (if you changed your mind, change the var DISPLAY_ALL_EU_COUNTRIES_INFO to 'true' in 'docker compose' - service 'app_base' environment.) ---")

    names = os.getenv("COUNTRIES_OF_INTEREST", "").strip()
    if names:
        wb_api_db.get_country_info(names)
    else:
        print("\n--- Printing general country info for the countries of interest: No info about countries of interest was given ^. .^₎⟆ ---")

    normalised_api_data_region = [(country_tuple[4], country_tuple[5], country_tuple[3]) for country_tuple in api_data]
    wb_api_db.add_data_to_region_table(normalised_api_data_region)

    normalised_api_data_country_general = [(country_tuple[0], country_tuple[1], country_tuple[2], country_tuple[4], country_tuple[6], country_tuple[7], country_tuple[8], country_tuple[9]) for country_tuple in api_data]
    wb_api_db.add_data_to_country_general_info_table(normalised_api_data_country_general)

    normalised_api_data_alias = [(country_tuple[2], country_tuple[0]) for country_tuple in api_data]
    wb_api_db.add_data_to_country_alias_table(normalised_api_data_alias)

    print("Adding additional country aliases...")
    other_country_aliases = [
        ('Macedonia', 'MKD'),
        ('Czech Republic', 'CZE'),
        ('Czechia', 'CZE'),
        ('United Kingdom', 'GBR'),
        ('Great Britain', 'GBR'),
        ('UK', 'GBR'),
        ('Russian Federation', 'RUS'),
        ('Russia', 'RUS'),
        ('Kosovo', 'XKX'),
        ('Turkiye', 'TUR'),
        ('Turkey', 'TUR'),
        ('Hong Kong', 'HKG'),
        ("Venezuela", "VEN"),
        ("South Korea", "KOR"),
        ("Vietnam", "VNM"),
        ("Egypt", "EGY"),
        ("Ivory Coast", "CIV"),
        ("Slovakia", "SVK"),
        ("Yemen", "YEM"),
        ("Gambia", "GMB"),
        ("Iran", "IRN"),
        ("Kyrgyzstan", "KGZ"),
        ("Syria", "SYR"),
        ("Democratic Republic of the Congo", "COD"),
        ("Laos", "LAO"),
        ("Somalia", "SOM"),
        ("Cape Verde", "CPV"),
        ("Republic of the Congo", "COG"),
        ("Saint Lucia", "LCA"),
        ("Saint Vincent and the Grenadines", "VCT"),
        ("North Korea", "PRK"),
        ("Bahamas", "BHS"),
        ("FYR Macedonia", "MKD"),
        ("Guinea Bissau", "GNB"),
        ("Swaziland", "SWZ"),
        ("Timor Leste", "TLS"),
        ("Macau", "MAC"),
        ("Congo", "COG"),
        ("Puerto Rico", "PRI"),
        ("Palestine", "PSE")
    ]
    # unresolved: ("Serbia and Montenegro", ""), ("Taiwan", "TWN"), ("FR Yugoslavia", "YUG"), and ("Congo", "COG / COD")
    wb_api_db.add_data_to_country_alias_table(other_country_aliases)

    wb_topics_rows = get_all_wb_topics()
    wb_api_db.add_data_to_wb_topics_table(wb_topics_rows)

    wb_sources_rows = get_all_wb_sources()
    wb_api_db.add_data_to_wb_source_table(wb_sources_rows)

    wb_indicators_rows, indicator_ids, indicator_topics_rows = get_all_wb_indicators()
    wb_api_db.add_data_to_wb_indicators_table(wb_indicators_rows)
    wb_api_db.add_data_to_wb_indicator_topics_table(indicator_topics_rows)

    wb_api_db.close_connection()


