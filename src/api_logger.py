import os # os is part of python standard library -> no need to add to requirements.txt
import requests
import psycopg2
import time # time is part of python standard library -> no need to add to requirements.txt
from psycopg2 import sql
from dotenv import load_dotenv

#### API requests
# Full list of World Bank ISO2 codes for countries in Europe

def get_European_country_general_info():
    try:
        url = "https://api.worldbank.org/v2/country/AT;BE;BG;HR;CY;CZ;DK;EE;FI;FR;DE;GR;HU;IS;IE;IT;LV;LT;LU;MT;NL;NO;PL;PT;RO;SK;SI;ES;SE;CH;GB;AL;BA;RS;MK;ME;XK;UA;MD;BY?format=json"
        response = requests.get(url, timeout=5)
        print("\nQueried URL:", response.url, "\n")

        if response.status_code == 200:
            country_info = response.json()
            # print(country_info)

            country_code_all = []
            country_name_all = []
            country_income_level_all = []
            country_capital_city_all = []
            country_longitude_all = []
            country_latitude_all = []

            for country_dict in country_info[1]:
                country_code = country_dict["iso2Code"]
                country_code_all.append(country_code)

                country_name = country_dict["name"]
                country_name_all.append(country_name)

                country_income_level = country_dict["incomeLevel"]["value"]
                country_income_level_all.append(country_income_level)

                country_capital_city = country_dict["capitalCity"]
                country_capital_city_all.append(country_capital_city)

                country_longitude = country_dict["longitude"]
                country_longitude_all.append(country_longitude)

                country_latitude = country_dict["latitude"]
                country_latitude_all.append(country_latitude)

            print("....Collecting data.... (•˕•マ.ᐟ \n")
            print(f"\nList of country codes:\n{country_code_all}\n")
            print(f"\nList of country names:\n{country_name_all}\n")
            print(f"\nList of country income levels:\n{country_income_level_all}\n")
            print(f"\nList of country capital city:\n{country_capital_city_all}\n")
            print(f"\nList of country longitude:\n{country_longitude_all}\n")
            print(f"\nList of country latitude:\n{country_latitude_all}\n")
            print("\n ---- Finish collecting data! ᓚ₍⑅^..^₎♡ ----\n")

            rows = list(zip( # zip() transposes a list of columns into a list of rows, suitable for INSERT query later (to add the data to db)
                country_code_all,
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

#### Saving / persisting to db
class DatabaseError(Exception):
    pass

class WorldBankDBPostgres:
    def __init__(self):
        load_dotenv()  # this reads .env locally, in docker env is already there / set
        dbname = os.getenv("DB_NAME", os.getenv("POSTGRES_DB", "worldbank"))  # double fallbacks: if there's no env var name 'DB_NAME', then check for 'POSTGRES_DB', if still fails, use the default 'worldbank'
        user = os.getenv("DB_USER", os.getenv("POSTGRES_USER", "user"))
        password = os.getenv("DB_PASSWORD", os.getenv("POSTGRES_PASSWORD", "katzi"))  # in my .env file, I have a different pw but since it's in .gitignore, we can just use the default pw 'katzi'
        host = os.getenv("DB_HOST", "db")  # use 'db' inside docker container, 'localhost' outside (e.g. in locally installed apps such as pgAdmin)
        port = int(os.getenv("DB_PORT", 5555))  # use 5555 for locally installed apps such as pgAdmin, 5432 for inside the docker container

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
            print("\n𖹭 Connected to database (schema 'thi_miniproject' is set)! 𖹭\n")

        except (Exception, psycopg2.DatabaseError) as e:
            self.connection = psycopg2.connect(dbname = dbname, user = user, password = password, host = host, port = port)
            raise DatabaseError(f"Something went wrong with the connection ≽^- ˕ -^≼ Error type: {type(e).__name__}, error message: '{e}'.")

    @staticmethod
    def connect_with_retry(dsn_kwargs, retries = 15, delay = 3): # helper function
        """
        try to connect to postgres multiple times before giving up.
        useful when db starts slower than the app in docker compose.
        """
        last_err = None
        for attempt in range(1, retries + 1):
            try:
                conn = psycopg2.connect(**dsn_kwargs)
                conn.autocommit = False
                print(f"Connected on attempt # {attempt} ₍^. .^₎⟆")
                return conn
            except psycopg2.OperationalError as e:
                last_err = e
                print(f"Attempt {attempt}: Postgres not ready yet. Retrying in {delay}s...")
                time.sleep(delay)
        raise last_err

    def _create_schema(self, schema_name = "thi_miniproject"):
        try:
            self.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        except (Exception, psycopg2.DatabaseError) as e:
            raise DatabaseError(f"Something went wrong with creating the schema '{schema_name}'. Error type: {type(e).__name__}, error message: '{e}'.")

    def _drop_table(self, table_name):
        try:
            self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.connection.commit()
        except (Exception, psycopg2.DatabaseError) as e:
            raise DatabaseError(f"Something went wrong with dropping the table '{table_name}'. Error type: {type(e).__name__}, error message: '{e}'.")

    # I don't really need this _create_table() method because I wrote DDL for my db separately using pgAdmin and included it in the init folder, but still just for learning purposes
    def _create_table(self,
                    table_name: str = "european_country_general_info",
                    table_ddl: str | None = None
    ):
        if table_ddl is None:
            table_ddl = """
                country_code TEXT PRIMARY KEY,
                country_name TEXT,
                country_income_level TEXT,
                country_capital_city TEXT,
                country_longitude NUMERIC,
                country_latitude NUMERIC, 
                data_source TEXT NOT NULL DEFAULT 'WorldBank API',
                insert_time TIMESTAMP NOT NULL DEFAULT NOW(),
                update_count INTEGER NOT NULL DEFAULT 0,
                last_updated TIMESTAMP NOT NULL DEFAULT NOW()
            """

        build_table = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({});").format( # using sql.Identifier to prevent injection risk (e.g. caused by weird characters), thus safely quote identifiers (table names etc.)
            sql.Identifier(table_name),
            sql.SQL(table_ddl.strip())
        )

        try:
            self.cursor.execute(build_table)
            self.connection.commit()
            print(f"The table '{table_name}' has either been created or already existed. ᓚ₍ ^. ̫ .^₎")
        except (Exception, psycopg2.DatabaseError) as e:
            self.connection.rollback()
            raise DatabaseError(f"Something went wrong with creating the table '{table_name}'. Error type: {type(e).__name__}, error message: '{e}'.")

    def add_data_to_db(self, data: list = None, table_name: str = "european_country_general_info"):
        if not data:
            print("There is no data to add to the database. /ᐠ-˕-マ")
            return

        query = sql.SQL("""
                        INSERT INTO {} (country_code, country_name, country_income_level, country_capital_city,
                                        country_longitude, country_latitude)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (country_code) 
                        DO UPDATE SET
                            country_name = EXCLUDED.country_name,
                            country_income_level = EXCLUDED.country_income_level,
                            country_capital_city = EXCLUDED.country_capital_city,
                            country_longitude = EXCLUDED.country_longitude,
                            country_latitude = EXCLUDED.country_latitude,
                            data_source = EXCLUDED.data_source,
                            update_count = european_country_general_info.update_count + 1,
                            last_updated = NOW()
                        WHERE
                            (european_country_general_info.country_name,
                             european_country_general_info.country_income_level,
                             european_country_general_info.country_capital_city,
                             european_country_general_info.country_longitude,
                             european_country_general_info.country_latitude)
                        IS DISTINCT FROM
                            (EXCLUDED.country_name,
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
        except (Exception, psycopg2.DatabaseError) as e:
            self.connection.rollback()
            raise DatabaseError(f"Something went wrong with adding the data to the table '{table_name}'. Error type: {type(e).__name__}, error message: '{e}'.")





if __name__ == "__main__":
    api_data = get_European_country_general_info()
    # api_data is a list of the following lists: country_code, country_name, country_income_level, country_capital_city, country_longitude, country_latitude
    wb_db = WorldBankDBPostgres()
    wb_db._create_schema()
    wb_db._create_table()
    wb_db.add_data_to_db(api_data)


