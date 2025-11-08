# imports
import os # part of python standard library -> no need to add to requirements.txt
import psycopg
import time # part of python standard library
from psycopg import sql
from dotenv import load_dotenv
from decimal import Decimal # part of python standard library
from datetime import datetime # part of python standard library

class DatabaseError(Exception):
    pass

class DBPostgres:
    """
    parent class: handles connection, retries, helpers, and shared utilities
    children: ApiDB, WebDB inherit from this class
    """
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
        print(f".... Connecting to host '{host}' : port '{port}' .....\n")

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
            self.connection = psycopg.connect(dbname = dbname, user = user, password = password, host = host, port = port) # trying one last time
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

    # shared helpers
    def _executemany(self, query_sql: sql.SQL | str, rows: list[tuple]):
        """execute many rows at once"""
        try:
            if isinstance(query_sql, sql.SQL):
                self.cursor.executemany(query_sql.as_string(self.connection), rows)
            else:
                self.cursor.executemany(query_sql, rows)
            self.connection.commit()
        except (Exception, psycopg.DatabaseError) as e:
            self.connection.rollback()
            raise

    def _drop_table(self, table_name: str):
        """drop table as needed"""
        try:
            self.cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table_name)))
            self.connection.commit()
        except (Exception, psycopg.DatabaseError) as e:
            raise DatabaseError(f"Something went wrong with dropping the table '{table_name}'. Error type: {type(e).__name__}, error message: '{e}'.")

    @staticmethod
    def _pretty_row(columns, row):
        output = []
        for col, val in zip(columns, row):
            if isinstance(val, Decimal):
                val = float(val)
            elif isinstance(val, datetime):
                val = val.strftime("%Y-%m-%d %H:%M:%S %Z")
            output.append(f"{col}: {val}")
        return ", ".join(output)

    def __str__(self):
        """replace the string special method to automatically display the db name"""
        return f"WorldBank PostgreSQL database (schema: thi_miniproject)"

    def close_connection(self):
        try:
            self.cursor.close()
        except (Exception, psycopg.DatabaseError) as e:
            raise DatabaseError(f"Something went wrong with closing the connection. Error type: {type(e).__name__}, error message: '{e}'.")

if __name__ == "__main__":
    print("Hello from save_data!")
    test = DBPostgres()
    pass