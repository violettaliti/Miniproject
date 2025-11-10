# imports
import unittest
from src.save_data import DBPostgres
from datetime import datetime
from decimal import Decimal
from psycopg import sql

class TestDBPostgres(unittest.TestCase):
    """this unittest class doesn’t mock postgreSQL, rather, it creates a real unittest schema (thi_test), runs DBPostgres class against it, and checks that the database operations work."""
    @classmethod
    def setUpClass(cls):
        """this runs once before all tests, it connects to admin database and prepares test schema."""
        print("\nSetting up integration (not mock) test environment... ₍^. .^₎⟆\n")
        cls.db = DBPostgres()
        cls.cursor = cls.db.cursor

        # create a dedicated test schema 'thi_test'
        cls.cursor.execute("CREATE SCHEMA IF NOT EXISTS thi_test;")
        cls.cursor.execute("SET search_path TO thi_test;")
        cls.db.connection.commit()
        print("Created and switched to schema 'thi_test' ₍^. .^₎Ⳋ...\n")

        # create a test table in thi_test
        cls.cursor.execute("""
            CREATE TABLE IF NOT EXISTS thi_test.test_table (
                id SERIAL PRIMARY KEY,
                name TEXT,
                cat_score NUMERIC(5,2),
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        cls.db.connection.commit()
        print("Created test_table in schema 'thi_test'! (•˕ •マ.ᐟ\n ")

    @classmethod
    def tearDownClass(cls):
        """this runs once after all tests to clean up the test schema --> no db unittest is left behind."""
        print("\n... Cleaning up integration test environment... ₍^. .^₎Ⳋ\n")
        # if the last test closed the cursor, reopen it
        if getattr(cls.cursor, "closed", False):
            print("Cursor was closed by a previous test, reopening for cleanup ...")
            cls.cursor = cls.db.connection.cursor()
        # drop schema, commit and close cursor connection
        cls.cursor.execute("DROP SCHEMA IF EXISTS thi_test CASCADE;")
        cls.db.connection.commit()
        print("\n ---- All tests done - test schema dropped ᓚ₍^..^₎! ---- bye!")
        cls.db.close_connection()

    def setUp(self):
        """just in case, truncate table (remove all existing rows) before each test."""
        if self.db.cursor is None or getattr(self.db.cursor, "closed", False):
            self.db.cursor = self.db.connection.cursor()
            self.cursor = self.db.cursor
        else:
            self.cursor = self.db.cursor

        self.cursor.execute("SET search_path TO thi_test;")
        self.cursor.execute("TRUNCATE thi_test.test_table RESTART IDENTITY;")
        self.db.connection.commit()

    def test_connection_is_valid(self):
        """ensure db connection and schema are active"""
        self.cursor.execute("SHOW search_path;")
        path = self.cursor.fetchone()[0]
        print("\n- Current search_path:", path)
        self.assertIn("thi_test", path) # verifies that the path appears in the schema 'thi_test'

    def test_connect_with_retry_failure(self):
        """prove it actually retries and surfaces an error, without slowing down the whole process"""
        bad_conn = {
            "dbname": "worldbank",
            "user": "user",
            "password": "katzi",
            "host": "invalid_host",
            "port": 5432,
        }
        with self.assertRaises(Exception):
            DBPostgres.connect_with_retry(bad_conn, retries = 2, delay = 0.1)

    def test_insert_and_select_data(self):
        """insert data into the test table and read it back."""
        data = [("cat 1", 1.5), ("cat 2", 2.2)]
        self.db._executemany(
            "INSERT INTO thi_test.test_table (name, score) VALUES (%s, %s);", data
        )
        self.cursor.execute("SELECT name, score FROM thi_test.test_table ORDER BY id;")
        rows = self.cursor.fetchall()
        print("\n- Inserted rows:", rows)
        self.assertEqual(len(rows), 2) # check for equals
        self.assertEqual(rows[0][0], "cat 1")

    def test_executemany_with_sql_SQL_object(self):
        """ensure the isinstance(query_sql, sql.SQL) is correct"""
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS thi_test.many_cats (
                id SERIAL PRIMARY KEY,
                name TEXT,
                score NUMERIC(5,2)
            );
        """)
        self.db.connection.commit()
        # build a SQL query safely - sql.SQL(..) helps combine Python variables with SQL code and prevents SQL injection
        query = sql.SQL("INSERT INTO {} (name, score) VALUES (%s, %s);") \
            .format(sql.Identifier("many_cats"))
        rows = [("cat 1", 1.1), ("cat 2", 2.2), ("cat 3", 3.3)]
        self.db._executemany(query, rows)

        self.cursor.execute("SELECT COUNT(*) FROM thi_test.many_cats;")
        cat_count = self.cursor.fetchone()[0]
        self.assertEqual(cat_count, 3)

    def test_rollback_on_error(self):
        """force an error to ensure rollback works."""
        self.cursor.execute("INSERT INTO thi_test.test_table (name, score) VALUES ('cat3', 3.3);")
        self.db.connection.commit()
        with self.assertRaises(Exception):
            self.db._executemany("INSERT INTO thi_test.test_table (non_existing_col) VALUES (%s);", [("meow",)])
        self.cursor.execute("SELECT COUNT(*) FROM thi_test.test_table;")
        count = self.cursor.fetchone()[0]
        print("\n- Row count after failed insert:", count)
        self.assertEqual(count, 1)

    def test_pretty_row_output(self):
        """check that decimal and datetime formats work"""
        columns = ["score", "created_at"]
        row = [Decimal("42.42"), datetime(2020, 2, 14, 12, 0, 0)]
        result = self.db._pretty_row(columns, row)
        print("\n- Pretty row:", result)
        self.assertIn("score: 42.42", result)
        self.assertIn("created_at: 2020-02-14", result)

    def test_drop_table_and_recreate(self):
        """drop and recreate the test table to ensure _drop_table works"""
        self.db._drop_table("test_table")
        self.cursor.execute("SELECT to_regclass('thi_test.test_table');")
        exists = self.cursor.fetchone()[0]
        print("\n- Table exists after drop:", exists)
        self.assertIsNone(exists)
        # recreate table to test again
        self.cursor.execute("""
            CREATE TABLE thi_test.test_table (
                id SERIAL PRIMARY KEY,
                name TEXT,
                score NUMERIC(5,2),
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        self.db.connection.commit()
        print("\n... Recreated test_table successfully... (•˕ •マ.ᐟ")

    def test_close_connection(self):
        """ensure connection can close properly"""
        self.db.close_connection()
        with self.assertRaises(Exception):
            self.db.cursor.execute("SELECT 1;")  # this should fail because cursor is closed
        # reopen for next tests
        self.db = DBPostgres()
        self.cursor = self.db.cursor

if __name__ == "__main__":
    unittest.main()
