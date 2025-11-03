CREATE schema IF NOT EXISTS thi_miniproject;

SET search_path TO thi_miniproject;

----------------------------------------------------------
-- General generic tables
----------------------------------------------------------
-- year (1960 - 2025)
CREATE TABLE IF NOT EXISTS thi_miniproject.year(
	year INTEGER PRIMARY KEY
);

DO $$ -- anonymous one-time executable block, not a stored function 
	FOR y IN 1960..2025 LOOP
		INSERT INTO thi_miniproject.year(year)
		VALUES (y)
		ON CONFLICT (year) DO NOTHING; -- to not throw errors or duplicate the years
	END LOOP;
END $$
LANGUAGE plpgsql;

----------------------------------------------------------
-- Tables for data from API requests
----------------------------------------------------------
-- european_country_general_info
CREATE TABLE IF NOT EXISTS thi_miniproject.european_country_general_info(
	country_iso3code TEXT PRIMARY KEY,
    country_iso2code TEXT,
	country_name TEXT,
	country_income_level TEXT,
	country_capital_city TEXT,
	country_longitude NUMERIC,
	country_latitude NUMERIC,
    data_source TEXT NOT NULL DEFAULT 'WorldBank API',
    insert_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    update_count INTEGER NOT NULL DEFAULT 0,
    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

----------------------------------------------------------
-- Tables for data from web scraping
----------------------------------------------------------
