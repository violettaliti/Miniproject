CREATE schema IF NOT EXISTS thi_miniproject;

SET search_path TO thi_miniproject;

----------------------------------------------------------
-- General generic tables
----------------------------------------------------------
-- year (1960 - 2025)
CREATE TABLE IF NOT EXISTS thi_miniproject.year(
	year INTEGER PRIMARY KEY
		CHECK (year BETWEEN 1900 AND 2100)
);

-- anonymous one-time executable block, not a stored function
-- ON CONFLICT (year) DO NOTHING: to not throw errors or duplicate the years
DO $$
BEGIN
	FOR y IN 1960..2025 LOOP
		INSERT INTO thi_miniproject.year(year)
		VALUES (y)
		ON CONFLICT (year) DO NOTHING; 
	END LOOP;
END; 
$$
LANGUAGE plpgsql;

----------------------------------------------------------
-- Tables for data from API requests
----------------------------------------------------------
-- country_general_info table
CREATE TABLE IF NOT EXISTS thi_miniproject.country_general_info(
	country_iso3code TEXT PRIMARY KEY,
    country_iso2code TEXT,
	country_name TEXT,
	region_name TEXT,
	region_id TEXT,
	region_iso2code TEXT,
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
-- staging CPI table
CREATE TABLE IF NOT EXISTS thi_miniproject.staging_cpi_raw(
	country_name TEXT NOT NULL,
	year INTEGER NOT NULL,
	cpi_score NUMERIC(5, 2) CHECK (cpi_score BETWEEN 0 AND 100)
);

-- final corruption perception index (CPI) table
CREATE TABLE IF NOT EXISTS thi_miniproject.corruption_perception_index(
	country_iso3code TEXT REFERENCES country_general_info(country_iso3code),
	country_name TEXT NOT NULL,
	year INTEGER NOT NULL REFERENCES year(year),
	cpi_score NUMERIC(5, 2) CHECK (cpi_score BETWEEN 0 AND 100),
	PRIMARY KEY(country_iso3code, year)
);