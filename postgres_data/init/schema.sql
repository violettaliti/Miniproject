CREATE schema IF NOT EXISTS thi_miniproject;

SET search_path TO thi_miniproject;

----------------------------------------------------------
-- General helper
----------------------------------------------------------
-- helper for accent-/case-insensitive matches
CREATE EXTENSION IF NOT EXISTS unaccent; -- activates Postgres’s built-in accent-remover so we can safely compare strings

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
-- staging_country_general_info table
CREATE TABLE IF NOT EXISTS thi_miniproject.staging_country_general_info(
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

-- region table
CREATE TABLE IF NOT EXISTS thi_miniproject.region(	
	region_id TEXT PRIMARY KEY,
	region_iso2code TEXT,
	region_name TEXT NOT NULL
);

-- country_general_info table
CREATE TABLE IF NOT EXISTS thi_miniproject.country_general_info(
	country_iso3code TEXT PRIMARY KEY,
    country_iso2code TEXT,
	country_name TEXT,
	region_id TEXT NOT NULL REFERENCES thi_miniproject.region(region_id),
	country_income_level TEXT,
	country_capital_city TEXT,
	country_longitude NUMERIC,
	country_latitude NUMERIC
);

-- country name alias table
CREATE TABLE IF NOT EXISTS thi_miniproject.country_alias (
	country_name_alias TEXT PRIMARY KEY,
	country_iso3code TEXT NOT NULL REFERENCES thi_miniproject.country_general_info(country_iso3code)
);

-- world bank topics table
CREATE TABLE IF NOT EXISTS thi_miniproject.wb_topics (
	topic_id INTEGER PRIMARY KEY,
	topic_name TEXT NOT NULL,
	description TEXT NOT NULL
);

-- world bank source table
CREATE TABLE IF NOT EXISTS thi_miniproject.wb_source (
	source_id INTEGER PRIMARY KEY,
	source_name TEXT NOT NULL,
	source_code TEXT,
	data_availability BOOLEAN,
	metadata_availability BOOLEAN,
	concepts INTEGER,
	last_updated DATE
);

-- world bank indicators table
CREATE TABLE IF NOT EXISTS thi_miniproject.wb_indicators (
	indicator_id TEXT PRIMARY KEY,
	indicator_name TEXT NOT NULL,
	source_id INTEGER NOT NULL REFERENCES thi_miniproject.wb_source(source_id),
	description TEXT NOT NULL
);

-- world bank indicator_topics table
CREATE TABLE IF NOT EXISTS thi_miniproject.wb_indicator_topics (
	indicator_id TEXT REFERENCES thi_miniproject.wb_indicators(indicator_id),
	topic_id INTEGER REFERENCES thi_miniproject.wb_topics(topic_id),
	PRIMARY KEY(indicator_id, topic_id)
);

-- long fact table
CREATE TABLE IF NOT EXISTS wb_indicator_country_year_value (
	indicator_id TEXT NOT NULL REFERENCES thi_miniproject.wb_indicators(indicator_id),
	country_iso3code TEXT NOT NULL REFERENCES thi_miniproject.country_general_info(country_iso3code),
	year INTEGER NOT NULL REFERENCES thi_miniproject.year(year),
	value NUMERIC,
	PRIMARY KEY (indicator_id, country_iso3code, year)
);

-- indexes for faster queries
CREATE INDEX IF NOT EXISTS idx_wb_indicator_id ON wb_indicator_country_year_value (indicator_id);
CREATE INDEX IF NOT EXISTS idx_wb_year ON wb_indicator_country_year_value (year);

----------------------------------------------------------
-- Tables for data from web scraping
----------------------------------------------------------
-- staging CPI table
CREATE TABLE IF NOT EXISTS thi_miniproject.staging_cpi_raw(
	country_name TEXT NOT NULL,
	year INTEGER NOT NULL,
	cpi_score NUMERIC(5, 2) CHECK (cpi_score BETWEEN 0 AND 100),
	CONSTRAINT country_year_unique_check UNIQUE (country_name, year)
);

-- final corruption perception index (CPI) table
CREATE TABLE IF NOT EXISTS thi_miniproject.corruption_perception_index(
	country_iso3code TEXT REFERENCES thi_miniproject.country_general_info(country_iso3code),
	year INTEGER NOT NULL REFERENCES thi_miniproject.year(year),
	cpi_score NUMERIC(5, 2) CHECK (cpi_score BETWEEN 0 AND 100),
	PRIMARY KEY(country_iso3code, year)
);

----------------------------------------------------------
-- Trigger functions
----------------------------------------------------------
CREATE OR REPLACE FUNCTION thi_miniproject.staging_cpi_to_final()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    c_iso3 TEXT;
BEGIN
    -- 1) try alias match first
    SELECT ca.country_iso3code
      INTO c_iso3
      FROM thi_miniproject.country_alias AS ca
     WHERE unaccent(lower(ca.country_name_alias))
           = unaccent(lower(NEW.country_name))
     LIMIT 1;

    -- 2) fallback: direct match against wb official country names
    IF c_iso3 IS NULL THEN
        SELECT cgi.country_iso3code
          INTO c_iso3
          FROM thi_miniproject.country_general_info AS cgi
         WHERE unaccent(lower(cgi.country_name))
               = unaccent(lower(NEW.country_name))
         LIMIT 1;
    END IF;

    -- 3) if still unknown, just log and keep the staging row
    IF c_iso3 IS NULL THEN
        RAISE NOTICE 'No country_iso3code match for "%" (year %); leaving row in staging.',
                     NEW.country_name, NEW.year;
        RETURN NEW;
    END IF;

    -- 4) upsert into final CPI table (idempotent)
    INSERT INTO thi_miniproject.corruption_perception_index
           (country_iso3code, year, cpi_score)
    VALUES (c_iso3, NEW.year, NEW.cpi_score)
    ON CONFLICT (country_iso3code, year)
    DO UPDATE SET
        cpi_score = EXCLUDED.cpi_score;

    RETURN NEW; -- keep the staging row as audit trail
END;
$$;

----------------------------------------------------------
-- Triggers
----------------------------------------------------------
DROP TRIGGER IF EXISTS trg_staging_cpi_to_final
    ON thi_miniproject.staging_cpi_raw;

CREATE TRIGGER trg_staging_cpi_to_final
AFTER INSERT ON thi_miniproject.staging_cpi_raw
FOR EACH ROW
EXECUTE FUNCTION thi_miniproject.staging_cpi_to_final();

----------------------------------------------------------
-- Views
----------------------------------------------------------
-- CPI with country + region info 
CREATE OR REPLACE VIEW thi_miniproject.v_cpi_with_region AS
SELECT
  cpi.country_iso3code,
  cgi.country_iso2code,
  cgi.country_name,
  r.region_id,
  r.region_iso2code,
  r.region_name,
  cpi.year,
  cpi.cpi_score
FROM thi_miniproject.corruption_perception_index AS cpi
JOIN thi_miniproject.country_general_info AS cgi
	USING (country_iso3code)
LEFT JOIN thi_miniproject.region AS r
	ON r.region_id = cgi.region_id;

-- latest CPI per country (one row per country)
CREATE OR REPLACE VIEW thi_miniproject.v_cpi_latest AS
SELECT DISTINCT ON (cpi.country_iso3code)
  cpi.country_iso3code,
  cgi.country_iso2code,
  cgi.country_name,
  cpi.year,
  cpi.cpi_score
FROM thi_miniproject.corruption_perception_index AS cpi
JOIN thi_miniproject.country_general_info AS cgi
	USING (country_iso3code)
ORDER BY cpi.country_iso3code, cpi.year DESC;  
