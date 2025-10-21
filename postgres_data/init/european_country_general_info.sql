CREATE schema IF NOT EXISTS thi_miniproject;

SET search_path TO thi_miniproject;

CREATE TABLE european_country_general_info(
	country_code TEXT PRIMARY KEY,
	country_name TEXT,
	country_income_level TEXT,
	country_capital_city TEXT,
	country_longitude NUMERIC,
	country_latitude NUMERIC
);

