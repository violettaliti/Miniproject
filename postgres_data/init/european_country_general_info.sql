CREATE schema IF NOT EXISTS thi_miniproject;

SET search_path TO thi_miniproject;

CREATE TABLE european_country_general_info(
	country_code TEXT PRIMARY KEY,
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

