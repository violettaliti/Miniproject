SET search_path TO thi_miniproject;

------------------------------------------------------------------
-- General generic / helper tables
------------------------------------------------------------------
SELECT * FROM year;
SELECT * FROM country_alias;

------------------------------------------------------------------
-- Data from WorldBank API requests
------------------------------------------------------------------
---- staging tables ----
SELECT * FROM staging_country_general_info;

---- main tables ----
SELECT * FROM country_general_info;
SELECT * FROM region;
SELECT * FROM wb_source;
SELECT * FROM wb_topics;
SELECT * FROM wb_indicators;
SELECT * FROM wb_indicator_topics;
SELECT * FROM wb_indicator_country_year_value;

---- example dql queries ----
-- 1/.
SELECT * FROM country_general_info
	WHERE country_name ILIKE '%china%';

-- 2/. which topic has the most total indicator count
SELECT t.topic_id, t.topic_name, 
	COUNT(*) AS total_indicator_count
FROM wb_indicator_topics AS it
JOIN wb_topics AS t
	ON it.topic_id = t.topic_id
GROUP BY t.topic_id, t.topic_name
	ORDER BY total_indicator_count DESC;

-- 3/. which source has the most total indicator count
SELECT s.source_id, s.source_name,
	COUNT(*) AS total_indicator_count
FROM wb_indicators AS i
JOIN wb_source AS s
	ON s.source_id = i.source_id
GROUP BY s.source_id, s.source_name
	ORDER BY total_indicator_count DESC;

-- 4/. indicators have gdp in their name
SELECT * FROM wb_indicators
	WHERE indicator_name ILIKE '%gdp%';
	
------------------------------------------------------------------
-- Data from web scraping (e.g. Corruption Perception Index)
------------------------------------------------------------------
---- staging tables ----
SELECT * FROM staging_cpi_raw;

---- main tables ----
SELECT * FROM corruption_perception_index;

---- views ----
SELECT * FROM thi_miniproject.v_cpi_with_region;
SELECT * FROM thi_miniproject.v_cpi_latest;

---- example dql queries ----
-- 1/.
SELECT * FROM staging_cpi_raw 
	ORDER BY year, country_name;
	
-- 2/.
SELECT * FROM staging_cpi_raw
	WHERE country_name ILIKE '%brunei%';
	
-- 3/.latest CPI per country
SELECT * FROM thi_miniproject.v_cpi_latest 
	ORDER BY country_name;

-- 4/. all CPI rows with names
SELECT * FROM thi_miniproject.v_cpi_with_region 
	ORDER BY country_name, year DESC;

-- 5/. region-level average for 2024
SELECT region_name, ROUND(AVG(cpi_score), 2) AS avg_cpi_2024
FROM thi_miniproject.v_cpi_with_region
	WHERE year = 2024
	GROUP BY region_name
ORDER BY avg_cpi_2024 DESC;

------------------------------------------------------------------
-- sanity checks
------------------------------------------------------------------
-- CPI - quick count comparison
SELECT (SELECT COUNT(*) FROM thi_miniproject.staging_cpi_raw) AS staging_count,
       (SELECT COUNT(*) FROM thi_miniproject.corruption_perception_index) AS final_count;

-- CPI - unmatched names (not resolvable by alias or canonical name)
WITH norm AS (
  SELECT r.*,
         unaccent(lower(trim(r.country_name))) AS nname
  FROM thi_miniproject.staging_cpi_raw r
)
SELECT n.country_name, COUNT(*) AS rows_unmatched
FROM norm n
LEFT JOIN thi_miniproject.country_general_info c
  ON unaccent(lower(trim(c.country_name))) = n.nname
LEFT JOIN thi_miniproject.country_alias a
  ON unaccent(lower(trim(a.country_name_alias))) = n.nname
WHERE c.country_iso3code IS NULL AND a.country_iso3code IS NULL
GROUP BY n.country_name
ORDER BY rows_unmatched DESC;