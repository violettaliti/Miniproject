Thi Hoang | 10-11/2025

# Thi's Miniproject ₍^. .^₎⟆

## Project goal

To carry out a mini project and datapipeline using `APIs`, `web scraping`, `Docker`, `Git`, databases (`PostgreSQL`, `pgAdmin4`) and data visualisation tool (`Power BI`).

**Topic**: country data and trends over the years from World Bank API and other data sources such as Transparency International (Corruption Perception Index), World Happiness Report.

*Why?*
- Cause I'm an economist by training (gotta stay somewhat connected to my roots ദ്ദി/ᐠ｡‸｡ᐟ\ ...).
- World Bank API data are rich, multi-layered, highly hierarchical (--> ideal for relational database).
- My imagination runs wild when it comes to what I can potentially scrap the web additionally /ᐠ • ˕ •マ ?...

## Project flow

- [X] Choose an API and its relevant topics /•᷅‎‎•᷄\੭ 
- [X] Dockerise the process ❯❯ `Dockerfile` + `Dockerimage` + `Dockercompose`
  - [X] Docker prototype
  - [X] Improve docker compose as needed
- [X] Retrieve data from `APIs` ❯❯ redefine my project goals based on the available and potential data retrieved
  - [X] API prototype
  - [X] Extend API requests - 1
  - [X] Extend API requests - 2
- [X] Scrape complementary data from `websites` ❯❯ see the list below
  - [X] Web scraper prototype
  - [X] Extend data sources for web scraping - 1
- [ ] Store the acquired data in `PostgreSQL` ❯❯ `pgAdmin4` 
  - [X] Database prototype
  - [X] Initial relational database schema
  - [ ] Extend / update relational database schema
  - [X] Extend database with the additional data acquired from extended API requests and web-scraping - 1
  - [ ] Extend database with the additional data acquired from extended API requests and web-scraping - 2
  - [ ] Initial advanced relational database schema (star / galaxy schemas)
- [X] Unittests for save_data.py
- [ ] Clean, transform and export data for visualisation ❯❯ `Power BI` 
- [ ] Connect `Power BI` to `PostgreSQL` database container
- [X] Play with visualisation 📊
- [ ] Tidy up Git (merge branches if needed)
- [ ] Include AI models to play with the processed data (mostly supervised learning)
- [ ] Wrap up the project and go harass my Siamese cats with unsolicited kisses ^. .^₎ฅ

## Project structure

```bash
miniproject/
├─ README.md
├─ .gitignore
├─ .dockerignore
├─ .env # to store API_KEYs
├─ requirements.txt
├─ docker-compose.yml
├─ Dockerfile
├─ src/
│  ├─ __init__.py
│  ├─ api_logger.py # APIs (requests)
│  ├─ web_logger.py # web scraper (requests + BeautifulSoup)
│  ├─ save_data.py # export to sql
│  └─ tests/ # unittests
│     ├─ __init__.py
│     └─ test_save_data.py
└─ postgres_data/
   ├─ db/ # actual database files (postgres storage)
   ├─ data/ # for storing CSVs if needed
   ├─ queries.sql # pre-written sql queries (SELECT statements) for exploring the db
   └─ init/
     └─ schema.sql # DDL in 3NF, triggers, functions, procedures etc.
```
## Project concept

- How is the chosen API used?
  - To collect country-specific data over several time periods.
  - Data collected:
    - 217 countries' general info
    - 7 world regions
    - 71 World Bank data sources (e.g. World Development Indicators, Global Economic Monitor, Education Statistics etc.)
    - 21 World Bank topics (e.g. Economy & growth, education, environment)
    - 29 256 World Bank indicators (e.g. GDP, trade, import/export, education/literacy levels, population demographics etc.)
      - each indicator will then be queried to give values for each country over all available years (mostly from 1960 to 2024)
      --> currently it has collected over 15 million datapoints. 

- Specific topics:
  - Overview of regional and global data over different economic, geographic, social and political indicators over time, e.g.:
    - Economic data (GDP, import/export etc.)
    - Education
    - Social protection & labour
    - Poverty
    - Gender
    - Climate change

- Additional data collected via web scraping:
  - Corruption Perception Index over the years (from Transparency International) (1995-2024)
  - World happiness index / report (2011-2024)

- Potential AI models used for data training and prediction:
  - Supervised learning:
    - Multilinear regression.
      - modelling trends, within-and cross-country patterns.
      - predict GDP growth, unemployment rate, investment rate, inflation etc.
      - predict one indicator using the others (e.g. CO2 emissions from GDP and population).
  - Unsupervised learning:
    - KMeans clustering.
      - group countries by political, socio-economic similarities / differences.

## Project details

- APIs:
  - ```World Bank indicators API (v2)```. Example endpoints: 
    - Country list: https://api.worldbank.org/v2/country?format=json&per_page=300 
    - Data for a specific indicator (e.g. SP.POP.TOTL - population count): https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL
- Websites:
  - Corruption Perception Index – Transparency International:
    - https://www.transparency.org/en/cpi/2024
    - Scraped this page instead because of the Java challenges installed on the TI website:
    https://en.wikipedia.org/wiki/List_of_countries_by_Corruption_Perceptions_Index
  - World Happiness Report:
    - https://data.worldhappiness.report/table

## Requirements
- ```pgAdmin4``` installed locally (I didn't include a service / container for pgAdmin in my docker compose)
- ```Power BI```

## How to start the project using Docker Compose
- Step 1: open the project's folder and copy the folder's path
- Step 2: open the host (your laptop)'s terminal, go to the project's folder using its path
- Step 3: run the following command in your terminal
```
docker compose up --build
```

*which will both build the docker image, run and open the following containers:*
  1. db starts --> healthcheck runs
  2. app_db_test --> runs integration ```unittests```
  3. if all tests pass --> ```api_logger.py``` and ```web_logger.py``` containers start ₍^. .^₎⟆

## How to access to the database using pgAdmin4
- Step 1: install ```pgAdmin4``` (if applicable)
- Step 2: open ```pgAdmin4``` -> right click on ```Servers``` -> ```Register``` -> ```Server```
- Step 3: on the 'General' tab:
  - Name: anything goes (e.g. 'Katzi')
- Step 4: on the ```Connection``` tab:
  - ```Host name/address```: localhost
  - ```Port```: 5555
  - ```Maintenance database```: worldbank
  - ```Username```: user
  - ```Password```: katzi
    - Save password? --> Yes for peace of mind later!!
- Step 5: 
  - > ```Servers``` -> miniproject 
    - > ```Databases``` -> worldbank 
      - > ```Schemas``` -> thi_miniproject
        - > ```Tables``` -> right click 'Refresh' -> Query Tool
- Step 6: type this in the pop-up Query window:
```
SET search_path TO thi_miniproject;
```
then have fun ദ്ദി/ᐠ｡‸｡ᐟ\ !!

## How to connect to the database in PowerBI
- Step 1: install ```Power BI```
- Step 2: click on ```Get data``` on the Home tab of the main ribbon, or ```Get data from another source``` on the main dashboard
- Step 3: on the pop-up window ```Get data```, type in / choose ```PostgreSQL database``` in the search bar
- Step 4: on the pop-up window:
  - ```Server```: localhost:5555
  - ```Database```: worldbank
  - ```Username```: user
  - ```Password```: katzi
- Step 5: Choose the following tables / views:
  - thi_miniproject.year
  - thi_miniproject.country_general_info
  - thi_miniproject.region
  - thi_miniproject.wb_source
  - thi_miniproject.wb_topics
  - thi_miniproject.wb_indicators
  - thi_miniproject.wb_indicator_topics
  - thi_miniproject.wb_indicator_country_year_value
  - thi_miniproject.corruption_perception_index
  - thi_miniproject.world_happiness_report
  - thi_miniproject.v_cpi_with_region
  - thi_miniproject.v_cpi_latest

then have fun ദ്ദി/ᐠ｡‸｡ᐟ\ !!