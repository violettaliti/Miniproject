Thi Hoang | 10-11/2025

# Thi's Miniproject ₍^. .^₎⟆

## Project goal

To carry out a mini project and datapipeline using `APIs`, `web scraping`, `Docker`, `Git`, databases (`PostgreSQL`, `pgAdmin4`) and data visualisation tool (`Power BI`).

**Topic**: to be confirmed (tbc), but related to World Bank API, data and its indicators.

*Why?*
- Cause I'm an economist by training (gotta stay somewhat connected to my roots ദ്ദി/ᐠ｡‸｡ᐟ\ ...).
- World Bank data and API are rich, multi-layered, highly hierarchical (--> ideal for relational database).
- My imagination runs wild when it comes to what I can potentially scrap the web additionally /ᐠ • ˕ •マ ?...

## Project flow

- [X] Choose an API and its relevant topics /•᷅‎‎•᷄\੭ 
- [X] Dockerise the process ❯❯ `Dockerfile` + `Dockerimage` + `Dockercompose`
  - [X] Docker prototype
- [X] Retrieve data from `APIs` ❯❯ redefine my project goals based on the available and potential data retrieved
  - [X] API prototype
- [ ] Scrape complementary data from `websites` ❯❯ see the list below
  - [ ] Web scraper prototype
- [ ] Store the acquired data in `PostgreSQL` ❯❯ `pgAdmin4` 
  - [ ] Database prototype
- [ ] Clean, transform and export data for visualisation ❯❯ `Power BI` 
- [ ] Connect `Power BI` to `PostgreSQL` database container
- [ ] Play with visualisation 📊
- [ ] Tidy up Git (e.g. API keys)
- [ ] Write AI models to play with the processed data (mostly supervised learning)
  - [ ] AI models prototype
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
│  ├─ exporter.py # export to sql
│  └─ tests/ # unittests
│     ├─ __init__.py
│     ├─ test_api_logger.py
│     ├─ test_web_logger.py
│     └─ test_exporter.py
└─ postgres_data/
   ├─ db/ # actual database files (postgres storage)
   ├─ data/ # for storing CSVs if needed
   └─ init/
     └─ schema.sql # DDL in 3Nf
```
## Project concept
- How is the chosen API used?
  - To collect country-specific data over several time periods.
  - Data to be collected: tbc.

- Specific topics: tbc.
  - Still brainstorming what I want to achieve with the collected data, cause it depends on what data I can collect.
  - Potentially an overview of regional and global data over different economic, geographic, social and political indicators over time.

- Additional data collected via web scraping (still brainstorming):
  - Corruption / governance (CPI – Transparency International).
  - World happiness index / report.

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
  - World Bank indicators API (v2). Example endpoints: 
    - Country list: https://api.worldbank.org/v2/country?format=json&per_page=300 
    - https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL
- Websites:
  - :)