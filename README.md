Thi Hoang | 14.10.2025

# Thi's Miniproject ₍^. .^₎⟆

## Project goal:

To carry out a mini project and datapipeline using `APIs`, `web scraping`, `Docker`, `Git`, databases (`PostgreSQL`, `pgAdmin4`) and data visualisation tool (`Power BI`).

***Topic***: still brainstorming.

## Project flow

- [ ] Choose a topic /•᷅‎‎•᷄\੭ ❯❯❯❯ choose relevant APIs
- [ ] Dockerise the process ❯❯❯❯ `Dockerfile` + `Dockerimage` + `Dockercompose`
  - [ ] Docker prototype
- [ ] Retrieve data from `APIs` ❯❯❯❯ redefine my project goals based on the available and potential data retrieved
  - [ ] API prototype
- [ ] Scrape complementary data from `websites` ❯❯❯❯ see the list below
  - [ ] Web scraper prototype
- [ ] Store the acquired data in `PostgreSQL` ❯❯❯❯ `pgAdmin4` 
  - [ ] Database prototype
- [ ] Clean, transform and export data for visualisation ❯❯❯❯ `Power BI` 
- [ ] Connect `Power BI` to `PostgreSQL` database container
- [ ] Play with visualisation 📊
- [ ] Tidy up Git (e.g. API keys)
- [ ] Write AI models to play with the processed data (mostly supervised learning)
  - [ ] AI models prototype
- [ ] Wrap up the project ^. .^₎ฅ

## Project details:

- APIs:

- Websites:

## Project structure:
```bash
miniproject/
├─ README.md
├─ .gitignore
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
   ├─ db/
   └─ init/
     └─ schema.sql # DDL in 3Nf
```