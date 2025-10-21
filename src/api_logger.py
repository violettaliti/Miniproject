import requests, save_data

# Full list of World Bank ISO2 codes for countries in Europe


def get_European_country_general_info():
    try:
        url = "https://api.worldbank.org/v2/country/AT;BE;BG;HR;CY;CZ;DK;EE;FI;FR;DE;GR;HU;IS;IE;IT;LV;LT;LU;MT;NL;NO;PL;PT;RO;SK;SI;ES;SE;CH;GB;AL;BA;RS;MK;ME;XK;UA;MD;BY?format=json"
        response = requests.get(url, timeout=5)
        print("\nQueried URL:", response.url, "\n")

        if response.status_code == 200:
            country_info = response.json()
            # print(country_info)

            country_code_all = []
            country_name_all = []
            country_income_level_all = []
            country_capital_city_all = []
            country_longitude_all = []
            country_latitude_all = []

            for country_dict in country_info[1]:
                country_code = country_dict["iso2Code"]
                country_code_all.append(country_code)

                country_name = country_dict["name"]
                country_name_all.append(country_name)

                country_income_level = country_dict["incomeLevel"]["value"]
                country_income_level_all.append(country_income_level)

                country_capital_city = country_dict["capitalCity"]
                country_capital_city_all.append(country_capital_city)

                country_longitude = country_dict["longitude"]
                country_longitude_all.append(country_longitude)

                country_latitude = country_dict["latitude"]
                country_latitude_all.append(country_latitude)

            print("....Collecting data.... (•˕•マ.ᐟ \n")
            print(f"\nList of country codes:\n{country_code_all}\n")
            print(f"\nList of country names:\n{country_name_all}\n")
            print(f"\nList of country income levels:\n{country_income_level_all}\n")
            print(f"\nList of country capital city:\n{country_capital_city_all}\n")
            print(f"\nList of country longitude:\n{country_longitude_all}\n")
            print(f"\nList of country latitude:\n{country_latitude_all}\n")
            print("\n ---- Finish collecting data! ᓚ₍⑅^..^₎♡ ----\n")

        else:
                print(f"Something went wrong, I couldn't fetch the requested data /ᐠ-˕-マ. Error status code: {response.status_code}.")

    except requests.exceptions.RequestException as e:
        print(f"Something went wrong ૮₍•᷄  ༝ •᷅₎ა --> Error message: {type(e)} - {e}.")

if __name__ == "__main__":
    get_European_country_general_info()
    save_data()
