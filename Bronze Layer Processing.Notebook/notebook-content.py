# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "59adc769-c4f6-448a-aaca-4e152882c0fe",
# META       "default_lakehouse_name": "BronzeLakehouse",
# META       "default_lakehouse_workspace_id": "9f45cf09-d170-4231-8301-6c69cdbcf006",
# META       "known_lakehouses": [
# META         {
# META           "id": "59adc769-c4f6-448a-aaca-4e152882c0fe"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests
import json
from datetime import datetime

ingestion_date = datetime.now().date()

current_year = datetime.now().year
start_year = current_year - 5

sources = {
    
    'ue_pork_prices': {
        'url': 'https://ec.europa.eu/agrifood/api/pigmeat/prices',
        'parser': lambda r: r.json(),
        'format' : 'json'
    },
    'ue_pork_production': {
        'url': 'https://ec.europa.eu/agrifood/api/pigmeat/production',
        'parser': lambda r: r.json(),
        'format' : 'json'
    },
    'population': {
        'url' : f'https://api.worldbank.org/v2/country/all/'
                f'indicator/SP.POP.TOTL'
                f'?date={start_year}:{current_year}'
                f'&format=json&per_page=20000',
        'parser' : lambda r: r.json()[1] if len(r.json()) > 1 else [],
        'format' : 'json'
    },
    'gdp_per_capita': {
        'url' : f'https://api.worldbank.org/v2/country/all/indicator/NY.GDP.PCAP.CD'
                f'?date={start_year}:{current_year}'
                f'&format=json&per_page=20000',
        'parser' : lambda r: r.json()[1] if len(r.json()) > 1 else [],
        'format' : 'json'
    },
    'pork_consumption_per_capita' : {
        'url' : f'https://ourworldindata.org/explorers/global-food.csv?v=1&csvType=full'
                f'&useColumnShortNames=true&Food=Meat%2C+pig'
                f'&Metric=Allocated+to+human+food&Per+capita=true',
        'parser' : lambda r: r.text,
        'format' : 'csv'
    },
    'pork_consumption' : {
        'url' : f'https://ourworldindata.org/explorers/global-food.csv?v=1&csvType=full'
                f'&useColumnShortNames=true&Food=Meat%2C+pig'
                f'&Metric=Allocated+to+human+food&Per+capita=false',
        'parser' : lambda r: r.text,
        'format' : 'csv'
    },
    'pork_domestic_supply' : {
        'url' : f'https://ourworldindata.org/explorers/global-food.csv?v=1&csvType=full'
                f'&useColumnShortNames=true&Food=Meat%2C+pig'
                f'&Metric=Domestic+supply&Per+capita=false',
        'parser' : lambda r: r.text,
        'format' : 'csv'
    },
    'pork_production' : {
        'url' : f'https://ourworldindata.org/grapher/pigmeat-production-tonnes.csv?v=1'
                f'&csvType=filtered&useColumnShortNames=true&tab=table&time=2020..latest',
        'parser' : lambda r: r.text,
        'format' : 'csv'
    },
    'gdp_growth' : {
        'url' : f'https://ourworldindata.org/grapher/real-gdp-growth.csv?v=1'
                f'&csvType=filtered&useColumnShortNames=true&tab=table&time=2026..latest',
        'parser' : lambda r: r.text,
        'format' : 'csv'
    },
    'pork_import' : {
        'url' : f'https://ourworldindata.org/explorers/global-food.csv?v=1'
                f'&csvType=filtered&useColumnShortNames=true&tab=table&time=2018..latest'
                f'&Food=Meat%2C+pig&Metric=Imports&Per+capita=false',
        'parser' : lambda r: r.text,
        'format' : 'csv'
    }

}


for key, config in sources.items():
    response = requests.get(config['url'])

    if response.status_code == 200:
      
        data = config['parser'](response)

        if config['format'] == 'json':
            file_path = f'/lakehouse/default/Files/{key}/{key}_{ingestion_date}.json'
            with open(file_path, 'w') as file:
                json.dump(data, file, indent=4)

        elif config['format'] == 'csv': 
            file_path = f'/lakehouse/default/Files/{key}/{key}_{ingestion_date}.csv'
            with open(file_path, 'w') as file:
                file.write(data)

        else:
            raise ValueError(f"Unsupported format: {config['format']}")

        print(f'Data successfully saved to {file_path}')
    else:
        print('Failed to fetch data. Status code:', response.status_code)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# import requests
# import json

# urls = { "prices": "https://ec.europa.eu/agrifood/api/pigmeat/prices",
# "production": "https://ec.europa.eu/agrifood/api/pigmeat/production" }

# for key, url in urls.items():
# # Make the GET request to fetch data
#     response = requests.get(url)

# # Check if the request was successful
#     if response.status_code == 200:
#     # Get the JSON response
#         data = response.json()

#     # Specify the file name (and path if needed)
#         file_path = f'/lakehouse/default/Files/pork_{key}/pork_{key}_data_{ingestion_date}.json'


#     # Open the file in write mode ('w') and save the JSON data
#         with open(file_path, 'w') as file:
#             json.dump(data, file, indent=4)

#         print(f"Data successfully saved to {file_path}")
#     else:
#         print("Failed to fetch data. Status code:", response.status_code)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
