import os
from dotenv import load_dotenv
import requests
import json

load_dotenv('../project.env')

# Create local directory if needed
file_dir = os.getenv('download_folder')
os.makedirs(file_dir, exist_ok=True)

# for future use to customise the file type being downloaded
file_type = 'json'

# Base URL for the  data
base_url = 'https://opendata.ecdc.europa.eu/covid19/'

# List of endpoint paths to fetch data from
endpoint_list = [
    'COVID-19_VC_data_from_September_2023/json/data_v7.json', 
    'vaccine_tracker/json/',
    'movementindicators/json/',
    'movementindicatorsarchive/json/data.json',
    'nationalcasedeath/json/',
    'nationalcasedeath_archive/json/',
    'nationalcasedeath_eueea_daily_ei/json/'
]

# Download each dataset
for endpoint in endpoint_list:
    url = f"{base_url}{endpoint}"
    
    # Generate filename
    base_part = endpoint.strip('/').split('/')[0].replace('_', '-').lower()
    filename = os.path.join(file_dir, f"{base_part}.json")

    print(f"Fetching: {url}")
    print(f"Saving as: {filename}")

    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises HTTPError for bad responses (4xx/5xx)

        with open(filename, 'w', encoding='utf-8') as file:
            file.write(response.text)

    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch {url}: {e}")
