import os
from dotenv import load_dotenv
import requests
import json

load_dotenv('project.env')

file_location = os.getenv('download_folder')
file_type = 'json'
url_base = 'https://opendata.ecdc.europa.eu/covid19/'
endpoint_list = [
    'COVID-19_VC_data_from_September_2023/json/data_v7.json', 
    'vaccine_tracker/json/',
    'movementindicators/json/',
    'movementindicatorsarchive/json/data.json',
    'nationalcasedeath/json/',
    'nationalcasedeath_archive/json/',
    'nationalcasedeath_eueea_daily_ei/json/'
]

for endpoint in endpoint_list:
    url = url_base+endpoint
    filename = endpoint.split('/')[0].replace('_','-').lower() + '.json'
   
    r = requests.get(url)
    
    if r.ok == True:
        with open(file_location+filename, 'w') as f:
            f.write(r.text)
            print(f'{filename} saved')
