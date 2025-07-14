import requests
import json


# gets eu covid data in json format and stores in local directory ready to be processed

url_base = 'https://opendata.ecdc.europa.eu/covid19/'
endpoint_list = [
    'COVID-19_VC_data_from_September_2023/json/data_v7.json', 
    'vaccine_tracker/json/','movementindicators/json/',
    'movementindicatorsarchive/json/data.json',
    'nationalcasedeath/json/',
    'nationalcasedeath_archive/json/',
    'nationalcasedeath_eueea_daily_ei/json/'
]

# loop through enpoints and download data seeting filename based on endpointy used
for endpoint in endpoint_list:
    url = url_base+endpoint
    filename = endpoint.split('/')[0].replace('_','-').lower() + '.json'
    
    print(url)
    print(filename)
    
    r = requests.get(url)
    
    if r.ok == True:
        with open(filename, 'w') as f:
            f.write(r.text)
