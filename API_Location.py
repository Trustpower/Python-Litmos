#!/usr/bin/env python3
import json
import requests
from datetime import datetime
import os


url1 = 'https://api.litmos.com/v1.svc/'
url2 = '?source=MY-APP&format=json&limit=1000&start=0'
api_key = 'api_key'
dir = '/home/attunity/airflow/LitmosAPI/files/'


##LOCATION
response = requests.get(url1 + 'locations' + url2, headers={'apikey': api_key})
print(response,  datetime.now(), 'location')

json_decoded = json.loads(json.dumps(response.json()))  # list

with open(dir + 'location.json', mode='w', encoding='utf-8') as fa:
    json.dump(json_decoded, fa, indent=4);