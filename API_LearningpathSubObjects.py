#!/usr/bin/env python3
import json
import requests
from datetime import datetime
import re
import os


url1 = 'https://api.litmos.com/v1.svc/'
url2 = '?source=MY-APP&format=json&limit=1000&start=0'
api_key = 'api_key'
dir = '/home/attunity/airflow/LitmosAPI/files/'


def jprint(obj):
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)


with open(dir + 'learning_path_course.json', mode='w', encoding='utf-8') as f:    json.dump([], f)
with open(dir + 'learning_path_user.json', mode='w', encoding='utf-8') as f:  json.dump([], f)


with open(dir + 'learning_path.json', 'r',  encoding='utf-8') as json_file:
    data = json.loads(json_file.read())
    for index, records in enumerate(data):
        id_value = records.get('Id', None)


        ##LEARNING_PATH COURSE
        response = requests.get(url1+'learningpaths/'+id_value+'/courses'+url2, headers={'apikey': api_key})
        json_decoded = json.loads(json.dumps(response.json()))#list
        for d in json_decoded:
            for element in d.items():
                print(response, index, id_value, datetime.now(), 'learning_path_course')
            d['LearningPathId'] = id_value
            d['LoadDateTime'] = datetime.now().isoformat()
            #jprint(json_decoded)
        with open(dir + 'learning_path_course.json', mode='a', encoding='utf-8') as fa:
            json.dump(json_decoded, fa, indent=4);


        ##LEARNING_PATH USER
        response = requests.get(url1+'learningpaths/'+id_value+'/users'+url2, headers={'apikey': api_key})
        json_decoded = json.loads(json.dumps(response.json()))#list
        for d in json_decoded:
            for element in d.items():
                print(response, index, id_value, datetime.now(), 'learning_path_user')
            d['LearningPathId'] = id_value
            d['LoadDateTime'] = datetime.now().isoformat()
            #jprint(json_decoded)
        with open(dir + 'learning_path_user.json', mode='a', encoding='utf-8') as fa:
            json.dump(json_decoded, fa, indent=4);


    #FORMATTING
    with open(dir + 'learning_path_course.json', 'r') as json_file:
        formatted = re.sub(r'(?!^|.$)\[*\]*', '', json_file.read()[2:]).replace('}' + '\n', '},').replace(',]', '\n]')[:-1]+'\n]'
    with open(dir + 'learning_path_course.json', mode='w', encoding='utf-8') as fa:
        fa.write(formatted)

    with open(dir + 'learning_path_user.json', 'r') as json_file:
        formatted = re.sub(r'(?!^|.$)\[*\]*', '', json_file.read()[2:]).replace('}' + '\n', '},').replace(',]', '\n]')[:-1]+'\n]'
    with open(dir + 'learning_path_user.json', mode='w', encoding='utf-8') as fa:
        fa.write(formatted)