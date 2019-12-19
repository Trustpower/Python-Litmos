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


with open(dir + 'user_badge1.json', mode='w', encoding='utf-8') as f: json.dump([], f)
with open(dir + 'user_course1.json', mode='w', encoding='utf-8') as f: json.dump([], f)
with open(dir + 'user_gamification_detail1.json', mode='w', encoding='utf-8') as f: json.dump([], f)
with open(dir + 'user_gamification_summary1.json', mode='w', encoding='utf-8') as f: json.dump([], f)
with open(dir + 'user_learning_path1.json', mode='w', encoding='utf-8') as f: json.dump([], f)


def jprint(obj):
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)


with open(dir + 'user.json', 'r', encoding='utf8') as json_file:
    data = json.loads(json_file.read())[0:500]
    for index, records in enumerate(data):
        id_value = records.get('Id', None)


        ##USER_BADGE
        response = requests.get(url1 + 'users/' + id_value + '/badges' + url2, headers={'apikey': api_key})
        json_decoded = json.loads(json.dumps(response.json()))  # list
        for d in json_decoded:
            for element in d.items():
                print(response, index, id_value, datetime.now(), 'user_badge1')
            d['UserId'] = id_value
            d['LoadDateTime'] = datetime.now().isoformat()
        with open(dir + 'user_badge1.json', mode='a', encoding='utf-8') as fa:
            json.dump(json_decoded, fa, indent=4);


        ##USER_COURSE
        response = requests.get(url1 + 'users/' + id_value + '/courses' + url2, headers={'apikey': api_key})
        json_decoded = json.loads(json.dumps(response.json()))  # list
        for d in json_decoded:
            for element in d.items():
                print(response, index, id_value, datetime.now(), 'user_course1')
            d['UserId'] = id_value
            d['LoadDateTime'] = datetime.now().isoformat()
        with open(dir + 'user_course1.json', mode='a', encoding='utf-8') as fa:
            json.dump(json_decoded, fa, indent=4);


        ##USER_GAMIFICATION_DETAIL
        response = requests.get(url1 + 'users/' + id_value + '/gamificationdetails' + url2, headers={'apikey': api_key})
        json_decoded = json.loads(json.dumps(response.json()))  # list
        for d in json_decoded:
            for element in d.items():
                print(response, index, id_value, datetime.now(), 'user_gamification_detail1')
            d['UserId'] = id_value
            d['LoadDateTime'] = datetime.now().isoformat()
        with open(dir + 'user_gamification_detail1.json', mode='a', encoding='utf-8') as fa:
            json.dump(json_decoded, fa, indent=4);


        ##USER_GAMIFICATION_SUMMARY
        response = requests.get(url1 + 'users/' + id_value + '/gamificationsummary' + url2, headers={'apikey': api_key})
        json_coded = json.dumps(response.json())# list
        json_coded = '[' + json_coded +']'
        json_decoded = json.loads(json_coded)
        for d in json_decoded:
            for element in d.items():
                print(response, index, id_value, datetime.now(), 'user_gamification_summary1')
            d['UserId'] = id_value
            d['LoadDateTime'] = datetime.now().isoformat()
        with open(dir + 'user_gamification_summary1.json', mode='a', encoding='utf-8') as fa:
            json.dump(json_decoded, fa, indent=4);


        ##USER_LEARNING_PATH
        response = requests.get(url1 + 'users/' + id_value + '/learningpaths' + url2, headers={'apikey': api_key})
        json_decoded = json.loads(json.dumps(response.json()))  # list
        for d in json_decoded:
            for element in d.items():
                print(response, index, id_value, datetime.now(), 'user_learning_path1')
            d['UserId'] = id_value
            d['LoadDateTime'] = datetime.now().isoformat()
        with open(dir + 'user_learning_path1.json', mode='a', encoding='utf-8') as fa:
            json.dump(json_decoded, fa, indent=4);


    #FORMATTING
    with open(dir + 'user_badge1.json', 'r') as json_file:
        formatted = re.sub(r'(?!^|.$)\[*\]*', '', json_file.read()[2:]).replace('}' + '\n', '},').replace(',]', '\n]')[:-1]+'\n]'
    with open(dir + 'user_badge1.json', mode='w', encoding='utf-8') as fa:
        fa.write(formatted)

    with open(dir + 'user_course1.json', 'r') as json_file:
        formatted = re.sub(r'(?!^|.$)\[*\]*', '', json_file.read()[2:]).replace('}' + '\n', '},').replace(',]', '\n]')[:-1]+'\n]'
    with open(dir + 'user_course1.json', mode='w', encoding='utf-8') as fa:
        fa.write(formatted)

    with open(dir + 'user_gamification_detail1.json', 'r') as json_file:
        formatted = re.sub(r'(^[\r\n]*|[\r\n]+)[\s\t]*[\r\n]+', '\n', (json_file.read()[2:]).replace('][', ''))
    with open(dir + 'user_gamification_detail1.json', mode='w', encoding='utf-8') as fa:
        fa.write(formatted)

    with open(dir + 'user_gamification_summary1.json', 'r') as json_file:
        formatted = re.sub(r'(?!^|.$)\[*\]*', '', json_file.read()[2:]).replace('}' + '\n', '},').replace(',]', '\n]')[:-1]+'\n]'
    with open(dir + 'user_gamification_summary1.json', mode='w', encoding='utf-8') as fa:
        fa.write(formatted)

    with open(dir + 'user_learning_path1.json', 'r') as json_file:
        formatted = re.sub(r'(?!^|.$)\[*\]*', '', json_file.read()[2:]).replace('}' + '\n', '},').replace(',]', '\n]')[:-1]+'\n]'
    with open(dir + 'user_learning_path1.json', mode='w', encoding='utf-8') as fa:
        fa.write(formatted);