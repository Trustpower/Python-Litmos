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


with open(dir + 'team_administrator.json', mode='w', encoding='utf-8') as f:  json.dump([], f)
with open(dir + 'team_course.json', mode='w', encoding='utf-8') as f: json.dump([], f)
with open(dir + 'team_gamification_detail.json', mode='w', encoding='utf-8') as f:  json.dump([], f)
with open(dir + 'team_leader.json', mode='w', encoding='utf-8') as f: json.dump([], f)
with open(dir + 'team_learning_path.json', mode='w', encoding='utf-8') as f:  json.dump([], f)
with open(dir + 'team_user.json', mode='w', encoding='utf-8') as f: json.dump([], f)


with open(dir + 'team.json', 'r',  encoding='utf-8') as json_file:
    data = json.loads(json_file.read())
    for index, records in enumerate(data):
        id_value = records.get('Id', None)


        ##TEAM_ADMINISTRATOR
        response = requests.get(url1+'teams/'+id_value+'/admins'+url2, headers={'apikey': api_key})
        json_decoded = json.loads(json.dumps(response.json()))#list
        for d in json_decoded:
            for element in d.items():
                print(response, index, id_value, datetime.now(), 'team_administrator')
            d['TeamId'] = id_value
            d['LoadDateTime'] = datetime.now().isoformat()
            #jprint(json_decoded)
        with open(dir + 'team_administrator.json', mode='a', encoding='utf-8') as fa:
            json.dump(json_decoded, fa, indent=4);


        ##TEAM_COURSE
        response = requests.get(url1+'teams/'+id_value+'/courses'+url2, headers={'apikey': api_key})
        json_decoded = json.loads(json.dumps(response.json()))#list
        for d in json_decoded:
            for element in d.items():
                print(response, index, id_value, datetime.now(), 'team_course')
            d['TeamId'] = id_value
            d['LoadDateTime'] = datetime.now().isoformat()
            #jprint(json_decoded)
        with open(dir + 'team_course.json', mode='a', encoding='utf-8') as fa:
            json.dump(json_decoded, fa, indent=4);


        ##TEAM_GAMIFICATION_DETAIL
        response = requests.get(url1+'teams/'+id_value+'/gamificationdetails'+url2, headers={'apikey': api_key})
        json_decoded = json.loads(json.dumps(response.json()))#list
        for d in json_decoded:
            for element in d.items():
                print(response, index, id_value, datetime.now(), 'team_gamification_detail')
            d['TeamId'] = id_value
            d['LoadDateTime'] = datetime.now().isoformat()
            #jprint(json_decoded)
        with open(dir + 'team_gamification_detail.json', mode='a', encoding='utf-8') as fa:
            json.dump(json_decoded, fa, indent=4);


        ##TEAM_LEADER
        response = requests.get(url1+'teams/'+id_value+'/leaders'+url2, headers={'apikey': api_key})
        json_decoded = json.loads(json.dumps(response.json()))#list
        for d in json_decoded:
            for element in d.items():
                print(response, index, id_value, datetime.now(), 'team_leader')
            d['TeamId'] = id_value
            d['LoadDateTime'] = datetime.now().isoformat()
            #jprint(json_decoded)
        with open(dir + 'team_leader.json', mode='a', encoding='utf-8') as fa:
            json.dump(json_decoded, fa, indent=4);


        ##TEAM_LEARNING_PATH
        response = requests.get(url1+'teams/'+id_value+'/learningpaths'+url2, headers={'apikey': api_key})
        json_decoded = json.loads(json.dumps(response.json()))#list
        for d in json_decoded:
            for element in d.items():
                print(response, index, id_value, datetime.now(), 'team_learning_path')
            d['TeamId'] = id_value
            d['LoadDateTime'] = datetime.now().isoformat()
            #jprint(json_decoded)
        with open(dir + 'team_learning_path.json', mode='a', encoding='utf-8') as fa:
            json.dump(json_decoded, fa, indent=4);


        ##TEAM_USER
        response = requests.get(url1+'teams/'+id_value+'/users'+url2, headers={'apikey': api_key})
        json_decoded = json.loads(json.dumps(response.json()))#list
        for d in json_decoded:
            for element in d.items():
                print(response, index, id_value, datetime.now(), 'team_user')
            d['TeamId'] = id_value
            d['LoadDateTime'] = datetime.now().isoformat()
            #jprint(json_decoded)
        with open(dir + 'team_user.json', mode='a', encoding='utf-8') as fa:
            json.dump(json_decoded, fa, indent=4);


    ##FORMATTING
    with open(dir + 'team_administrator.json', 'r') as json_file:
        formatted = re.sub(r'(?!^|.$)\[*\]*', '', json_file.read()[2:]).replace('}' + '\n', '},').replace(',]', '\n]')[:-1]+'\n]'
    with open(dir + 'team_administrator.json', mode='w', encoding='utf-8') as fa:
        fa.write(formatted)

    with open(dir + 'team_course.json', 'r') as json_file:
        formatted = re.sub(r'(?!^|.$)\[*\]*', '', json_file.read()[2:]).replace('}' + '\n', '},').replace(',]', '\n]')[:-1]+'\n]'
    with open(dir + 'team_course.json', mode='w', encoding='utf-8') as fa:
        fa.write(formatted)

    with open(dir + 'team_gamification_detail.json', 'r') as json_file:
        formatted = re.sub(r'(^[\r\n]*|[\r\n]+)[\s\t]*[\r\n]+', '\n', (json_file.read()[2:]).replace('][', ''))
    with open(dir + 'team_gamification_detail.json', mode='w', encoding='utf-8') as fa:
        fa.write(formatted)

    with open(dir + 'team_leader.json', 'r') as json_file:
        formatted = re.sub(r'(?!^|.$)\[*\]*', '', json_file.read()[2:]).replace('}' + '\n', '},').replace(',]', '\n]')[:-1]+'\n]'
    with open(dir + 'team_leader.json', mode='w', encoding='utf-8') as fa:
        fa.write(formatted)

    with open(dir + 'team_learning_path.json', 'r') as json_file:
        formatted = re.sub(r'(?!^|.$)\[*\]*', '', json_file.read()[2:]).replace('}' + '\n', '},').replace(',]', '\n]')[:-1]+'\n]'
    with open(dir + 'team_learning_path.json', mode='w', encoding='utf-8') as fa:
        fa.write(formatted)

    with open(dir + 'team_user.json', 'r') as json_file:
        formatted = re.sub(r'(?!^|.$)\[*\]*', '', json_file.read()[2:]).replace('}' + '\n', '},').replace(',]', '\n]')[:-1]+'\n]'
    with open(dir + 'team_user.json', mode='w', encoding='utf-8') as fa:
        fa.write(formatted)