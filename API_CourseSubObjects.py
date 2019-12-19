#!/usr/bin/env python3
import json
import requests
from datetime import datetime
import re
import os
from subprocess import Popen


url1 = 'https://api.litmos.com/v1.svc/'
url2 = '?source=MY-APP&format=json&limit=1000&start=0'
api_key = 'api_key'
dir = '/home/attunity/airflow/LitmosAPI/files/'


def jprint(obj):
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)


with open(dir + 'course_module.json', mode='w', encoding='utf-8') as f:  json.dump([], f)
with open(dir + 'course_module_ilt.json', mode='w', encoding='utf-8') as f: json.dump([], f)
with open(dir + 'course_user.json', mode='w', encoding='utf-8') as f:  json.dump([], f)
with open(dir + 'course_category.json', mode='w', encoding='utf-8') as f: json.dump([], f)
with open(dir + 'course_custom_field.json', mode='w', encoding='utf-8') as f:  json.dump([], f)


with open(dir + 'course.json', 'r', encoding='utf-8') as json_file:
    data = json.loads(json_file.read())
    for index, records in enumerate(data):
        id_value = records.get('Id', None)


        ##COURSE_MODULE
        response = requests.get(url1+'courses/'+id_value+'/modules'+url2, headers={'apikey': api_key})
        json_decoded = json.loads(json.dumps(response.json()))#list
        for d in json_decoded:
            for element in d.items():
                print(response, index, id_value, datetime.now(), 'course_module')
            d['CourseId'] = id_value
            d['LoadDateTime'] = datetime.now().isoformat()
        with open(dir + 'course_module.json', mode='a', encoding='utf-8') as fa:
            json.dump(json_decoded, fa, indent=4);


        ##COURSE_MODULE_ILT
        response = requests.get(url1+'courses/'+id_value+'/modules/ilt'+url2, headers={'apikey': api_key})
        json_decoded = json.loads(json.dumps(response.json()))#list
        for d in json_decoded:
            for element in d.items():
                print(response, index, id_value, datetime.now(), 'course_module_ilt')
            d['CourseId'] = id_value
            d['LoadDateTime'] = datetime.now().isoformat()
        with open(dir + 'course_module_ilt.json', mode='a', encoding='utf-8') as fa:
            json.dump(json_decoded, fa, indent=4);


        ##COURSE_USER
        response = requests.get(url1+'courses/'+id_value+'/users'+url2, headers={'apikey': api_key})
        json_decoded = json.loads(json.dumps(response.json()))
        print(json_decoded)
        for d in json_decoded:
            for element in d.items():
                print(response, index, id_value, datetime.now(), 'course_user')
            d['CourseId'] = id_value
            d['LoadDateTime'] = datetime.now().isoformat()
        with open(dir + 'course_user.json', mode='a', encoding='utf-8') as fa:
            json.dump(json_decoded, fa, indent=4);


        ##COURSE_CATEGORY
        response = requests.get(url1+'courses/'+id_value+url2, headers={'apikey': api_key})
        json_coded = json.dumps(response.json())  # string
        json_coded = '[' + json_coded + ']'
        json_decoded = json.loads(json_coded)
        for d in json_decoded:
            for element in d.items():
                print(response, index, id_value, datetime.now(), 'course_category')
            d['LoadDateTime'] = datetime.now().isoformat()
        with open(dir + 'course_category.json', mode='a', encoding='utf-8') as fa:
            json.dump(json_decoded, fa, indent=4);


        ##COURSE_CUSTOM FIELD
        response = requests.get(url1+'courses/'+id_value+'/coursecustomfields'+url2, headers={'apikey': api_key})
        json_decoded = json.loads(json.dumps(response.json()))#list
        print(response, index, id_value, datetime.now(), 'course_custom_field')
        for d in json_decoded:
            for element in d.items():
                print(response, index, id_value, datetime.now(), 'course_custom_field')
            d['CourseId'] = id_value
            d['LoadDateTime'] = datetime.now().isoformat()
        with open(dir + 'course_custom_field.json', mode='a', encoding='utf-8') as fa:
            json.dump(json_decoded, fa, indent=4);

    ##FORMATTING
    with open(dir + 'course_module.json', 'r') as json_file:
        formatted = re.sub(r'(?!^|.$)\[*\]*', '', json_file.read()[2:]).replace('}' + '\n', '},').replace(',]', '\n]')[:-1]+'\n]'
    with open(dir + 'course_module.json', mode='w', encoding='utf-8') as fa:
        fa.write(formatted)

    with open(dir + 'course_module_ilt.json', 'r') as json_file:
        formatted = re.sub(r'(?!^|.$)\[*\]*', '', json_file.read()[2:]).replace('}' + '\n', '},').replace(',]', '\n]')[:-1]+'\n]'
    with open(dir + 'course_module_ilt.json', mode='w', encoding='utf-8') as fa:
        fa.write(formatted)

    with open(dir + 'course_user.json', 'r') as json_file:
        formatted = re.sub(r'(?!^|.$)\[*\]*', '', json_file.read()[2:]).replace('}' + '\n', '},').replace(',]', '\n]')[:-1]+'\n]'
    with open(dir + 'course_user.json', mode='w', encoding='utf-8') as fa:
        fa.write(formatted)

    with open(dir + 'course_category.json', 'r') as json_file:
        formatted = re.sub(r'(?!^|.$)\[*\]*', '', json_file.read()[2:]).replace('}' + '\n', '},').replace(',]', '\n]')[:-1]+'\n]'
    with open(dir + 'course_category.json', mode='w', encoding='utf-8') as fa:
        fa.write(formatted)

    with open(dir + 'course_custom_field.json', 'r') as json_file:
        formatted = re.sub(r'(?!^|.$)\[*\]*', '', json_file.read()[2:]).replace('}' + '\n', '},').replace(',]', '\n]')[:-1]+'\n]'
    with open(dir + 'course_custom_field.json', mode='w', encoding='utf-8') as fa:
        fa.write(formatted)


    # #CALL CourseModuleSubObjects
    Popen(['python3', 'API_CourseModuleSubObjects.py'])