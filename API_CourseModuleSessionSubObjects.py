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


with open(dir + 'course_module_session_attendance.json', mode='w', encoding='utf-8') as f:  json.dump([], f)
with open(dir + 'course_module_session_roll_call.json', mode='w', encoding='utf-8') as f: json.dump([], f)


with open(dir + 'course_module_session_detail.json', 'r') as json_file:
    data = json.loads(json_file.read())
    for index, records in enumerate(data):
        id_value1 = records.get('CourseId', None)
        id_value2 = records.get('ModuleId', None)
        id_value3 = records.get('Id', None)
        print(id_value1, id_value2, id_value3)


        ##COURSE_MODULE_SESSION_ATTENDANCE
        response = requests.get(url1 + 'courses/' + id_value1 + '/modules/' + id_value2 + '/sessions/' + id_value3 + '/attendance' + url2, headers={'apikey': api_key})
        json_decoded = json.loads(json.dumps(response.json()))
        for d in json_decoded:
            for element in d.items():
                print(response, index, id_value1, id_value2, id_value3, datetime.now(), 'course_module_session_attendance')
            d['CourseId'] = id_value1
            d['ModuleId'] = id_value2
            d['SessionId'] = id_value3
            d['LoadDateTime'] = datetime.now().isoformat()
            #jprint(json_decoded)
        with open(dir + 'course_module_session_attendance.json', mode='a', encoding='utf-8') as fa:
            json.dump(json_decoded, fa, indent=4);


        ##COURSE_MODULE_SESSION_ROLLCALL
        response = requests.get(url1 + 'courses/' + id_value1 + '/modules/' + id_value2 + '/sessions/' + id_value3 + '/rollcall' + url2, headers={'apikey': api_key})
        json_decoded = json.loads(json.dumps(response.json()))#list
        for d in json_decoded:
            for element in d.items():
                print(response, index, id_value1, id_value2, id_value3, datetime.now(), 'course_module_session_roll_call')
            d['CourseId'] = id_value1
            d['ModuleId'] = id_value2
            d['SessionId'] = id_value3
            d['LoadDateTime'] = datetime.now().isoformat()
            #jprint(json_decoded)
        with open(dir + 'course_module_session_roll_call.json', mode='a', encoding='utf-8') as fa:
            json.dump(json_decoded, fa, indent=4);


    #FORMATTING
    with open(dir + 'course_module_session_attendance.json', 'r') as json_file:
        formatted = re.sub(r'(?!^|.$)\[*\]*', '', json_file.read()[2:]).replace('}' + '\n', '},').replace(',]', '\n]')[:-1]+'\n]'
    with open(dir + 'course_module_session_attendance.json', mode='w', encoding='utf-8') as fa:
        fa.write(formatted)

    with open(dir + 'course_module_session_roll_call.json', 'r') as json_file:
        formatted = re.sub(r'(?!^|.$)\[*\]*', '', json_file.read()[2:]).replace('}' + '\n', '},').replace(',]', '\n]')[:-1]+'\n]'
    with open(dir + 'course_module_session_roll_call.json', mode='w', encoding='utf-8') as fa:
        fa.write(formatted)