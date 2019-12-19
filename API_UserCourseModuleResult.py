#!/usr/bin/env python3
import json
import requests
from datetime import datetime
import re


url1 = 'https://api.litmos.com/v1.svc/'
url2 = '?source=MY-APP&format=json&limit=1000&start=0'
api_key = 'api_key'
dir = '/home/attunity/airflow/LitmosAPI/files/'


def jprint(obj):
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)


with open(dir + 'user_course_module_result1.json', mode='w', encoding='utf-8') as f: json.dump([], f)


with open(dir + 'user_course1.json', 'r',  encoding='utf-8') as json_file:
    data = json.loads(json_file.read())
    for index, records in enumerate(data):
        id_value1 = records.get('UserId', None)
        id_value2 = records.get('Id', None)
        print(id_value1, id_value2)


        ##USER_COURSE_MODULE_RESULT
        response = requests.get(url1 + 'users/' + id_value1 + '/courses/' + id_value2 + url2, headers={'apikey': api_key})
        json_coded = json.dumps(response.json())  # string
        json_coded = '[' + json_coded + ']'
        json_decoded = json.loads(json_coded)
        for d in json_decoded:
            for element in d.items():
                print(response, index, id_value1, id_value2, datetime.now(), 'user_course_module_result1')
            d['UserId'] = id_value1
            d['CourseId'] = id_value2
            d['LoadDateTime'] = datetime.now().isoformat()
            #jprint(json_decoded)
        with open(dir + 'user_course_module_result1.json', mode='a', encoding='utf-8') as fa:
            json.dump(json_decoded, fa, indent=4);


    # # FORMATTING
    with open(dir + 'user_course_module_result1.json', 'r') as json_file:
        formatted = re.sub(r'(^[\r\n]*|[\r\n]+)[\s\t]*[\r\n]+', '\n', (json_file.read()[2:]).replace('][', '')).replace('    }\n    {', '    },\n    {')
    with open(dir + 'user_course_module_result1.json', mode='w', encoding='utf-8') as fa:
        fa.write(formatted)