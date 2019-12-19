#!/usr/bin/env python3
import json
import re
import os
from subprocess import Popen


dir = '/home/attunity/airflow/LitmosAPI/files/'


for i in range(1, 6):
    print('user_course_module_result' + str(i))
    with open(dir + 'user_course_module_result_detail' + str(i) + '.json', mode='w', encoding='utf-8') as f: json.dump([], f)


    def jprint(obj):
        text = json.dumps(obj, sort_keys=True, indent=4)
        print(text)


    with open(dir + 'user_course_module_result' + str(i) + '.json', 'r') as json_file:
        data = json.loads(json_file.read())


    json_selected = {}


    for d in data:
        UserId = d['UserId']
        CourseId = d['CourseId']
        LoadDateTime = d['LoadDateTime']
        json_selected = d['Modules']
        for d in json_selected:
            d['UserId'] = UserId
            d['CourseId'] = CourseId
            d['LoadDateTime'] = LoadDateTime
        with open(dir + 'user_course_module_result_detail' + str(i) + '.json', mode='a', encoding='utf-8') as fa:
            json.dump(json_selected, fa, indent=4);


    # FORMATTING
    with open(dir + 'user_course_module_result_detail' + str(i) + '.json', 'r') as json_file:
        formatted = re.sub(r'(^[\r\n]*|[\r\n]+)[\s\t]*[\r\n]+', '\n', (json_file.read()).replace('][', '')).replace('}' + '\n    {', '},' + '\n    {')
    with open(dir + 'user_course_module_result_detail' + str(i) + '.json', mode='w', encoding='utf-8') as fa:
        fa.write(formatted)


# #CALL LOADS
Popen(['python3', 'Load_UserCourseModuleResult.py'])