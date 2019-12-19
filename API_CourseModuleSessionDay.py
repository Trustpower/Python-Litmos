#!/usr/bin/env python3
import json
import re
import os

dir = '/home/attunity/airflow/LitmosAPI/files/'


with open(dir + 'course_module_session_day.json', mode='w', encoding='utf-8') as f:  json.dump([], f)


def jprint(obj):
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)


with open(dir + 'course_module_session.json', 'r') as json_file:
    data = json.loads(json_file.read())


json_selected = {}


for d in data:
    CourseId = d['CourseId']
    ModuleId = d['ModuleId']
    SessionId = d['Id']
    LoadDateTime = d['LoadDateTime']
    json_selected = d['Days']
    for d in json_selected:
        d['CourseId'] = CourseId
        d['ModuleId'] = ModuleId
        d['SessionId'] = SessionId
        d['LoadDateTime'] = LoadDateTime
    with open(dir + 'course_module_session_day.json', mode='a', encoding='utf-8') as fa:
        json.dump(json_selected, fa, indent=4);


# # FORMATTING
with open(dir + 'course_module_session_day.json', 'r') as json_file:
    formatted = re.sub(r'(^[\r\n]*|[\r\n]+)[\s\t]*[\r\n]+', '\n', (json_file.read()).replace('][', '')).replace('}' + '\n    {', '},' + '\n    {')
with open(dir + 'course_module_session_day.json', mode='w', encoding='utf-8') as fa:
    fa.write(formatted)