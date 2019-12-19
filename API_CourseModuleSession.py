#!/usr/bin/env python3
import json
import re
import os

dir = '/home/attunity/airflow/LitmosAPI/files/'


with open(dir + 'course_module_session_detail.json', mode='w', encoding='utf-8') as f:  json.dump([], f)


def jprint(obj):
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)


with open(dir + 'course_module_session.json', 'r') as json_file:
    data = json.loads(json_file.read())


json_selected = {}


for json_selected in data:
    json_selected = {k: json_selected[k] for k in ('CourseId', 'ModuleId', 'Id', 'Name',
                                                   'InstructorUserId', 'InstructorName',
                                                   'SessionType', 'TimeZone', 'Location', 'LocationId',
                                                   'StartDate', 'EndDate', 'CourseName', 'ModuleName',
                                                   'Slots', 'Accepted', 'EnableWaitList', 'LoadDateTime'
                                                   )}
    with open(dir + 'course_module_session_detail.json', mode='a', encoding='utf-8') as fa:
        json.dump(json_selected, fa, indent=4);


# FORMATTING
with open(dir + 'course_module_session_detail.json', 'r') as json_file:
    formatted = re.sub(r'(?!^|.$)\[*\]*', '', json_file.read()).replace('[{', '[' + '\n{').replace('}{', '},' + '\n{')+'\n]'
with open(dir + 'course_module_session_detail.json', mode='w', encoding='utf-8') as fa:
    fa.write(formatted)