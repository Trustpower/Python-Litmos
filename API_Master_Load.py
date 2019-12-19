#!/usr/bin/env python3
import html
import json
import time
import requests
from datetime import datetime
import os
from subprocess import Popen


root_url = 'https://api.litmos.com/v1.svc/'
pag_offset = 1000
api_key = 'api_key'
app_name = 'matillion'
since = '2000-01-01'
dir = '/home/attunity/airflow/LitmosAPI/files/'


def base_url(resource, **kwargs):
    return root_url + \
        resource + \
        ("/" + kwargs['resource_id'] if kwargs.get('resource_id', None) else "") + \
        ("/" + kwargs['sub_resource'] if kwargs.get('sub_resource', None) else "") + \
        ("/" + kwargs['sub_resource_id'] if kwargs.get('sub_resource_id', None) else "") + \
        '?source=' + app_name + \
        '&format=json' + \
        ("&search=" + str(kwargs['search_param']) if kwargs.get('search_param', None) else "") + \
        ("&limit=" + str(kwargs['limit']) if kwargs.get('limit', None) else "") + \
        ("&start=" + str(kwargs['start']) if kwargs.get('start', None) else "") + \
        ("&since=" + str(kwargs['since']) if kwargs.get('since', None) else "");


def perform_request(method, url, **kwargs):
    kwargs['headers'] = {'apikey': api_key}
    response = requests.request(method, url, **kwargs)

    if response.status_code == 503:  # request rate limit exceeded
        time.sleep(60)
        response = requests.request(method, url, **kwargs)

    response.raise_for_status()

    return response;


def parse_response(response):
    return json.loads(html.unescape(response.text));


def get_all(resource, results, start_pos, since):
    response = perform_request(
        'GET',
        base_url(resource, limit=pag_offset, start=start_pos, since=since)
    )

    response_list = parse_response(response)
    results += response_list

    print(datetime.now(), response, base_url(resource, limit=pag_offset, start=start_pos, since=since))

    if not response_list:
        return results
    else:
        return get_all(resource, results, start_pos + pag_offset, since);

def litmosall(resource):
    return get_all(resource, [], 0, since);

def litmosallsince(resource):
    return get_all(resource, [], 0);


def jprint(obj):
    # create a formatted string of the Python JSON object
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text);


#ACHIEVEMENT
achievement = litmosall('achievements')
with open(dir+'achievement.json', 'w', encoding='utf-8') as f:
    json.dump(achievement, f, ensure_ascii=False, indent=4);

#COURSE
course = litmosall('courses')
with open(dir+'course.json', 'w', encoding='utf-8') as f:
    json.dump(course, f, ensure_ascii=False, indent=4);

#INSTRUCTOR
instructor = litmosall('instructors')
with open(dir+'instructor.json', 'w', encoding='utf-8') as f:
    json.dump(instructor, f, ensure_ascii=False, indent=4);

#LEARNING PATH
learningpath = litmosall('learningpaths')
with open(dir+'learning_path.json', 'w', encoding='utf-8') as f:
    json.dump(learningpath, f, ensure_ascii=False, indent=4);

#RESULT
result = litmosall('results/details')
with open(dir+'result.json', 'w', encoding='utf-8') as f:
    json.dump(result, f, ensure_ascii=False, indent=4);

#TEAM
team = litmosall('teams')
with open(dir+'team.json', 'w', encoding='utf-8') as f:
    json.dump(team, f, ensure_ascii=False, indent=4);

#USER
user = litmosall('users/details')
with open(dir+'user.json', 'w', encoding='utf-8') as f:
    json.dump(user, f, ensure_ascii=False, indent=4);


#CALL LOADS
Popen(['python3', 'Load_Achievement.py'])
Popen(['python3', 'Load_Instructor.py'])
Popen(['python3', 'Load_Learningpath.py'])
Popen(['python3', 'Load_Result.py'])
Popen(['python3', 'Load_Team.py'])
Popen(['python3', 'Load_User.py'])

#CALL ORCHESTRATION
Popen(['python3', 'API_Orchestration.py'])