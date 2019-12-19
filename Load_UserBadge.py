#!/usr/bin/env python3
import snowflake.connector
import json

con = snowflake.connector.connect(
    user='user',
    password='password',
    account='trustpower.australia-east.azure',
    role='SYSADMIN',
    warehouse='DM_ELT',
    database='SRC_LOAD',
    schema='LITMOS',
)

json_dir = 'file:///home/attunity/airflow/LitmosAPI/files/'
object = 'user_badge'
object_db = 'LITMOS.USER_BADGE'

con.cursor().execute('truncate table ' + object_db)

for i in range(1, 6):
    cursor = con.cursor()
    con.cursor().execute('create or replace file format myjsonformat_' + object + str(i) + ' type="JSON" strip_outer_array=true;')
    con.cursor().execute('create or replace stage my_json_stage_' + object + str(i) + ' file_format=myjsonformat_' + object + str(i) + ';')

    print(json_dir + object + str(i))
    con.cursor().execute('put ' + json_dir + object + str(i) + '* @my_json_stage_' + object + str(i) + ' auto_compress=true;')

    table_list = cursor.execute('COPY INTO LITMOS.USER_BADGE '
                                '(USER_BADGE_ID'
                                ',USER_ID'
                                ',TITLE'
                                ',DESCRIPTION'
                                ',ICON'
                                ',ICON_BG_COLOR'
                                ',LOAD_DATETIME)'
                                'FROM (SELECT '
                                    'PARSE_JSON($1):"ID"'
                                    ',PARSE_JSON($1):"UserId"'
                                    ',PARSE_JSON($1):"Title"'
                                    ',PARSE_JSON($1):"Description"'
                                    ',PARSE_JSON($1):"Icon"'
                                    ',PARSE_JSON($1):"IconBgColor"'
                                    ',CONVERT_TIMEZONE(\'Pacific/Auckland\', \'America/Los_Angeles\', TO_TIMESTAMP(PARSE_JSON($1):"LoadDateTime"))'
                                'FROM @my_json_stage_' + object + str(i) + '/' + object + str(i) + '.json.gz);').fetchall()

    print(table_list)

    con.cursor().execute('REMOVE @my_json_stage_' + object + str(i) + ' ;')