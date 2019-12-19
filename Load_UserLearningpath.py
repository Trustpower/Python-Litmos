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
object = 'user_learning_path'
object_db = 'LITMOS.USER_LEARNING_PATH'

con.cursor().execute('truncate table ' + object_db)

for i in range (1, 6):
    cursor = con.cursor()
    con.cursor().execute('create or replace file format myjsonformat_' + object + str(i) + ' type="JSON" strip_outer_array=true;')
    con.cursor().execute('create or replace stage my_json_stage_' + object + str(i) + ' file_format=myjsonformat_' + object + str(i) + ';')

    print(json_dir + object + str(i))
    con.cursor().execute('put ' + json_dir + object + str(i) + '* @my_json_stage_' + object + str(i) + ' auto_compress=true;')

    table_list = cursor.execute('COPY INTO LITMOS.USER_LEARNING_PATH '
                                '(USER_ID'
                                ',LEARNING_PATH_ID'
                                ',NAME'
                                ',ACTIVE'
                                ',ORIGINAL_ID'
                                ',COMPLETE'
                                ',PERCENTAGE_COMPLETE'
                                ',ASSIGNED_DATE_RAW'
                                ',ASSIGNED_DATE'
                                ',START_DATE_RAW'
                                ',START_DATE'
                                ',DATE_COMPLETED_RAW'
                                ',DATE_COMPLETED'
                                ',RESULT_ID'
                                ',ACCESS_TILL_DATE_RAW'
                                ',ACCESS_TILL_DATE'
                                ',LOAD_DATETIME)'
                                'FROM (SELECT '
                                    'PARSE_JSON($1):"UserId"'
                                    ',PARSE_JSON($1):"Id"'
                                    ',PARSE_JSON($1):"Name"'
                                    ',PARSE_JSON($1):"Active"'
                                    ',PARSE_JSON($1):"OriginalId"'
                                    ',PARSE_JSON($1):"Complete"'
                                    ',PARSE_JSON($1):"PercentageComplete"'
                                    ',PARSE_JSON($1):"AssignedDate"'
                                    ',TO_TIMESTAMP_NTZ(SUBSTRING(PARSE_JSON($1):"AssignedDate",  7, 13))'
                                    ',PARSE_JSON($1):"StartDate"'
                                    ',TO_TIMESTAMP_NTZ(SUBSTRING(PARSE_JSON($1):"StartDate",  7, 13))'
                                    ',PARSE_JSON($1):"DateCompleted"'
                                    ',TO_TIMESTAMP_NTZ(SUBSTRING(PARSE_JSON($1):"DateCompleted",  7, 13))'
                                    ',PARSE_JSON($1):"ResultId"'
                                    ',PARSE_JSON($1):"AccessTillDate"'
                                    ',TO_TIMESTAMP_NTZ(SUBSTRING(PARSE_JSON($1):"AccessTillDate",  7, 13))'
                                    ',CONVERT_TIMEZONE(\'Pacific/Auckland\', \'America/Los_Angeles\', TO_TIMESTAMP(PARSE_JSON($1):"LoadDateTime"))'
                                'FROM @my_json_stage_' + object + str(i) + '/' + object + str(i) + '.json.gz);').fetchall()

    print(table_list)

    con.cursor().execute('REMOVE @my_json_stage_' + object + str(i) + ' ;')