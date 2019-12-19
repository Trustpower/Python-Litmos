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
object = 'user_course_module_result_detail'
object_db = 'LITMOS.USER_COURSE_MODULE_RESULT'

con.cursor().execute('truncate table ' + object_db)

for i in range(1, 6):
    cursor = con.cursor()
    con.cursor().execute('create or replace file format myjsonformat_' + object + str(i) + ' type="JSON" strip_outer_array=true;')
    con.cursor().execute('create or replace stage my_json_stage_' + object + str(i) + ' file_format=myjsonformat_' + object + str(i) + ';')

    print(json_dir + object + str(i))
    con.cursor().execute('put ' + json_dir + object + str(i) + '* @my_json_stage_' + object + str(i) + ' auto_compress=true;')

    table_list = cursor.execute('COPY INTO LITMOS.USER_COURSE_MODULE_RESULT '
                                '(USER_ID'
                                ',COURSE_ID'
                                ',COURSE_MODULE_ID'
                                ',MODULE_CODE'
                                ',MODULE_NAME'
                                ',MODULE_PASSMARK'
                                ',MODULE_SCORE'
                                ',MODULE_COMPLETED'
                                ',MODULE_LAST_UPDATED_RAW'
                                ',MODULE_LAST_UPDATED'
                                ',MODULE_ATTEMPT'
                                ',MODULE_ORIGINAL_ID'
                                ',MODULE_RESULT_ID'
                                ',LOAD_DATETIME)'
                                'FROM (SELECT '
                                    'PARSE_JSON($1):"UserId"'
                                    ',PARSE_JSON($1):"CourseId"'
                                    ',PARSE_JSON($1):"Id"'
                                    ',PARSE_JSON($1):"Code"'
                                    ',PARSE_JSON($1):"Name"'
                                    ',PARSE_JSON($1):"Passmark"'
                                    ',PARSE_JSON($1):"Score"'
                                    ',PARSE_JSON($1):"Completed"'
                                    ',PARSE_JSON($1):"LastUpdated"'
                                    ',TO_TIMESTAMP_NTZ(SUBSTRING(PARSE_JSON($1):"LastUpdated", 7, CHARINDEX(\'-\',PARSE_JSON($1):"LastUpdated")-7))'
                                    ',PARSE_JSON($1):"Attempt"'
                                    ',PARSE_JSON($1):"OriginalId"'
                                    ',PARSE_JSON($1):"ResultId"'
                                    ',CONVERT_TIMEZONE(\'Pacific/Auckland\', \'America/Los_Angeles\', TO_TIMESTAMP(PARSE_JSON($1):"LoadDateTime"))'
                                'FROM @my_json_stage_' + object + str(i) + '/' + object + str(i) + '.json.gz);').fetchall()

    print(table_list)

    con.cursor().execute('REMOVE @my_json_stage_' + object + str(i) + ' ;')