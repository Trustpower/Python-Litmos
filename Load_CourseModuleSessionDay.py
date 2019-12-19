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
object = 'course_module_session_day'
object_db = 'LITMOS.COURSE_MODULE_SESSION_DAY'

cursor = con.cursor()
con.cursor().execute('create or replace file format myjsonformat_' + object + ' type="JSON" strip_outer_array=true;')
con.cursor().execute('create or replace stage my_json_stage_' + object + ' file_format=myjsonformat_' + object + ';')

con.cursor().execute('put ' + json_dir + object + '* @my_json_stage_' + object + ' auto_compress=true;')

con.cursor().execute('truncate table ' + object_db)
table_list = cursor.execute('COPY INTO LITMOS.COURSE_MODULE_SESSION_DAY '
                            '(SESSION_DAY_ID'
                            ',COURSE_ID'
                            ',COURSE_MODULE_ID'
                            ',SESSION_ID'
                            ',SESSION_DAY_START_DATE_RAW'
                            ',SESSION_DAY_START_DATE'
                            ',SESSION_DAY_END_DATE_RAW'
                            ',SESSION_DAY_END_DATE'
                            ',SESSION_DAY_START_TIME'
                            ',SESSION_DAY_END_TIME'
                            ',SESSION_DAY_SEND_REMINDER'
                            ',SESSION_DAY_REMINDER_VALUE'
                            ',SESSION_DAY_REMINDER_METRIC'
                            ',LOAD_DATETIME)'
                            'FROM (SELECT '
                                'PARSE_JSON($1):"Id"'
                                ',PARSE_JSON($1):"CourseId"'
                                ',PARSE_JSON($1):"ModuleId"'
                                ',PARSE_JSON($1):"SessionId"'
                                ',PARSE_JSON($1):"StartDate"'
                                ',TO_TIMESTAMP_NTZ(SUBSTRING(PARSE_JSON($1):"StartDate", 7, 13))'
                                ',PARSE_JSON($1):"EndDate"'
                                ',TO_TIMESTAMP_NTZ(SUBSTRING(PARSE_JSON($1):"EndDate", 7, 13))'
                                ',PARSE_JSON($1):"StartTime"'
                                ',PARSE_JSON($1):"EndTime"'
                                ',PARSE_JSON($1):"SendReminder"'
                                ',PARSE_JSON($1):"ReminderValue"'
                                ',PARSE_JSON($1):"ReminderMetric"'
                                ',TO_TIMESTAMP(PARSE_JSON($1):"LoadDateTime")'
                                #',CONVERT_TIMEZONE(\'Pacific/Auckland\', \'America/Los_Angeles\', TO_TIMESTAMP(PARSE_JSON($1):"LoadDateTime"))'
                            'FROM @my_json_stage_' + object + '/' + object + '.json.gz);').fetchall()

print(table_list)

con.cursor().execute('REMOVE @my_json_stage_' + object + ' ;')