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
object = 'course_module_session_detail'
object_db = 'LITMOS.COURSE_MODULE_SESSION'

cursor = con.cursor()
con.cursor().execute('create or replace file format myjsonformat_' + object + ' type="JSON" strip_outer_array=true;')
con.cursor().execute('create or replace stage my_json_stage_' + object + ' file_format=myjsonformat_' + object + ';')

con.cursor().execute('put ' + json_dir + object + '* @my_json_stage_' + object + ' auto_compress=true;')

con.cursor().execute('truncate table ' + object_db)
table_list = cursor.execute('COPY INTO LITMOS.COURSE_MODULE_SESSION '
                            '(SESSION_ID'
                            ',COURSE_ID'
                            ',COURSE_MODULE_ID'
                            ',INSTRUCTOR_USER_ID'
                            ',INSTRUCTOR_NAME'
                            ',SESSION_NAME'
                            ',SESSION_TYPE'
                            ',TIME_ZONE'
                            ',LOCATION'
                            ',LOCATION_ID'
                            ',SESSION_START_DATE_RAW'
                            ',SESSION_START_DATE'
                            ',SESSION_END_DATE_RAW'
                            ',SESSION_END_DATE'
                            ',COURSE_NAME'
                            ',MODULE_NAME'
                            ',SLOTS'
                            ',ACCEPTED'
                            ',ENABLE_WAITLIST'
                            ',LOAD_DATETIME)'
                            'FROM (SELECT '
                                'PARSE_JSON($1):"Id"'
                                ',PARSE_JSON($1):"CourseId"'
                                ',PARSE_JSON($1):"ModuleId"'
                                ',PARSE_JSON($1):"InstructorUserId"'
                                ',PARSE_JSON($1):"InstructorName"'
                                ',PARSE_JSON($1):"Name"'
                                ',PARSE_JSON($1):"SessionType"'
                                ',PARSE_JSON($1):"TimeZone"'
                                ',PARSE_JSON($1):"Location"'
                                ',PARSE_JSON($1):"LocationId"'
                                ',PARSE_JSON($1):"StartDate"'
                                ',TO_TIMESTAMP_NTZ(SUBSTRING(PARSE_JSON($1):"StartDate", 7, 13))'
                                ',PARSE_JSON($1):"EndDate"'
                                ',TO_TIMESTAMP_NTZ(SUBSTRING(PARSE_JSON($1):"EndDate", 7, 13))'
                                ',PARSE_JSON($1):"CourseName"'
                                ',PARSE_JSON($1):"ModuleName"'
                                ',PARSE_JSON($1):"Slots"'
                                ',PARSE_JSON($1):"Accepted"'
                                ',PARSE_JSON($1):"EnableWaitList"'
                                ',TO_TIMESTAMP(PARSE_JSON($1):"LoadDateTime")'
                                #',CONVERT_TIMEZONE(\'Pacific/Auckland\', \'America/Los_Angeles\', TO_TIMESTAMP(PARSE_JSON($1):"LoadDateTime"))'
                            'FROM @my_json_stage_' + object + '/' + object + '.json.gz);').fetchall()

print(table_list)

con.cursor().execute('REMOVE @my_json_stage_' + object + ' ;')