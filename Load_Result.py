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
object = 'result'
object_db = 'LITMOS.RESULT'

cursor = con.cursor()
con.cursor().execute('create or replace file format myjsonformat_' + object + ' type="JSON" strip_outer_array=true;')
con.cursor().execute('create or replace stage my_json_stage_' + object + ' file_format=myjsonformat_' + object + ';')

con.cursor().execute('put ' + json_dir + object + '* @my_json_stage_' + object + ' auto_compress=true;')

con.cursor().execute('truncate table ' + object_db)
table_list = cursor.execute('COPY INTO LITMOS."RESULT" '
                            '(COURSE_ID,'
                            'USER_ID,'
                            'COURSE_ORIGINAL_ID,'
                            'USER_ORIGINAL_ID,'
                            'USER_NAME,'
                            'FIRST_NAME,'
                            'LAST_NAME,'
                            'ACTIVE,'
                            'EMAIL,'
                            'ACCESS_LEVEL,'
                            'LOGIN_KEY,'
                            'UPDATED_DATE_RAW,'
                            'UPDATED_DATE,'
                            'CODE,'
                            'COURSE_NAME,'
                            'COMPLETED,'
                            'PERCENTAGE_COMPLETE,'
                            'COMPLETED_DATE_RAW,'
                            'COMPLETED_DATE,'
                            'UP_TO_DATE,'
                            'OVERDUE,'
                            'COMPLIANT_TILL_DATE_RAW,'
                            'COMPLIANT_TILL_DATE,'
                            'LOAD_DATETIME) '
                            'FROM '
                                '(SELECT PARSE_JSON($1):"CourseId"'
                                ',PARSE_JSON($1):"Id"'
                                ',PARSE_JSON($1):"CourseOriginalId"'
                                ',PARSE_JSON($1):"UserOriginalId"'
                                ',PARSE_JSON($1):"Username"'
                                ',PARSE_JSON($1):"FirstName"'
                                ',PARSE_JSON($1):"LastName"'
                                ',PARSE_JSON($1):"Active"'
                                ',PARSE_JSON($1):"Email"'
                                ',PARSE_JSON($1):"AccessLevel"'
                                ',PARSE_JSON($1):"LoginKey"'
                                ',PARSE_JSON($1):"UpdatedDate"'
                                ',TO_TIMESTAMP_NTZ(SUBSTRING(PARSE_JSON($1):"UpdatedDate", 7, CHARINDEX(\'-\',PARSE_JSON($1):"UpdatedDate")-7))'
                                ',PARSE_JSON($1):"Code"'
                                ',PARSE_JSON($1):"CourseName"'
                                ',PARSE_JSON($1):"Completed"'
                                ',PARSE_JSON($1):"PercentageComplete"'
                                ',PARSE_JSON($1):"CompletedDate"'
                                ',TO_TIMESTAMP_NTZ(SUBSTRING(PARSE_JSON($1):"CompletedDate", 7, CHARINDEX(\'-\',PARSE_JSON($1):"CompletedDate")-7))'
                                ',PARSE_JSON($1):"UpToDate"'
                                ',PARSE_JSON($1):"Overdue"'
                                ',PARSE_JSON($1):"CompliantTillDate"'
                                ',TO_TIMESTAMP_NTZ(SUBSTRING(PARSE_JSON($1):"CompliantTillDate", 7, CHARINDEX(\'-\',PARSE_JSON($1):"CompliantTillDate")-7))'
                                ',TO_TIMESTAMP(CURRENT_TIMESTAMP(9))'
                            'FROM @my_json_stage_' + object + '/' + object + '.json.gz)').fetchall()

print(table_list)

con.cursor().execute('REMOVE @my_json_stage_' + object + ' ;')