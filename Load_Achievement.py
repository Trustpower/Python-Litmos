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
object = 'achievement'
object_db = 'LITMOS.ACHIEVEMENT'

cursor = con.cursor()
con.cursor().execute('create or replace file format myjsonformat_' + object + ' type="JSON" strip_outer_array=true;')
con.cursor().execute('create or replace stage my_json_stage_' + object + ' file_format=myjsonformat_' + object + ';')

con.cursor().execute('put ' + json_dir + object + '* @my_json_stage_' + object + ' auto_compress=true;')

con.cursor().execute('truncate table ' + object_db)
table_list = cursor.execute('COPY INTO LITMOS.ACHIEVEMENT '
                            '(ACHIEVEMENT_ID'
                            ',COURSE_ID'
                            ',USER_ID'
                            ',TITLE'
                            ',DESCRIPTION'
                            ',ACHIEVEMENT_DATE_RAW'
                            ',ACHIEVEMENT_DATE'
                            ',COMPLIANT_TILL_DATE_RAW'
                            ',COMPLIANT_TILL_DATE'
                            ',SCORE'
                            ',RESULT'
                            ',TYPE'
                            ',FIRST_NAME'
                            ',LAST_NAME'
                            ',CERTIFICATE_ID'
                            ',LOAD_DATETIME)'
                            'FROM'
                                '(SELECT '
                                'PARSE_JSON($1):"AchievementId"'
                                ',PARSE_JSON($1):"CourseId"'
                                ',PARSE_JSON($1):"UserId"'
                                ',PARSE_JSON($1):"Title"'
                                ',PARSE_JSON($1):"Description"'
                                ',PARSE_JSON($1):"AchievementDate"'
                                ',TO_TIMESTAMP_NTZ(SUBSTRING(PARSE_JSON($1):"AchievementDate", 7, 13))'
                                ',PARSE_JSON($1):"CompliantTillDate"'
                                ',TO_TIMESTAMP_NTZ(SUBSTRING(PARSE_JSON($1):"CompliantTillDate", 7, 13))'
                                ',PARSE_JSON($1):"Score"'
                                ',PARSE_JSON($1):"Result"'
                                ',PARSE_JSON($1):"Type"'
                                ',PARSE_JSON($1):"FirstName"'
                                ',PARSE_JSON($1):"LastName"'
                                ',PARSE_JSON($1):"CertificateId"'
                                ',TO_TIMESTAMP(CURRENT_TIMESTAMP(9))'
                            'FROM @my_json_stage_' + object + '/' + object + '.json.gz)').fetchall()

print(table_list)

con.cursor().execute('REMOVE @my_json_stage_' + object + ' ;')