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
object = 'team_course'
object_db = 'LITMOS.TEAM_COURSE'

cursor = con.cursor()
con.cursor().execute('create or replace file format myjsonformat_' + object + ' type="JSON" strip_outer_array=true;')
con.cursor().execute('create or replace stage my_json_stage_' + object + ' file_format=myjsonformat_' + object + ';')

con.cursor().execute('put ' + json_dir + object + '* @my_json_stage_' + object + ' auto_compress=true;')

con.cursor().execute('truncate table ' + object_db)
table_list = cursor.execute('COPY INTO LITMOS.TEAM_COURSE '
                            '(TEAM_ID'
                            ',COURSE_ID'
                            ',CODE'
                            ',NAME'
                            ',ACTIVE'
                            ',FOR_SALE'
                            ',ORIGINAL_ID'
                            ',DESCRIPTION'
                            ',ECOMMERCE_SHORT_DESCRIPTION'
                            ',ECOMMERCE_LONG_DESCRIPTION'
                            ',COURSE_CODE_FOR_BULK_IMPORT'
                            ',PRICE'
                            ',ACCESS_TILL_DATE_RAW'
                            ',ACCESS_TILL_DATE'
                            ',ACCESS_TILL_DAYS'
                            ',COURSE_TEAM_LIBRARY'
                            ',LOAD_DATETIME)'
                            'FROM (SELECT '
                                'PARSE_JSON($1):"TeamId"'
                                ',PARSE_JSON($1):"Id"'
                                ',PARSE_JSON($1):"Code"'
                                ',PARSE_JSON($1):"Name"'
                                ',PARSE_JSON($1):"Active"'
                                ',PARSE_JSON($1):"ForSale"'
                                ',PARSE_JSON($1):"OriginalId"'
                                ',PARSE_JSON($1):"Description"'
                                ',PARSE_JSON($1):"EcommerceShortDescription"'
                                ',PARSE_JSON($1):"EcommerceLongDescription"'
                                ',PARSE_JSON($1):"CourseCodeForBulkImport"'
                                ',PARSE_JSON($1):"Price"'
                                ',PARSE_JSON($1):"AccessTillDate"'
                                ',TO_TIMESTAMP_NTZ(SUBSTRING(PARSE_JSON($1):"AccessTillDate",  7, 13))'
                                ',PARSE_JSON($1):"AccessTillDays"'
                                ',PARSE_JSON($1):"CourseTeamLibrary"'
                                ',CONVERT_TIMEZONE(\'Pacific/Auckland\', \'America/Los_Angeles\', TO_TIMESTAMP(PARSE_JSON($1):"LoadDateTime"))'
                            'FROM @my_json_stage_' + object + '/' + object + '.json.gz);').fetchall()

print(table_list)

con.cursor().execute('REMOVE @my_json_stage_' + object + ' ;')