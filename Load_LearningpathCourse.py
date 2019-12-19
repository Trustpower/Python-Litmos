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
object = 'learning_path_course'
object_db = 'LITMOS.LEARNING_PATH_COURSE'

cursor = con.cursor()
con.cursor().execute('create or replace file format myjsonformat_' + object + ' type="JSON" strip_outer_array=true;')
con.cursor().execute('create or replace stage my_json_stage_' + object + ' file_format=myjsonformat_' + object + ';')

con.cursor().execute('put ' + json_dir + object + '* @my_json_stage_' + object + ' auto_compress=true;')

con.cursor().execute('truncate table ' + object_db)
table_list = cursor.execute('COPY INTO LITMOS.LEARNING_PATH_COURSE '
                            '(LEARNING_PATH_ID'
                            ',COURSE_ID'
                            ',CODE'
                            ',NAME'
                            ',DESCRIPTION'
                            ',ACTIVE'
                            ',FOR_SALE'
                            ',EQUIVALENCY_ID'
                            ',EQUIVALENCY_GROUP_NAME'
                            ',ORIGINAL_ID'
                            ',PRICE'
                            ',LOAD_DATETIME'
                            ')'
                            'FROM (SELECT '
                                'PARSE_JSON($1):"LearningPathId"'
                                ',PARSE_JSON($1):"Id"'
                                ',PARSE_JSON($1):"Code"'
                                ',PARSE_JSON($1):"Name"'
                                ',PARSE_JSON($1):"Description"'
                                ',PARSE_JSON($1):"Active"'
                                ',PARSE_JSON($1):"ForSale"'
                                ',PARSE_JSON($1):"EquivalencyId"'
                                ',PARSE_JSON($1):"EquivalencyGroupName"'
                                ',PARSE_JSON($1):"OriginalId"'
                                ',PARSE_JSON($1):"Price"'
                                ',CONVERT_TIMEZONE(\'Pacific/Auckland\', \'America/Los_Angeles\', TO_TIMESTAMP(PARSE_JSON($1):"LoadDateTime"))'
                            'FROM @my_json_stage_' + object + '/' + object + '.json.gz)').fetchall()

print(table_list)

con.cursor().execute('REMOVE @my_json_stage_' + object + ' ;')