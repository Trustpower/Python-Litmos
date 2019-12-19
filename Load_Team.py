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
object = 'team'
object_db = 'LITMOS.TEAM'

cursor = con.cursor()
con.cursor().execute('create or replace file format myjsonformat_' + object + ' type="JSON" strip_outer_array=true;')
con.cursor().execute('create or replace stage my_json_stage_' + object + ' file_format=myjsonformat_' + object + ';')

con.cursor().execute('put ' + json_dir + object + '* @my_json_stage_' + object + ' auto_compress=true;')

con.cursor().execute('truncate table ' + object_db)
table_list = cursor.execute('COPY INTO LITMOS.TEAM '
                            '(TEAM_ID'
                            ',NAME'
                            ',TEAM_CODE_FOR_BULK_IMPORT'
                            ',PARENT_TEAM_ID'
                            ',LOAD_DATETIME)'
                            'FROM (SELECT '
                                'PARSE_JSON($1):"Id"'
                                ',PARSE_JSON($1):"Name"'
                                ',PARSE_JSON($1):"TeamCodeForBulkImport"'
                                ',PARSE_JSON($1):"ParentTeamId"'
                                ',TO_TIMESTAMP(CURRENT_TIMESTAMP(9))'
                            'FROM @my_json_stage_' + object + '/' + object + '.json.gz);').fetchall()

print(table_list)

con.cursor().execute('REMOVE @my_json_stage_' + object + ' ;')