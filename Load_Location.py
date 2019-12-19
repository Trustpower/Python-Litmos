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
object = 'location'
object_db = 'LITMOS.LOCATION'

cursor = con.cursor()
con.cursor().execute('create or replace file format myjsonformat_' + object + ' type="JSON" strip_outer_array=true;')
con.cursor().execute('create or replace stage my_json_stage_' + object + ' file_format=myjsonformat_' + object + ';')

con.cursor().execute('put ' + json_dir + object + '* @my_json_stage_' + object + ' auto_compress=true;')

con.cursor().execute('truncate table ' + object_db)
table_list = cursor.execute('COPY INTO LITMOS.LOCATION'
                            '(LOCATION_ID'
                            ',LOCATION'
                            ',DESCRIPTION'
                            ',STREET1'
                            ',STREET2'
                            ',CITY'
                            ',STATE'
                            ',POSTAL_CODE'
                            ',COUNTRY'
                            ',PHONE'
                            ',LOAD_DATETIME'
                            ')'
                            'FROM ('
                            'SELECT '
                                'PARSE_JSON($1):"LocationId"'
                                ',PARSE_JSON($1):"Location"'
                                ',PARSE_JSON($1):"Description"'
                                ',PARSE_JSON($1):"Street1"'
                                ',PARSE_JSON($1):"Street2"'
                                ',PARSE_JSON($1):"City"'
                                ',PARSE_JSON($1):"State"'
                                ',PARSE_JSON($1):"PostalCode"'
                                ',PARSE_JSON($1):"Country"'
                                ',PARSE_JSON($1):"Phone"'
                                ',TO_TIMESTAMP(CURRENT_TIMESTAMP(9))'
                            'FROM @my_json_stage_' + object + '/' + object + '.json.gz);').fetchall()

print(table_list)

con.cursor().execute('REMOVE @my_json_stage_' + object + ' ;')