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
object = 'user_gamification_detail'
object_db = 'LITMOS.USER_GAMIFICATION_DETAIL'

con.cursor().execute('truncate table ' + object_db)

for i in range(1, 6):
    cursor = con.cursor()
    con.cursor().execute('create or replace file format myjsonformat_' + object + str(i) + ' type="JSON" strip_outer_array=true;')
    con.cursor().execute('create or replace stage my_json_stage_' + object + str(i) + ' file_format=myjsonformat_' + object + str(i) + ';')

    print(json_dir + object + str(i))
    con.cursor().execute('put ' + json_dir + object + str(i) + '* @my_json_stage_' + object + str(i) + ' auto_compress=true;')

    table_list = cursor.execute('COPY INTO LITMOS.USER_GAMIFICATION_DETAIL '
                                '(GAMIFICATION_ID'
                                ',USER_ID'
                                ',ITEM_ID'
                                ',ITEM_NAME'
                                ',EARNED_POINT'
                                ',EARNED_BADGE_ID'
                                ',DATE_COMPLETED_RAW'
                                ',DATE_COMPLETED'
                                ',BADGE_ID'
                                ',BADGE_TITLE'
                                ',BADGE_DESCRIPTION'
                                ',BADGE_ICON'
                                ',BADGE_ICON_BG_COLOR'
                                ',GAMIFICATION_COURSE_ITEM_ID'
                                ',GAMIFICATION_COURSE_COURSE_ID'
                                ',GAMIFICATION_COURSE_COURSE_NAME'
                                ',GAMIFICATION_LEARNING_PATH_ITEM_ID'
                                ',GAMIFICATION_LEARNING_PATH_LEARNING_PATH_ID'
                                ',GAMIFICATION_LEARNING_PATH_LEARNING_PATH_NAME'
                                ',LOAD_DATETIME)'
                                'FROM (SELECT '
                                    'PARSE_JSON($1):"ID"'
                                    ',PARSE_JSON($1):"UserId"'
                                    ',PARSE_JSON($1):"ItemID"'
                                    ',PARSE_JSON($1):"ItemName"'
                                    ',PARSE_JSON($1):"EarnedPoint"'
                                    ',PARSE_JSON($1):"EarnedBadgeID"'
                                    ',PARSE_JSON($1):"DateCompleted"'
                                    ',TO_TIMESTAMP_NTZ(SUBSTRING(PARSE_JSON($1):"DateCompleted", 7, 13))'
                                    ',PARSE_JSON($1):"Badge"."ID"'
                                    ',PARSE_JSON($1):"Badge"."Title"'
                                    ',PARSE_JSON($1):"Badge"."Description"'
                                    ',PARSE_JSON($1):"Badge"."Icon"'
                                    ',PARSE_JSON($1):"Badge"."IconBgColor"'
                                    ',PARSE_JSON($1):"GamificationCourseItemList"[0]."ID"'
                                    ',PARSE_JSON($1):"GamificationCourseItemList"[0]."CourseID"'
                                    ',PARSE_JSON($1):"GamificationCourseItemList"[0]."CourseName"'
                                    ',PARSE_JSON($1):"GamificationLearningPathItemList"[0]."ID"'
                                    ',PARSE_JSON($1):"GamificationLearningPathItemList"[0]."LearningPathID"'
                                    ',PARSE_JSON($1):"GamificationLearningPathItemList"[0]."LearningPathName"'
                                    ',CONVERT_TIMEZONE(\'Pacific/Auckland\', \'America/Los_Angeles\', TO_TIMESTAMP(PARSE_JSON($1):"LoadDateTime"))'
                                'FROM @my_json_stage_' + object + str(i) + '/' + object + str(i) + '.json.gz);').fetchall()

    print(table_list)

    con.cursor().execute('REMOVE @my_json_stage_' + object + str(i) + ' ;')