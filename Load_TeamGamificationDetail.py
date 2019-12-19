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
object = 'team_gamification_detail'
object_db = 'LITMOS.TEAM_GAMIFICATION_DETAIL'

cursor = con.cursor()
con.cursor().execute('create or replace file format myjsonformat_' + object + ' type="JSON" strip_outer_array=true;')
con.cursor().execute('create or replace stage my_json_stage_' + object + ' file_format=myjsonformat_' + object + ';')

con.cursor().execute('put ' + json_dir + object + '* @my_json_stage_' + object + ' auto_compress=true;')

con.cursor().execute('truncate table ' + object_db)
table_list = cursor.execute('COPY INTO LITMOS.TEAM_GAMIFICATION_DETAIL '
                            '(TEAM_ID'
                            ',USER_ID'
                            ',FIRST_NAME'
                            ',LAST_NAME'
                            ',GAMIFICATION_ACHIEVED_ID'
                            ',GAMIFICATION_ACHIEVED_ITEM_ID'
                            ',GAMIFICATION_ACHIEVED_ITEM_NAME'
                            ',GAMIFICATION_ACHIEVED_EARNED_POINT'
                            ',GAMIFICATION_ACHIEVED_EARNED_BADGE_ID'
                            ',GAMIFICATION_ACHIEVED_DATE_COMPLETED_RAW'
                            ',GAMIFICATION_ACHIEVED_DATE_COMPLETED'
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
                                'PARSE_JSON($1):"TeamId"'
                                ',PARSE_JSON($1):"Id"'
                                ',PARSE_JSON($1):"FirstName"'
                                ',PARSE_JSON($1):"LastName"'
                                ',PARSE_JSON($1):"GamificationAchievedItems"[0]."ID"'
                                ',PARSE_JSON($1):"GamificationAchievedItems"[0]."ItemID"'
                                ',PARSE_JSON($1):"GamificationAchievedItems"[0]."ItemName"'
                                ',PARSE_JSON($1):"GamificationAchievedItems"[0]."EarnedPoint"'
                                ',PARSE_JSON($1):"GamificationAchievedItems"[0]."EarnedBadgeID"'
                                ',PARSE_JSON($1):"GamificationAchievedItems"[0]."DateCompleted"'
                                ',TO_TIMESTAMP_NTZ(SUBSTRING(PARSE_JSON($1):"GamificationAchievedItems"[0]."DateCompleted", 7, 13))'
                                ',PARSE_JSON($1):"GamificationAchievedItems"[0]."Badge"."ID"'
                                ',PARSE_JSON($1):"GamificationAchievedItems"[0]."Badge"."Title"'
                                ',PARSE_JSON($1):"GamificationAchievedItems"[0]."Badge"."Description"'
                                ',PARSE_JSON($1):"GamificationAchievedItems"[0]."Badge"."Icon"'
                                ',PARSE_JSON($1):"GamificationAchievedItems"[0]."Badge"."IconBgColor"'
                                ',PARSE_JSON($1):"GamificationAchievedItems"[0]."GamificationCourseItemList"[0]."ID"'
                                ',PARSE_JSON($1):"GamificationAchievedItems"[0]."GamificationCourseItemList"[0]."CourseID"'
                                ',PARSE_JSON($1):"GamificationAchievedItems"[0]."GamificationCourseItemList"[0]."CourseName"'
                                ',PARSE_JSON($1):"GamificationAchievedItems"[0]."GamificationLearningPathItemList"[0]."ID"'
                                ',PARSE_JSON($1):"GamificationAchievedItems"[0]."GamificationLearningPathItemList"[0]."LearningPathID"'
                                ',PARSE_JSON($1):"GamificationAchievedItems"[0]."GamificationLearningPathItemList"[0]."LearningPathName"'
                                ',CONVERT_TIMEZONE(\'Pacific/Auckland\', \'America/Los_Angeles\', TO_TIMESTAMP(PARSE_JSON($1):"LoadDateTime"))'
                            'FROM @my_json_stage_' + object + '/' + object + '.json.gz);').fetchall()

print(table_list)

con.cursor().execute('REMOVE @my_json_stage_' + object + ' ;')