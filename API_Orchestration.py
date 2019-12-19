#!/usr/bin/env python3
import os
import json
import multiprocessing as mp
from multiprocessing import Pool
import re
import subprocess as sb
from subprocess import Popen


processes = (
             'API_CourseSubObjects.py'
             ,'API_LearningpathSubObjects.py'
             ,'API_TeamSubObjects.py'
             ,'API_Location.py'
             )

def run_process(process):
    sb.call(['python3', '{}'.format(process)]);


if __name__ == '__main__':

    print(run_process, processes)

    num_workers = mp.cpu_count()

    print(num_workers)
    
    pool = Pool(processes=num_workers)
    pool.map(run_process, processes)
    pool.close()

    ##CALL LOADS
    sb.call(['python3', 'Load_Course.py'])
    sb.call(['python3', 'Load_CourseModule.py'])
    sb.call(['python3', 'Load_CourseModuleILT.py'])
    sb.call(['python3', 'Load_CourseUser.py'])
    sb.call(['python3', 'Load_CourseCustomField.py'])
    sb.call(['python3', 'Load_LearningpathCourse.py'])
    sb.call(['python3', 'Load_LearningpathUser.py'])
    sb.call(['python3', 'Load_TeamAdministrator.py'])
    sb.call(['python3', 'Load_TeamCourse.py'])
    sb.call(['python3', 'Load_TeamLeader.py'])
    sb.call(['python3', 'Load_TeamLearningpath.py'])
    sb.call(['python3', 'Load_TeamUser.py'])
    sb.call(['python3', 'Load_TeamGamificationDetail.py'])
    sb.call(['python3', 'Load_Location.py'])
    sb.call(['python3', 'Load_CourseModuleSessionAttendance.py'])
    sb.call(['python3', 'Load_CourseModuleSessionRollCall.py'])
    sb.call(['python3', 'Load_CourseModuleSession.py'])
    sb.call(['python3', 'Load_CourseModuleSessionDay.py'])


    processes = (
                'API_UserSubObjects.py'
                ,'API_UserSubObjects2.py'
                ,'API_UserSubObjects3.py'
                ,'API_UserSubObjects4.py'
                ,'API_UserSubObjects5.py'
                )

    if __name__ == '__main__':
        num_workers = mp.cpu_count()

        print(num_workers)

        pool = Pool(processes=num_workers)
        pool.map(run_process, processes)
        pool.close()

        #CALL LOAD
        sb.call(['python3', 'Load_UserBadge.py'])
        sb.call(['python3', 'Load_UserCourse.py'])
        sb.call(['python3', 'Load_UserGamificationDetail.py'])
        sb.call(['python3', 'Load_UserGamificationSummary.py'])
        sb.call(['python3', 'Load_UserLearningpath.py'])

        processes = (
                    'API_UserCourseModuleResult.py'
                    ,'API_UserCourseModuleResult2.py'
                    ,'API_UserCourseModuleResult3.py'
                    )

        if __name__ == '__main__':
            num_workers = mp.cpu_count()

            print(num_workers)

            pool = Pool(processes=num_workers)
            pool.map(run_process, processes)
            pool.close()


            processes = (
                        'API_UserCourseModuleResult4.py'
                        ,'API_UserCourseModuleResult5.py'
                        )

            if __name__ == '__main__':
                num_workers = mp.cpu_count()

                print(num_workers)

                pool = Pool(processes=num_workers)
                pool.map(run_process, processes)
                pool.close()

                # CALL LOAD
                sb.call(['python3', 'API_UserCourseModuleResultF.py'])
                sb.call(['python3', 'API_UserCourseResultF.py'])