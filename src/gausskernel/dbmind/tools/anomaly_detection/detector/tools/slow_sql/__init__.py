"""
Copyright (c) 2020 Huawei Technologies Co.,Ltd.

openGauss is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:

         http://license.coscl.org.cn/MulanPSL2

THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
"""
import logging
import sys
import os
import time

import global_vars
from detector.tools.slow_sql import diagnosing

sql_rca_logger = logging.getLogger('sql_rca')
detector_logger = logging.getLogger('detector')


class SQL_RCA:
    def __init__(self, **kwargs):
        self.data_handler = kwargs['data_handler']
        self.database_dir = kwargs['database_dir']
        self.interval = kwargs['interval']

    def run(self):
        if not os.path.exists(self.database_dir):
            detector_logger.error("{database} is not found.".format(database=self.database_dir))
            sys.exit(0)

        try:
            detector_start_time = global_vars.DETECTOR_START_TIME
            detector_end_time = int(time.time())
            global_vars.DETECTOR_START_TIME = detector_end_time
            sql = "select query, start_time, finish_time from {table} where timestamp between {start_time} and {end_time};"
            for database in os.listdir(self.database_dir):
                if 'journal' in database:
                    continue

                database_path = os.path.join(self.database_dir, database)
                with self.data_handler(database_path) as db:
                    results = db.fetch_all_result(
                        sql.format(table='wdr', start_time=detector_start_time, end_time=detector_end_time))
                    for query, start_time, finish_time in results:
                        index = 1
                        if 'pg_stat_activity' in query:
                            continue
                        # prevent question marks from causing errors in the root cause analysis module
                        input_query = query.replace('?', '2')
                        rcas = diagnosing.diagnose_auto(db, input_query, start_time)
                        sql_rca_logger.info(
                            "START_TIME: %s, FINISH_TIME: %s.\n SQL Query: %s.", start_time, finish_time, query
                        )

                        if not rcas:
                            rca_ana = "query has no slow features."
                            suggestion_ana = "please check the query threshold, check the log, and analyze the reason."
                            sql_rca_logger.info(
                               "RCA: {rca}; Suggestion: {suggestion}".format(index=index,
                                                                             rca=rca_ana,
                                                                             suggestion=suggestion_ana)) 

                        else:
                            for rca, suggestion in rcas:
                                sql_rca_logger.info(
                                    "{index}: RCA: {rca}; Suggestion: {suggestion}".format(index=index,
                                                                                           rca=rca,
                                                                                           suggestion=suggestion))
                                index += 1
                        sql_rca_logger.info('\n')
        except Exception as e:
            detector_logger.error(str(e), exc_info=True)
            sys.exit(-1)
