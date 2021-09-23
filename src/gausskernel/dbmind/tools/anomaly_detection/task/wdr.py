import logging
import re
import time

import global_vars
from utils import DBAgent, extract_table_from_sql, input_sql_processing


class WDR:
    __tablename__ = 'wdr'

    def __init__(self, db_port, db_type):
        self.port = db_port
        self.db_type = db_type

    def extract_index(self, database, table):
        sql = "select indexname, indexdef from pg_indexes where tablename='{table}'".format(table=table)
        with DBAgent(port=self.port, database=database) as db:
            res = db.fetch_all_result(sql)
        return dict(res)

    def mapper_function(self, value):
        index_info = {}
        database = value[0]
        tables = extract_table_from_sql(value[1])
        if tables:
            for table in tables:
                indexes = self.extract_index(database, table)
                index_info[table] = indexes
        tables = ','.join(tables)
        index_info = str(index_info)
        wdr_features = ';'.join([str(item) for item in value[5:]])
        # Delete 'p_rows' 'p_time' in explain if value[2] is not None.
        if value[2] is not None:
            explain = re.sub(r"Datanode Name:.+?\n", "", re.sub(r' p-time=[\d\.]+? p-rows=[\d] ', r' ', value[2]))
            explain = explain.replace('\"', '\'')
        else:
            explain = ''
        return (value[0], tables, input_sql_processing(value[1]), explain,
                value[3].strftime(global_vars.DATE_FORMAT), value[4].strftime(global_vars.DATE_FORMAT), index_info,
                wdr_features)

    def wdr_features(self, start_time, end_time):
        if start_time and end_time:
            sql = "select db_name, query, query_plan, start_time, finish_time, n_returned_rows, n_tuples_fetched, " \
                  "n_tuples_returned, n_tuples_inserted, n_tuples_updated, n_tuples_deleted, n_blocks_fetched, " \
                  "n_blocks_hit, db_time, cpu_time, execution_time, parse_time, plan_time, rewrite_time, " \
                  "pl_execution_time, pl_compilation_time, data_io_time, lock_count, lock_time, lock_wait_count, " \
                  "lock_wait_time, lock_max_count, lwlock_count, lwlock_wait_count, lwlock_time, lwlock_wait_time from " \
                  "statement_history where finish_time between '{start_time}' and '{end_time}'" \
                  .format(start_time=start_time, end_time=end_time)
        with DBAgent(port=self.port, database='postgres') as db:
            result = db.fetch_all_result(sql)
            if result:
                result = list(filter(lambda x: re.match(r'UPDATE|SELECT|INSERT|DELETE', x[1].strip().upper()), result))
                result = list(map(self.mapper_function, result))
            return result

    def output(self):
        # Collect wdr only on CN or single node.
        if self.db_type not in ('cn', 'single'):
            return []
        start_time = global_vars.SLOW_START_TIME
        end_time = int(time.time())
        global_vars.SLOW_START_TIME = end_time
        start_time_string = time.strftime(global_vars.DATE_FORMAT, time.localtime(start_time))
        end_time_string = time.strftime(global_vars.DATE_FORMAT, time.localtime(end_time))
        result = self.wdr_features(start_time_string, end_time_string)
        return result
