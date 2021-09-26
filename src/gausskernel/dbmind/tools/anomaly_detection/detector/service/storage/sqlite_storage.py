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
import json
import logging
import os
import sqlite3
import time

import config
import global_vars
from utils import TimeString

service_logger = logging.getLogger('service')
m_logger = logging.getLogger('detector')


class SQLiteStorage:
    """
    This class is used for connecting to database and
     acquiring data by timestamp or number.
    """

    def __init__(self, dbpath):
        """
        :param table: string, name of table in database.
        :param dbpath: string, name of database path.
        """
        self._dbpath = dbpath
        self._conn = None
        self._cur = None
        self.sql_operation = None
        self.load_sql_operation()
        self.storage_duration = TimeString(config.get('database', 'storage_duration')).to_second()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def load_sql_operation(self):
        if not os.path.exists(global_vars.TABLE_INFO_PATH):
            service_logger.error(
                "Not found table information: {tableinfo}.".format(tableinfo=global_vars.TABLE_INFO_PATH))
            raise SystemExit(1)
        with open(global_vars.TABLE_INFO_PATH, 'r') as f:
            self.sql_operation = json.load(f)

    def connect(self):
        self._conn = sqlite3.connect(self._dbpath, timeout=2)
        self._cur = self._conn.cursor()

    def execute(self, sql):
        try:
            self._cur.execute(sql)
            self._conn.commit()
        except Exception as e:
            service_logger.warning(e, exc_info=True)
            self._conn.rollback()

    def fetch_all_result(self, sql):
        self._cur.execute(sql)
        result = self._cur.fetchall()
        return result

    def execute_sql(self, sql):
        try:
            self._cur.execute(sql)
            self._conn.commit()
        except Exception as e:
            service_logger.warning(e, exc_info=True)
            self._conn.rollback()

    def create_table(self):
        for table in self.sql_operation['sqlite']:
            create_table = self.sql_operation['sqlite'][table]['create_table']
            self._cur.execute(create_table)

    def insert(self, table, *args):
        insert_opt = self.sql_operation['sqlite'][table]['insert']
        parameters = tuple((item for item in args))
        sql = insert_opt % parameters
        try:
            self._cur.execute(sql)
        except Exception as e:
            service_logger.warning(e, exc_info=True)
            self._conn.rollback()
            return
        self._conn.commit()
        self.limit_capacity_by_time(table, parameters[0])

    def limit_capacity_by_time(self, table, last_timestamp):
        earliest_timestamp = self.get_earliest_timestamp(table)
        margin_timestamp = last_timestamp - self.storage_duration
        if earliest_timestamp < margin_timestamp:
            limit_max_period_opt = self.sql_operation['sqlite'][table]['limit_max_periods']
            self._cur.execute(limit_max_period_opt % margin_timestamp)
            self._conn.commit()

    def get_latest_timestamp(self, table):
        operation = self.sql_operation['function']['get_latest_timestamp']
        self._cur.execute(operation.format(table=table))
        last_timestamp = self._cur.fetchall()[0][0]
        return last_timestamp

    def get_earliest_timestamp(self, table):
        operation = self.sql_operation['function']['get_earliest_timestamp']
        self._cur.execute(operation.format(table=table))
        earliest_timestamp = self._cur.fetchall()[0][0]
        return earliest_timestamp

    def select_timeseries_by_timestamp(self, table, field, period, timestamp):
        """
        Acquire all timeseries in a timedelta from the present.
        :param timestamp: the timestamp of the end time.
        :param field: field of database.
        :param table: string, table name from database.
        :param period: string, name of timedelta from now, like '100S', '1H'.
        :return: list, timeseries dataset.
        """
        timeseries = []
        times = 0
        times_limit = 5
        while times < times_limit:
            try:
                last_timestamp = timestamp
                select_timestamp = last_timestamp - TimeString(period).to_second()
                operation = self.sql_operation['function']['get_timeseries_by_timestamp']
                self._cur.execute(
                    operation.format(table=table, field=field, select_timestamp=select_timestamp))
                timeseries = self._cur.fetchall()
                if not timeseries:
                    m_logger.warning("Get no time series from '{table}', retrying...".format(table=table))
                else:
                    return timeseries
            except Exception as e:
                m_logger.warning('An exception (%s) occurs when getting time series, retrying...', e,
                                 exc_info=True)
                times += 1
                time.sleep(0.2)
        return timeseries

    def select_timeseries_by_number(self, table, field, number):
        """
        Acquire number of timeseries from the present.
        :param field: string, a field that needs be selected in the table.
        :param table: string, table name in database.
        :param number: int, number of timeseries from present.
        :return: list, timeseries dataset.
        """
        timeseries = []
        times = 0
        times_limit = 5
        while times < times_limit:
            try:
                operation = self.sql_operation['function']['get_timeseries_by_number']
                self._cur.execute(operation.format(table=table, field=field, number=number))
                timeseries = self._cur.fetchall()
                if not timeseries:
                    m_logger.warning("Get no time series from '{table}', retrying...".format(table=table))
                else:
                    return timeseries

            except Exception as e:
                m_logger.error('An exception (%s) occurs when getting time series: .', e, exc_info=True)
                times += 1
                time.sleep(0.2)
            return timeseries

    def get_timeseries(self, table, field, period, timestamp=None):
        """
        Acquire timeseries from database by timestamp or number.
        :param table: string, table name from database.
        :param period: int or string, represent number or period like: 100, '100S'.
        :return: list, timeseries dataset.
        """
        if not self.check_table(table):
            return []
        if isinstance(period, int):
            timeseries = self.select_timeseries_by_number(table=table, field=field, number=period)
        else:
            timeseries = self.select_timeseries_by_timestamp(table=table, field=field, period=period,
                                                             timestamp=timestamp)
        timeseries = list(map(lambda x: (x[0], float(x[1])), timeseries))
        return timeseries

    def get_all_tables(self):
        """
        Acquire all tables in database.
        :return: list, list of table name
        """
        operation = self.sql_operation['function']['get_all_tables']
        self._cur.execute(operation)
        tables = self._cur.fetchall()
        tables = [item[0] for item in tables]
        return tables

    def get_all_fields(self, table):
        operation = self.sql_operation['function']['get_all_fields']
        self._cur.execute(operation.format(table=table))
        fields = self._cur.fetchall()
        fields = [item[1] for item in fields[1:]]
        return fields

    def get_table_rows(self, table):
        """
        Acquire table rows.
        :return: string, table name.
        """
        if self.check_table(table):
            operation = self.sql_operation['function']['get_table_rows']
            self._cur.execute(operation.format(table=table))
            table_rows = self._cur.fetchall()
            table_rows = table_rows[0][0]
        else:
            table_rows = 0
        return table_rows

    def check_table(self, table):
        """
        Check if table is in the database.
        :param table: string, table name.
        :return: boolean, True/False.
        """
        operation = self.sql_operation['function']['check_table']
        self._cur.execute(operation)
        tables = self._cur.fetchall()
        tables = [item[0] for item in tables]
        if table not in tables:
            return False
        return True

    def close(self):
        self._cur.close()
        self._conn.close()
