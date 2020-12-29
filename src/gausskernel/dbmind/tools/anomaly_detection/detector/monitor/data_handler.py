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
import os
import sqlite3
import time

from utils import transform_time_string
from .monitor_logger import logger


class DataHandler:
    '''
    This class is used for connecting to database and acquiring data by timestamp or number.
    '''

    def __init__(self, dbpath):
        """
        :param table: string, name of table in database.
        :param dbpath: string, name of database path.
        """
        self._dbpath = dbpath
        self._conn = None
        self._cur = None

    def __enter__(self):
        self.connect_db()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect_db(self):
        if not os.path.exists(self._dbpath):
            logger.error('{dbpath} not found'.format(dbpath=self._dbpath), exc_info=True)
            return
        self._conn = sqlite3.connect(self._dbpath, timeout=2)
        self._cur = self._conn.cursor()

    def select_timeseries_by_timestamp(self, table, period):
        """
        Acquire all timeseries in a timedelta from the present.
        :param table: string, table name from database.
        :param period: string, name of timedelta from now, like '100S', '1H'.
        :return: list, timeseries dataset.
        """
        timeseries = []
        times = 0
        times_limit = 5
        while times < times_limit:
            try:
                get_last_timestamp_sql = "select timestamp from {table} order by timestamp desc limit 1"
                self._cur.execute(get_last_timestamp_sql.format(table=table))
                last_timestamp = self._cur.fetchall()[0][0]
                time_interval = transform_time_string(period, mode='to_second')
                select_timestamp = last_timestamp - time_interval
                get_timeseries_sql = "select timestamp, value from {table} where timestamp >= '{select_timestamp}'"
                self._cur.execute(get_timeseries_sql.format(table=table, select_timestamp=select_timestamp))
                timeseries = self._cur.fetchall()
                if not timeseries:
                    logger.warn("get no timeseries from {table}', retry...".format(table=table))
                else:
                    return timeseries
            except Exception as e:
                logger.exception('exception occur when get timeseries: {error}, retry...'.format(error=str(e)),
                                 exc_info=True)
                times += 1
                time.sleep(0.11)
        return timeseries

    def select_timeseries_by_number(self, table, number):
        """
        Acquire number of timeseries from the present.
        :param table: string, table name in database.
        :param number: int, number of timeseries from present.
        :return: list, timeseries dataset.
        """
        timeseries = []
        times = 0
        times_limit = 5
        while times < times_limit:
            try:
                sql = "select * from (select * from {table} order by timestamp desc limit {number}) order by timestamp"
                self._cur.execute(sql.format(table=table, number=number))
                timeseries = self._cur.fetchall()
                if not timeseries:
                    logger.warn("get no timeseries from {table}', retry...".format(table=table))
                else:
                    return timeseries

            except Exception as e:
                logger.exception('exception occur when get timeseries: {error}'.format(error=str(e)), exc_info=True)
                times += 1
                time.sleep(0.11)
            return timeseries

    def get_timeseries(self, table, period):
        """
        Acquire timeseries from database by timestamp or number.
        :param table: string, table name from database.
        :param period: int or string, represent number or period like: 100, '100S'.
        :return: list, timeseries dataset.
        """
        if not self.check_table(table):
            return []
        if isinstance(period, int):
            timeseries = self.select_timeseries_by_number(table=table, number=period)
        else:
            timeseries = self.select_timeseries_by_timestamp(table=table, period=period)
        return timeseries

    def get_all_tables(self):
        """
        Acquire all tables in database.
        :return: list, list of table name.
        """
        sql = "select name from sqlite_master where type = 'table'"
        self._cur.execute(sql)
        tables = self._cur.fetchall()
        tables = [item[0] for item in tables]
        return tables

    def get_table_rows(self, table):
        """
        Acquire table rows.
        :return: string, table name.
        """
        if self.check_table(table):
            sql = "select count(*) from {table}".format(table=table)
            self._cur.execute(sql)
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
        sql = "select name from sqlite_master where type = 'table'"
        self._cur.execute(sql)
        tables = self._cur.fetchall()
        tables = [item[0] for item in tables]
        if table not in tables:
            return False
        return True

    def close(self):
        self._cur.close()
        self._conn.close()
