import os
import sqlite3
import time

from utils import transform_time_string
from .monitor_logger import logger


class DataHandler:
    '''
    process sqlite3 data, provide data to forecastor
    '''

    def __init__(self, table, dbpath):
        self._table = table
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
        self._conn = sqlite3.connect(self._dbpath)
        self._cur = self._conn.cursor()

    def select_timeseries_by_timestamp(self, period):
        timeseries = []
        times = 0
        times_limit = 5
        while times < times_limit:
            try:
                get_last_timestamp_sql = "select timestamp from {table} order by timestamp desc limit 1"
                self._cur.execute(get_last_timestamp_sql.format(table=self._table))
                last_timestamp = self._cur.fetchall()[0][0]
                time_interval = transform_time_string(period, mode='to_second')
                select_timestamp = last_timestamp - time_interval
                get_timeseries_sql = "select timestamp, value from {table} where timestamp >= '{select_timestamp}'"
                self._cur.execute(get_timeseries_sql.format(table=self._table, select_timestamp=select_timestamp))
                timeseries = self._cur.fetchall()
                if not timeseries:
                    logger.warn("get no timeseries from {table}', retry...".format(table=self._table))
                else:
                    return timeseries
            except Exception as e:
                logger.exception('exception occur when get timeseries: {error}, retry...'.format(error=str(e)), exc_info=True)
                times += 1
                time.sleep(0.11)
        return timeseries

    def select_timeseries_by_length(self, length):
        timeseries = []
        times = 0
        times_limit = 5
        while times < times_limit:
            try:
                sql = "select * from (select * from {table} order by timestamp desc limit {length}) order by timestamp"
                self._cur.execute(sql.format(table=self._table, length=length))
                timeseries = self._cur.fetchall()
                if not timeseries:
                    logger.warn("get no timeseries from {table}', retry...".format(table=self._table))
                else:
                    return timeseries

            except Exception as e:
                logger.exception('exception occur when get timeseries: {error}'.format(error=str(e)), exc_info=True)
                times += 1
                time.sleep(0.11)
            return timeseries

    def get_timeseries(self, period):
        if isinstance(period, int):
            timeseries = self.select_timeseries_by_length(length=period)
        else:
            timeseries = self.select_timeseries_by_timestamp(period=period)
        return timeseries

    def check_table(self, table):
        '''
        check whether table exist in data
        '''
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

