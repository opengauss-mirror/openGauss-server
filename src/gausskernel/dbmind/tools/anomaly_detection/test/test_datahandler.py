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
import sys
import unittest

sys.path.append('../')

from detector.monitor import data_handler


class TestDataHandler(unittest.TestCase):
    def test_select_timeseries_by_timestamp(self, table='io_read', database_path='./data/metric.db'):
        with data_handler.DataHandler(database_path) as db:
            timeseries_by_timestamp = db.select_timeseries_by_timestamp(table=table, period='100S')
            print(timeseries_by_timestamp)
            self.assertGreater(len(timeseries_by_timestamp), 0)

    def test_select_timeseries_by_number(self, table='io_write', database_path='./data/metric.db'):
        with data_handler.DataHandler(database_path) as db:
            timeseries_by_number = db.select_timeseries_by_number(table=table, number=100)
            self.assertGreater(len(timeseries_by_number), 0)

    def test_get_table_rows(self, table='io_read', database_path='./data/metric.db'):
        with data_handler.DataHandler(database_path) as db:
            rows = db.get_table_rows(table=table)
            self.assertGreater(rows, 0)


if __name__ == '__main__':
    unittest.main()
