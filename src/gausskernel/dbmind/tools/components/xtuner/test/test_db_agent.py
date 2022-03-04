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

import unittest

from tuner.db_agent import DB_Agent

db_agent = DB_Agent(host='',
                    host_user='',
                    host_user_pwd='',
                    db_user='',
                    db_user_pwd='',
                    db_name='',
                    db_port='')  # Padding your information.


class TestDB(unittest.TestCase):
    def test_main_func(self):
        db_agent.restart()
        self.assertTrue(db_agent.is_alive())
        self.assertIsNotNone(db_agent.data_path)
        self.assertEqual(db_agent.get_knob_value('statement_timeout'), 0)
        self.assertGreater(db_agent.metric.get_used_mem(), 0)


if __name__ == '__main__':
    unittest.main()
