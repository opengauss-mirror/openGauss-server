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
from datetime import timedelta

import utils


class TestUtilFunctions(unittest.TestCase):
    def test_transform_time_string(self):
        transform_result_1 = utils.transform_time_string('10S', mode='to_second')
        transform_result_2 = utils.transform_time_string('2M', mode='to_second')
        transform_result_3 = utils.transform_time_string('3H', mode='to_second')
        transform_result_4 = utils.transform_time_string('1D', mode='to_second')
        transform_result_5 = utils.transform_time_string('2W', mode='to_second')

        transform_result_6 = utils.transform_time_string('10S', mode='timedelta')
        transform_result_7 = utils.transform_time_string('2M', mode='timedelta')
        transform_result_8 = utils.transform_time_string('3H', mode='timedelta')
        transform_result_9 = utils.transform_time_string('1D', mode='timedelta')
        transform_result_10 = utils.transform_time_string('2W', mode='timedelta')

        self.assertEqual(transform_result_1, 10)
        self.assertEqual(transform_result_2, 2 * 60)
        self.assertEqual(transform_result_3, 3 * 60 * 60)
        self.assertEqual(transform_result_4, 1 * 24 * 60 * 60)
        self.assertEqual(transform_result_5, 2 * 7 * 24 * 60 * 60)

        self.assertEqual(transform_result_6, timedelta(seconds=10))
        self.assertEqual(transform_result_7, timedelta(minutes=2))
        self.assertEqual(transform_result_8, timedelta(hours=3))
        self.assertEqual(transform_result_9, timedelta(days=1))
        self.assertEqual(transform_result_10, timedelta(weeks=2))

    def test_unify_byte_info(self):
        unify_result_1 = utils.unify_byte_unit('10K')
        unify_result_2 = utils.unify_byte_unit('10M')
        unify_result_3 = utils.unify_byte_unit('10G')
        unify_result_4 = utils.unify_byte_unit('10T')
        unify_result_5 = utils.unify_byte_unit('10P')

        self.assertEqual(unify_result_1, 0.009765625)
        self.assertEqual(unify_result_2, 10)
        self.assertEqual(unify_result_3, 10 * 1024)
        self.assertEqual(unify_result_4, 10 * 1024 * 1024)
        self.assertEqual(unify_result_5, 10 * 1024 * 1024 * 1024)


if __name__ == '__main__':
    unittest.main()
