# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
from dbmind.app.diagnosis.query.slow_sql.significance_detection import average_base, ks_base, sum_base

big_current_data = [10, 10, 10, 10, 10]
big_history_data = [1, 1, 1, 1, 1]

small_current_data = [1.2, 1.2, 1.2, 1.2, 1.2]
small_history_data = [1, 1, 1, 1, 1]


def test_average_base():
    check_res_1 = average_base.detect(big_current_data, big_history_data)
    check_res_2 = average_base.detect(small_current_data, small_history_data)
    assert check_res_1
    assert not check_res_2


def test_ks_base():
    check_res_1 = ks_base.detect(big_current_data, big_history_data)
    check_res_2 = ks_base.detect(small_current_data, small_history_data)
    assert check_res_1
    assert not check_res_2


def test_sum_base():
    check_res_1 = sum_base.detect(big_current_data, big_history_data)
    check_res_2 = sum_base.detect(small_current_data, small_history_data)
    assert check_res_1
    assert not check_res_2
