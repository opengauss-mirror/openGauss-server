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
from dbmind.common.algorithm.statistics import *

test_values1 = list(range(0, 10))
# [0, 0, ..., 1, 1, ..., ..., 0, 0]
test_values2 = [0] * 10 + [1] * 10 + [0] * 10
test_values3 = [1, 2, 3.5, 0.4, 5]


def test_quantile():
    assert np_quantile(test_values1, 1) == 0.09
    assert np_quantile(test_values2, 2) == 0
    assert np_quantile(test_values3, 1) > 0.42


def test_shift():
    assert np_shift(test_values1).tolist() == [0, 0, 1, 2, 3, 4, 5, 6, 7, 8]
    assert np_shift(test_values1, 9).tolist() == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]


def test_moving_avg():
    assert np_moving_avg(test_values1, window=1).tolist() == test_values1
    assert np_moving_avg([1, 2], 2).tolist() == [1.5, 1.5]
    assert np_moving_avg([1, 2, 3], 2).tolist() == [0.5, 1.5, 1.5]


def test_moving_std():
    assert np_moving_std([1, 2], 1).tolist() == [0, 0]
    assert np_moving_std([1, 2, 3], 2).tolist() == [0.5, 0.5, 0.5]


def test_double_rolling():
    assert np_double_rolling([1, 2, 3], window1=1, window2=2).tolist() == [1.5, 1.5, 1.5]
