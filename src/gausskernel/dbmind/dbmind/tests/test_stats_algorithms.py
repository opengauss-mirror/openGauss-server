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

from dbmind.common.algorithm import stat_utils

test_values1 = list(range(0, 10))
test_values2 = [0] * 10 + [1] * 10 + [0] * 10
test_values3 = [1, 2, 3.5, 0.4, 5]


def test_shift():
    assert stat_utils.np_shift(test_values1).tolist() == [0, 0, 1, 2, 3, 4, 5, 6, 7, 8]
    assert stat_utils.np_shift(test_values1, 9).tolist() == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]


def test_moving_avg():
    assert stat_utils.np_moving_avg(test_values1, window=1).tolist() == test_values1
    assert stat_utils.np_moving_avg([1, 2], 2).tolist() == [1.5, 1.5]
    assert stat_utils.np_moving_avg([1, 2, 3], 2).tolist() == [1.5, 1.5, 2.5]
    assert stat_utils.np_moving_avg([0, 2, 4, 6, 8], 3).tolist() == [2.0, 2.0, 4.0, 6.0, 6.0]


def test_moving_std():
    assert stat_utils.np_moving_std([1, 2, 4, 5, 6], 1).tolist() == [0.0, 0.0, 0.0, 0.0, 0.0]
    assert stat_utils.np_moving_std([1, 2, 3, 4, 5, 6], 2).tolist() == [0.5, 0.5, 0.5, 0.5, 0.5, 0.5]


def test_double_rolling():
    assert stat_utils.np_double_rolling([1, 2, 3], window1=1, window2=2).tolist() == [1.5, 1.5, 0.5]
    assert stat_utils.np_double_rolling([1, 91, 3, 5, 5, 6, 1], window1=10, window2=5,
                                        diff_mode='abs_diff').tolist() == [70, 70, 70, 19, 1, 1, 1]


def test_fill_missing_value():
    s0 = stat_utils.Sequence([1, 2, 4, 5, 7, 8, 12], [1, 2, 4, 5, 7, 8, 12])
    r = stat_utils.tidy_up_sequence(s0)
    assert r.timestamps == (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
    assert str(r.values) == '(1, 2, nan, 4, 5, nan, 7, 8, nan, nan, nan, 12)'

    s1 = stat_utils.Sequence([1, 2, 4, 5, 6, 8, 12], [1, 2, 4, 5, 6, 8, 12], step=3)
    r = stat_utils.tidy_up_sequence(s1)
    assert r.timestamps == (1, 4, 7, 10)
    # Notice: the values of r are ambiguous, which is because filling strategy may be changeful and
    # the filling value captures from nearby value.
    assert r.values

    r = stat_utils.sequence_interpolate(s1)
    assert r.timestamps == (1, 4, 7, 10)
