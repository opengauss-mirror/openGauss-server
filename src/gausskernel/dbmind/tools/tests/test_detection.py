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
from sklearn.svm import SVR

from dbmind.common.algorithm.forecasting.simple_forecasting import SupervisedModel
from dbmind.common.algorithm.forecasting import ForecastingFactory
from dbmind.common.types import Sequence


linear_seq = Sequence(tuple(range(1, 10)), tuple(range(1, 10)))


def roughly_compare(list1, list2, threshold=1):
    if len(list1) != len(list2):
        return False
    for v1, v2 in zip(list1, list2):
        if abs(v1 - v2) > threshold:
            return False
    return True


def test_linear_regression():
    linear = ForecastingFactory.get_instance('linear')
    linear.fit(linear_seq)
    result = linear.forecast(10)
    assert result.length == 10
    timestamps, values = result.to_2d_array()
    assert tuple(timestamps) == tuple(range(10, 20))
    assert roughly_compare(values, range(10, 20))

    assert ForecastingFactory.get_instance('linear') is linear


def test_supervised_linear_regression():
    linear = SupervisedModel()
    linear.fit(linear_seq)
    result = linear.forecast(10)
    assert result.length == 10
    timestamps, values = result.to_2d_array()
    assert tuple(timestamps) == tuple(range(9, 19))  # different from SimpleLinearFitting.
    assert roughly_compare(values, range(9, 19))


def test_supervised_svr():
    # WARNING: the SVR model with nonlinear kernel does not work.
    svr = SupervisedModel(SVR(kernel='linear', verbose=True, max_iter=100))
    svr.fit(linear_seq)
    result = svr.forecast(10)
    assert result.length == 10
    timestamps, values = result.to_2d_array()
    assert tuple(timestamps) == tuple(range(9, 19))  # different from SimpleLinearFitting.
    assert roughly_compare(values, range(9, 19))

