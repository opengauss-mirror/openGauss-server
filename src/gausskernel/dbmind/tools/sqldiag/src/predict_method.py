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
import numpy as np
from sklearn import linear_model

sample_for_predict = 1000  # threshold for using predict method


def smoothing(data):
    """
    function: smoothing the input data series
    :param data: original dataset
    :return: smoothed result
    """
    if len(data) < sample_for_predict:
        return np.mean(data)
    else:
        # use linear regression for data series
        X = np.linspace(1, sample_for_predict, sample_for_predict).reshape(-1, 1)
        y = list()
        split = len(data) / sample_for_predict
        for i in range(sample_for_predict):
            y += [data[round(i * split)]]
        y = np.array(y)
        model = linear_model.LinearRegression()
        model.fit(X, y)
        return model.predict([[sample_for_predict + 1]])[0]
