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


def detect(data1, data2, threshold=0.5, method='bool'):
    """
    Calculate whether the data has abrupt changes based on the sum value
    :param data1: input data array
    :param data2: input data array
    :param threshold: Mutation rate
    :param method: The way to calculate the mutation
    :return: bool
    """
    alpha = 1e-10
    if not isinstance(data1, list) or not isinstance(data2, list):
        raise TypeError("The format of the input data is wrong.")
    sum1 = sum(data1)
    sum2 = sum(data2)
    if method == 'bool':
        return sum1 * (1 - threshold) > sum2
    elif method == 'other':
        if sum1 < sum2:
            return 0
        else:
            return (sum1 - sum2) / (sum1 + alpha)
    else:
        raise ValueError('Not supported method %s.' % method)
