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

alpha = 1e-10


def detect(data1, data2, threshold=0.5, method='bool'):
    """
    Calculate whether the data has abrupt changes based on the average value
    :param data1: input data array
    :param data2: input data array
    :param threshold: Mutation rate
    :param method: The way to calculate the mutation
    :return: bool
    """
    if not isinstance(data1, list) or not isinstance(data2, list):
        raise TypeError("The format of the input data is wrong.")
    avg1 = sum(data1) / len(data1) if data1 else 0
    avg2 = sum(data2) / len(data2) if data2 else 0

    if method == 'bool':
        return avg1 * (1 - threshold) > avg2
    elif method == 'other':
        if avg1 < avg2:
            return 0
        else:
            return (avg1 - avg2) / (avg1 + alpha)
    else:
        raise ValueError('Not supported method %s.' % method)

