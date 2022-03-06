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
from scipy.stats import ks_2samp


def detect(data1, data2, p_value=0.05):
    """
    Calculate whether the data has abrupt changes based on the KS algorithm
    :param data1: input data array
    :param data2: input data array
    :param p_value: confidence value
    :return: bool
    """
    if not data1 or not data2:
        return False
    beta, norm = ks_2samp(data1, data2)
    if norm < p_value:
        return True
    return False
