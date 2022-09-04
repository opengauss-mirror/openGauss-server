# Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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

from functools import reduce

import numpy as np

from ...types import Sequence

np.seterr(divide="ignore", invalid="ignore")


def merge_with_and_operator(s_list):
    result_values = reduce(
        lambda x, y: np.logical_and(x, y.values),
        s_list[1:], np.array(s_list[0].values)
    )
    return Sequence(timestamps=s_list[0].timestamps, values=result_values)


def merge_with_or_operator(s_list):
    result_values = reduce(
        lambda x, y: np.logical_or(x, y.values),
        s_list[1:], np.array(s_list[0].values)
    )
    return Sequence(timestamps=s_list[0].timestamps, values=result_values)
