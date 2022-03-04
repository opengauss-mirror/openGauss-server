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


def detect(data1, data2, method='bool', threshold=0.01, p_value=0.5):
    if method == 'bool':
        from .sum_base import detect as sum_detect
        from .average_base import detect as avg_detect
        from .ks_base import detect as ks_detect
        vote_res = [sum_detect(data1, data2, threshold=threshold),
                    avg_detect(data1, data2, threshold=threshold),
                    ks_detect(data1, data2, p_value=p_value)]
        return vote_res.count(True) > vote_res.count(False)
    elif method == 'other':
        from .average_base import detect as avg_detect
        return avg_detect(data1, data2, threshold=threshold, method=method)
    else:
        raise
