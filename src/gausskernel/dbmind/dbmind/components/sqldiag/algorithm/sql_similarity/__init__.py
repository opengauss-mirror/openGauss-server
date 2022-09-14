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


def calc_sql_distance(algorithm):
    if algorithm == 'list':
        from .list_distance import distance
    elif algorithm == 'levenshtein':
        from .levenshtein import distance
    elif algorithm == 'parse_tree':
        from .parse_tree import distance
    elif algorithm == 'cosine_distance':
        from .cosine_distance import distance
    else:
        raise NotImplementedError("do not support '{}'".format(algorithm))
    return distance
