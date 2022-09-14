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


def distance(str1, str2):
    sql_distance = 0.0
    list1 = str1.split()
    list2 = str2.split()
    sorted_list1 = sorted(list1)
    sorted_list2 = sorted(list2)
    max_len = max(len(sorted_list1), len(sorted_list2))
    min_len = min(len(sorted_list1), len(sorted_list2))
    short_list = sorted_list1 if len(sorted_list1) < len(sorted_list2) else sorted_list2
    long_list = sorted_list1 if len(sorted_list1) > len(sorted_list2) else sorted_list2
    for item in short_list:
        if item in long_list:
            sql_distance += 1.0
    length_similarity = float(min_len / max_len)
    return sql_distance + length_similarity
