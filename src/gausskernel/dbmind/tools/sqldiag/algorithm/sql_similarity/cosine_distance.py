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
import math
from collections import Counter


def distance(str1, str2):
    c_1 = Counter(str1)
    c_2 = Counter(str2)
    c_union = set(c_1).union(c_2)
    dot_product = sum(c_1.get(item, 0) * c_2.get(item, 0) for item in c_union)
    mag_c1 = math.sqrt(sum(c_1.get(item, 0)**2 for item in c_union))
    mag_c2 = math.sqrt(sum(c_2.get(item, 0)**2 for item in c_union))
    return dot_product / (mag_c1 * mag_c2)
