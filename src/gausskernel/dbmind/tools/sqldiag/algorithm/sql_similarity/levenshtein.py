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


def distance(str1, str2):
    """
    func: calculate levenshtein distance between two strings.
    :param str1: string1
    :param str2: string2
    :return: distance
    """
    len_str1 = len(str1) + 1
    len_str2 = len(str2) + 1
    
    mat = [[0]*len_str2 for i in range(len_str1)]
    mat[0][0] = 0
    for i in range(1,len_str1):
        mat[i][0] = mat[i-1][0] + 1
    for j in range(1,len_str2):
        mat[0][j] = mat[0][j-1]+1
    for i in range(1,len_str1):
        for j in range(1,len_str2):
            if str1[i-1] == str2[j-1]:
                mat[i][j] = mat[i-1][j-1]
            else:
                mat[i][j] = min(mat[i-1][j-1],mat[i-1][j],mat[i][j-1])+1
    
    return 1 / mat[len_str1-1][j-1]
