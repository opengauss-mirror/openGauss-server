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
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Comparison, Parenthesis, Values
from sqlparse.tokens import Keyword, DML, DDL

ALPHA = 0.8


def levenshtein_distance(str1, str2):
    """
    func: caculate levenshten distance between two string
    :param str1: string1
    :param str2: string2
    :return: distance
    """
    m, n = len(str1) + 1, len(str2) + 1
    matrix = [[0] * n for _ in range(m)]
    matrix[0][0] = 0
    for i in range(1, m):
        matrix[i][0] = matrix[i - 1][0] + 1
        for j in range(1, n):
            matrix[0][j] = matrix[0][j - 1] + 1
    for i in range(1, m):
        for j in range(1, n):
            if str1[i - 1] == str2[j - 1]:
                matrix[i][j] = matrix[i - 1][j - 1]
            else:
                matrix[i][j] = min(matrix[i - 1][j - 1], matrix[i - 1][j], matrix[i][j - 1]) + 1

    return matrix[m - 1][n - 1]


def dtw_distance(template1, template2):
    distance_matrix = []
    for i in template1:
        row_dist = []
        for j in template2:
            row_dist.append(0 if i == j else 1)
        distance_matrix.append(row_dist)
    m, n = len(template1), len(template2)
    dtw = [[0] * n for _ in range(m)]

    for i in range(m):
        for j in range(n):
            if not i:
                if not j:
                    dtw[i][j] = 2 * distance_matrix[i][j]
                else:
                    dtw[i][j] = dtw[i][j - 1] + distance_matrix[i][j]
            else:
                if not j:
                    dtw[i][j] = dtw[i - 1][j] + distance_matrix[i][j]
                else:
                    dtw[i][j] = min(dtw[i - 1][j - 1] + 2 * distance_matrix[i][j],
                                    dtw[i - 1][j] + distance_matrix[i][j],
                                    dtw[i][j - 1] + distance_matrix[i][j])
    return dtw[-1][-1]


def list_distance(list1, list2):
    distance = 0.0
    sorted_list1 = sorted(list1)
    sorted_list2 = sorted(list2)
    max_len = max(len(sorted_list1), len(sorted_list2))
    min_len = min(len(sorted_list1), len(sorted_list2))
    short_list = sorted_list1 if len(sorted_list1) < len(sorted_list2) else sorted_list2
    long_list = sorted_list1 if len(sorted_list1) > len(sorted_list2) else sorted_list2
    for item in short_list:
        if item in long_list:
            distance += 1.0
    length_similarity = float(min_len / max_len)
    return distance + length_similarity


def token2value(tokens):
    value_list = []
    for token in tokens:
        if isinstance(token, IdentifierList):
            for item in token.tokens:
                value_list.append(item.value)
        elif isinstance(token, Identifier) or isinstance(token, Comparison) or isinstance(token, Values):
            value_list.append(token.value)
    return value_list


def build_child_nodes(tokens, root={}):
    st_child = False
    sub_tokens = []
    sub_sql_count = 0
    for token in tokens:
        if token.ttype in [Keyword, DDL, DML] or isinstance(token, Where) or isinstance(token, Values) \
                or isinstance(token, Parenthesis):
            if st_child:
                root[child_key] = build_child_nodes(sub_tokens)
            if isinstance(token, Where):
                root['Where'] = []
                for item in token.tokens:
                    if isinstance(item, Comparison):
                        root['Where'].append(item.value)
                st_child = False
            elif isinstance(token, Values):
                root['Values'] = token.value
                st_child = False
            elif isinstance(token, Parenthesis):
                sub_sql_count = sub_sql_count + 1
                child_key = 'sub_sql' + str(sub_sql_count)
                root[child_key] = build_child_nodes(token.tokens)
                st_child = False
            else:
                st_child = True
                sub_tokens = []
                child_key = token.value.lower()
        else:
            sub_tokens.append(token)
    if st_child:
        root[child_key] = build_child_nodes(sub_tokens)
    else:
        root = token2value(tokens)

    return root


def build_tree(sql):
    parsed_sql = sqlparse.parse(sql)[0]
    parse_tree = dict()
    build_child_nodes(parsed_sql.tokens, parse_tree)

    return parse_tree


def compare_two_tree(tree1, tree2, total_score, cur_score):
    if isinstance(tree1, dict) and isinstance(tree2, dict):
        for i in range(len(tree1)):
            key = list(tree1.keys())[i]
            if key in tree2.keys():
                total_score[0] += cur_score
                compare_two_tree(tree1[key], tree2[key], total_score, cur_score * ALPHA)
    if isinstance(tree1, list) and isinstance(tree2, list):
        for value in tree1:
            if value in tree2:
                total_score[0] += cur_score


def parse_tree_distance(parse_tree1, parse_tree2):
    similarity = [0]
    compare_two_tree(parse_tree1, parse_tree2, similarity, 1)
    return -similarity[0]
