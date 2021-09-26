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
import ast
import re

import sqlparse
from sqlparse.sql import Where, Comparison, Operation, Function, Identifier
from sqlparse.tokens import DML, Token


def is_subquery(parse_tree):
    if not parse_tree.is_group:
        return False
    for item in parse_tree.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False


def analyze_column(column, where_clause):
    for tokens in where_clause.tokens:
        if isinstance(tokens, Comparison) and isinstance(tokens.left, Identifier):
            column.add(tokens.left.value)


def get_columns(sql):
    column = set()
    parsed_tree = sqlparse.parse(sql)[0]
    for item in parsed_tree:
        if isinstance(item, Where):
            analyze_column(column, item)
    return list(column)


def get_indexes(dbagent, sql, timestamp):
    """
    Get indexes of SQL from dataset.
    :param dbagent: obj, interface for sqlite3.
    :param sql: str, query.
    :return: list, the set of indexes.
    """
    indexes = []
    indexes_dict = dbagent.fetch_all_result("SELECT indexes from wdr where timestamp ==\"{timestamp}\""
                                            " and query == \"{query}\"".format(timestamp=timestamp,
                                                                               query=sql))
    if len(indexes_dict):
        try:
            indexes_dict = ast.literal_eval(indexes_dict[0][0])
            indexes_def_list = list(list(indexes_dict.values())[0].values())
            for sql_index in indexes_def_list:
                value_in_bracket = re.compile(r'[(](.*?)[)]', re.S)
                indexes.append(re.findall(value_in_bracket, sql_index)[0].split(',')[0])
        except Exception:
            return indexes
    return indexes


def analyze_unequal_clause(tokens):
    for token in tokens:
        if token.ttype is Token.Operator.Comparison and token.value.upper() == 'LIKE':
            return 'FuzzyQuery'
        elif token.ttype is Token.Operator.Comparison and token.value.upper() == '!=':
            return 'UnEqual'


def analyze_where_clause(dbagent, where, timestamp):
    """
    Analyze RCA of SQL from the where clause.
    :param dbagent: obj, interface for sqlite3.
    :param where: tokens, where clause of sqlparse.
    :return: str, key value of RCA.
    """
    if "OR" in where.value.upper():
        columns = get_columns(where.parent.value)
        indexes = get_indexes(dbagent, where.parent.value, timestamp)
        for column in columns:
            if column not in indexes:
                return 'OR'

    res = None
    for tokens in where.tokens:
        if isinstance(tokens, Comparison):
            if isinstance(tokens.left, Operation):
                res = 'ExprInWhere'
                break
            elif isinstance(tokens.left, Function):
                res = 'Function'
                break
            elif isinstance(tokens, Comparison) and "<" in tokens.parent.value or ">" in tokens.parent.value:
                res = 'RangeTooLarge'
                break
            else:
                res = analyze_unequal_clause(tokens)
                break
    if res:
        return res
    if 'NOT IN' in where.value.upper():
        return "NotIn"
    if "is not null".upper() in where.value.upper():
        return 'IsNotNULL'


def sql_parse(dbagent, sql, timestamp):
    sql = re.sub(r'\n|\t', r' ', sql)
    sql = re.sub(r'[ ]{2,}', r' ', sql)
    parse_tree = sqlparse.parse(sql)[0]

    if "select count( * ) from".upper() in parse_tree.value.upper() or \
        "select * from".upper() in parse_tree.value.upper() or \
        "select count(*) from".upper() in parse_tree.value.upper() or \
        "select count( *) from".upper() in parse_tree.value.upper() or \
        "select count(* ) from".upper() in parse_tree.value.upper():
        return "FullScan"

    if "update".upper() in parse_tree.value.upper() and "set".upper() in parse_tree.value.upper():
        return 'Update'

    for item in parse_tree:
        if isinstance(item, Where):
            return analyze_where_clause(dbagent, item, timestamp)
