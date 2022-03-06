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

import ast
import re

import sqlparse
from sqlparse.sql import Identifier, IdentifierList
from sqlparse.sql import Where, Comparison, Operation, Function
from sqlparse.tokens import Keyword, DML
from sqlparse.tokens import Token

SQL_SYMBOLS = (
    '!=', '<=', '>=', '==', '<', '>', '=', ',', '*', ';', '%', '+', ',', ';', '/'
)


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
        elif token.ttype is Token.Operator.Comparison and token.value.upper() == 'NOT IN':
            return 'NotIn'


def analyze_where_clause(dbagent, where, timestamp):
    """
    Analyze RCA of SQL from the where clause.
    :param dbagent: obj, interface for sqlite3.
    :param where: tokens, where clause of sqlparse.
    :return: str, key target of RCA.
    """
    if "OR" in where.value.upper():
        columns = get_columns(where.parent.value)
        indexes = get_indexes(dbagent, where.parent.value, timestamp)
        for column in columns:
            if column not in indexes:
                return 'OR'

    for tokens in where.tokens:
        if isinstance(tokens, Comparison):
            if isinstance(tokens.left, Operation):
                return 'ExprInWhere'
            elif isinstance(tokens.left, Function):
                return 'Function'
            elif isinstance(tokens, Comparison) and "<" in tokens.parent.value or ">" in tokens.parent.value:
                return 'RangeTooLarge'
            else:
                return analyze_unequal_clause(tokens)

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


def wdr_sql_processing(sql):
    standard_sql = unify_sql(sql)
    standard_sql = re.sub(r';', r'', standard_sql)
    standard_sql = re.sub(r'VALUES (\(.*\))', r'VALUES', standard_sql)
    standard_sql = re.sub(r'\$\d+?', r'?', standard_sql)
    return standard_sql


def check_select(parsed_sql):
    if not parsed_sql.is_group:
        return False
    for token in parsed_sql.tokens:
        if token.ttype is DML and token.value.upper() == 'SELECT':
            return True
    return False


def get_table_token_list(parsed_sql, token_list):
    flag = False
    for token in parsed_sql.tokens:
        if not flag:
            if token.ttype is Keyword and token.value.upper() == 'FROM':
                flag = True
        else:
            if check_select(token):
                get_table_token_list(token, token_list)
            elif token.ttype is Keyword:
                return
            else:
                token_list.append(token)


def extract_table_from_select(sql):
    tables = []
    table_token_list = []
    sql_parsed = sqlparse.parse(sql)[0]
    get_table_token_list(sql_parsed, table_token_list)
    for table_token in table_token_list:
        if isinstance(table_token, Identifier):
            tables.append(table_token.get_name())
        elif isinstance(table_token, IdentifierList):
            for identifier in table_token.get_identifiers():
                tables.append(identifier.get_name())
        else:
            if table_token.ttype is Keyword:
                tables.append(table_token.value)
    return tables


def extract_table_from_sql(sql):
    """
    Function: get table name in sql
    has many problems in code, especially in 'delete', 'update', 'insert into' sql
    """
    if not sql.strip():
        return []
    delete_pattern_1 = re.compile(r'FROM\s+([^\s]*)[;\s ]?', re.IGNORECASE)
    delete_pattern_2 = re.compile(r'FROM\s+([^\s]*)\s+WHERE', re.IGNORECASE)
    update_pattern = re.compile(r'UPDATE\s+([^\s]*)\s+SET', re.IGNORECASE)
    insert_pattern = re.compile(r'INSERT\s+INTO\s+([^\s]*)\s+VALUES', re.IGNORECASE)
    if sql.upper().strip().startswith('SELECT'):
        tables = extract_table_from_select(sql)
    elif sql.upper().strip().startswith('DELETE'):
        if 'WHERE' not in sql:
            tables = delete_pattern_1.findall(sql)
        else:
            tables = delete_pattern_2.findall(sql)
    elif sql.upper().strip().startswith('UPDATE'):
        tables = update_pattern.findall(sql)
    elif sql.upper().strip().startswith('INSERT INTO'):
        sql = re.sub(r'\(.*?\)', r' ', sql)
        tables = insert_pattern.findall(sql)
    else:
        tables = []
    return tables


def remove_comment(sql):
    sql = re.sub(r'\n', r' ', sql)
    sql = re.sub(r'/\s*\*[\w\W]*?\*\s*/\s*', r'', sql)
    sql = re.sub(r'^--.*\s?', r'', sql)
    return sql


def unify_sql(sql):
    index = 0
    sql = remove_comment(sql)
    while index < len(sql):
        if sql[index] in SQL_SYMBOLS:
            if sql[index:index + 2] in SQL_SYMBOLS:
                sql = sql[:index].strip() + ' ' + sql[index:index + 2] + ' ' + sql[index + 2:].strip()
                index = index + 3
            else:
                sql = sql[:index].strip() + ' ' + sql[index] + ' ' + sql[index + 1:].strip()
                index = index + 2
        else:
            index = index + 1
    new_sql = list()
    for word in sql.split():
        new_sql.append(word.upper())
    sql = ' '.join(new_sql)
    return sql.strip()


def sql_processing(sql):
    if not sql:
        return ''
    standard_sql = unify_sql(sql)

    if standard_sql.startswith('INSERT'):
        standard_sql = re.sub(r'VALUES (\(.*\))', r'VALUES', standard_sql)
    # remove digital like 12, 12.565
    standard_sql = re.sub(r'[\s]+\d+(\.\d+)?', r' ?', standard_sql)
    # remove '$n' in sql
    standard_sql = re.sub(r'\$\d+', r'?', standard_sql)
    # remove single quotes content
    standard_sql = re.sub(r'\'.*?\'', r'?', standard_sql)
    # remove double quotes content
    standard_sql = re.sub(r'".*?"', r'?', standard_sql)
    # remove '(1' format
    standard_sql = re.sub(r'\(\d+(\.\d+)?', r'(?', standard_sql)
    # remove '`' in sql
    standard_sql = re.sub(r'`', r'', standard_sql)
    # remove ; in sql
    standard_sql = re.sub(r';', r'', standard_sql)

    return standard_sql.strip()


def is_num(input_str):
    if isinstance(input_str, str) and re.match(r'^\d+\.?\d+$', input_str):
        return True
    return False


def str2int(input_str):
    return int(re.match(r'^(\d+)\.?\d+$', input_str).groups()[0])
