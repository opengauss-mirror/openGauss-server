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
import re

import sqlparse
from sqlparse.sql import Identifier, IdentifierList
from sqlparse.tokens import Keyword, DML


DDL_WORDS = ('CREATE', 'ALTER', 'DROP', 'TRUNCATE', 'COMMIT', 'RENAME')
DML_WORDS = ('SELECT', 'INSERT INTO', 'UPDATE', 'DELETE', 'MERGE', 'CALL',
             'EXPLAIN PLAN', 'LOCK TABLE', 'COMMIT', 'REPLACE', 'START', 'ROLLBACK')
KEYWORDS = ('GRANT', 'REVOKE', 'DENY', 'ABORT', 'ADD', 'AGGREGATE', 'ANALYSE', 'AVG', 'ALLOCATE', 'CALL', 'DESC',
            'ASC', 'EQUALS', 'EXCEPT', 'EXISTS', 'EXPLAIN', 'FALSE', 'TRUE', 'DATE', 'SET', 'GROUP BY', 'WHERE',
            'AND', 'OR', 'BETWEEN', 'LIKE', 'JOIN', 'LEFT', 'RIGHT', 'CROSS', 'INNER', 'LIMIT', 'ORDER BY', 'AS',
            'FROM', 'REVOKE', 'DENY', 'VALUES')
FUNC = ('FLOOR', 'SUM', 'NOW', 'UUID', 'COUNT')
SQL_SIG = ('&', '&&')


def _unify_sql(sql):
    """
    function: unify sql format
    """
    index = 0
    sql = re.sub(r'\n', r' ', sql)
    sql = re.sub(r'/\s*\*[\w\W]*?\*\s*/\s*', r'', sql)
    sql = re.sub(r'^--.*\s?', r'', sql)
   
    sql = re.sub(r'([!><=]=)', r' \1 ', sql)
    sql = re.sub(r'([^!><=])([=<>])', r'\1 \2 ', sql)
    sql = re.sub(r'([,()*%/+])', r' \1 ', sql)
    sql = re.sub(r'\s+', r' ', sql)
    sql = sql.upper()
    return sql.strip()


def split_sql(sqls):
    if not sqls:
        return []
    sqls = sqls.split(';')
    result = list(map(lambda item: _unify_sql(item), sqls))
    return result


def templatize_sql(sql):
    """
    SQL desensitization
    """
    if not sql:
        return ''
    standard_sql = _unify_sql(sql)

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
    # remove '`' in sql
    standard_sql = re.sub(r'`', r'', standard_sql)
    # remove ; in sql
    standard_sql = re.sub(r';', r'', standard_sql)

    return standard_sql.strip()


def _is_select_clause(parsed_sql):
    if not parsed_sql.is_group:
        return False
    for token in parsed_sql.tokens:
        if token.ttype is DML and token.value.upper() == 'SELECT':
            return True
    return False


def _get_table_token_list(parsed_sql, token_list):
    flag = False
    for token in parsed_sql.tokens:
        if not flag:
            if token.ttype is Keyword and token.value.upper() == 'FROM':
                flag = True
        else:
            if _is_select_clause(token):
                _get_table_token_list(token, token_list)
            elif token.ttype is Keyword:
                return
            else:
                token_list.append(token)


def _extract_table(sql):
    tables = []
    table_token_list = []
    sql_parsed = sqlparse.parse(sql)[0]
    _get_table_token_list(sql_parsed, table_token_list)
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


def _get_sql_table_name(sql):
    """
    function: get table name in sql
    has many problems in code, especially in 'delete', 'update', 'insert into' sql
    """
    if sql.startswith('SELECT'):
        tables = _extract_table(sql)
    elif sql.startswith('DELETE'):
        if 'WHERE' not in sql:
            tables = re.findall(r'FROM\s+([^\s]*)[;\s ]?', sql)
        else:
            tables = re.findall(r'FROM\s+([^\s]*)\s+WHERE', sql)
    elif sql.startswith('UPDATE'):
        tables = re.findall(r'UPDATE\s+([^\s]*)\s+SET', sql)
    elif sql.startswith('INSERT INTO'):
        sql = re.sub(r'\(.*?\)', r' ', sql)
        tables = re.findall(r'INSERT\s+INTO\s+([^\s]*)\s+VALUES', sql)
    else:
        tables = []
    return tables


def _get_table_column_name(sql):
    remove_sign = (r'=', r'<', r'>')
    tables = _get_sql_table_name(sql)
    sql = re.sub(r'[?]', r'', sql)
    sql = re.sub(r'[()]', r'', sql)
    sql = re.sub(r'`', r'', sql)
    sql = re.sub(r',', r'', sql)
    for table in tables:
        sql = re.sub(r'(\s+{table}\.|\s+{table}\s+|\s+{table})'.format(table=table), r' ', sql)
    for word in DML_WORDS:
        sql = re.sub(r'(\s+|^)' + word + r'\s+', r' ', sql)
    for word in DDL_WORDS:
        sql = re.sub(r'(\s+|^)' + word + r'\s+', r' ', sql)
    for word in KEYWORDS:
        sql = re.sub(r'(\s+|^)' + word + r'\s+', r' ', sql)
    for word in SQL_SIG:
        sql = re.sub(r'(\s+|^)' + word + r'\s+', r' ', sql)
    for word in FUNC:
        sql = re.sub(word, r' ', sql)

    for sign in remove_sign:
        sql = re.sub(sign, r'', sql)

    columns = re.split(r'\s+', sql)
    columns = list(map(lambda x: x.split('.')[1] if '.' in x else x, columns))
    columns = list(filter(lambda x: x, columns))
    return tables, columns


def get_sql_template(sql):
    """
    function: derive skeleton of sql
    """
    fine_template = templatize_sql(sql)
    rough_template = fine_template
    tables, columns = _get_table_column_name(fine_template)
    if rough_template.startswith('INSERT INTO'):
        table = tables[0]
        rough_template = re.sub(r'INTO ' + table + r' \(.*?\)', r'INTO tab ()', rough_template)

    for table in tables:
        rough_template = re.sub(r'(\s+{table}\.|\s+{table}\s+|\s+{table})'.format(table=table), r' tab ',
                                rough_template)

    for column in columns:
        if column in ['*', '.', '+', '?']:
            continue
        rough_template = re.sub(r'\s+' + column + r'\s+', r' col ', rough_template)
    return fine_template, rough_template


class LoadData:
    def __init__(self, csv_file):
        self.csv_file = csv_file

    def load_predict_file(self):
        for line in self.csv_file:
            line = line.strip()
            if line:
                yield line

    def load_train_file(self):
        for line in self.csv_file:
            line = line.strip()
            if not line or ',' not in line:
                continue
            last_delimater_pos = line.rindex(',')
            if re.search(r'\d+(\.\d+)?$', line[last_delimater_pos + 1:]) is None:
                continue
            sql = line[:last_delimater_pos]
            duration_time = float(line[last_delimater_pos + 1:])
            yield sql, duration_time

    def __getattr__(self, name):
        if name not in ('train_data', 'predict_data'):
            raise AttributeError('{} has no attribute {}.'.format(LoadData.__name__, name))
        if name == 'train_data':
            return self.load_train_file()
        else:
            return self.load_predict_file()
