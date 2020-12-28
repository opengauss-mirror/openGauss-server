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

from .similarity import list_distance

# split flag in SQL
split_flag = ('!=', '<=', '>=', '==', '<', '>', '=', ',', '(', ')', '*', ';', '%', '+', ',', ';')

DDL_WORDS = ('CREATE', 'ALTER', 'DROP', 'TRUNCATE', 'COMMIT', 'RENAME')
DML_WORDS = ('SELECT', 'INSERT INTO', 'UPDATE', 'DELETE', 'MERGE', 'CALL',
             'EXPLAIN PLAN', 'LOCK TABLE', 'COMMIT', 'REPLACE', 'START', 'ROLLBACK')
KEYWORDS = ('GRANT', 'REVOKE', 'DENY', 'ABORT', 'ADD', 'AGGREGATE', 'ANALYSE', 'AVG', 'ALLOCATE', 'CALL', 'DESC',
            'ASC', 'EQUALS', 'EXCEPT', 'EXISTS', 'EXPLAIN', 'FALSE', 'TRUE', 'DATE', 'SET', 'GROUP BY', 'WHERE',
            'AND', 'OR', 'BETWEEN', 'LIKE', 'JOIN', 'LEFT', 'RIGHT', 'CROSS', 'INNER', 'LIMIT', 'ORDER BY', 'AS',
            'FROM', 'REVOKE', 'DENY', 'VALUES')
FUNC = ('FLOOR', 'SUM', 'NOW', 'UUID', 'COUNT')
SQL_SIG = ('&', '&&')

# filter like (insert into aa (c1, c2) values (v1, v2) => insert into aa * values *)
BRACKET_FILTER = r'\(.*?\)'

# filter (123, 123.123)
PURE_DIGIT_FILTER = r'[\s]+\d+(\.\d+)?'

# filter ('123', '123.123')
SINGLE_QUOTE_DIGIT_FILTER = r'\'\d+(\.\d+)?\''

# filter ("123", "123.123")
DOUBLE_QUOTE_DIGIT_FILTER = r'"\d+(\.\d+)?"'

# filter ('123', 123, '123,123', 123.123) not filter(table1, column1, table_2, column_2)
DIGIT_FILTER = r'([^a-zA-Z])_?\d+(\.\d+)?'

# filter date in sql ('1999-09-09', '1999/09/09', "1999-09-09 20:10:10", '1999/09/09 20:10:10.12345')
PURE_TIME_FILTER = r'[0-9]{4}[-/][0-9]{1,2}[-/][0-9]{1,2}\s*([0-9]{1,2}[:][0-9]{1,2}[:][0-9]{1,2})?(\.\d+)?'
SINGLE_QUOTE_TIME_FILTER = r'\'[0-9]{4}[-/][0-9]{1,2}[-/][0-9]{1,2}\s*([0-9]{1,2}[:][0-9]{1,2}[:][0-9]{1,' \
                           r'2})?(\.\d+)?\' '
DOUBLE_QUOTE_TIME_FILTER = r'"[0-9]{4}[-/][0-9]{1,2}[-/][0-9]{1,2}\s*([0-9]{1,2}[:][0-9]{1,2}[:][0-9]{1,2})?(\.\d+)?"'

# filter like "where id='abcd" => "where id=#"
SINGLE_QUOTE_FILTER = r'\'.*?\''

# filter like 'where id="abcd" => 'where id=#'
DOUBLE_QUOTE_FILTER = r'".*?"'

# filter annotation like "/* XXX */"
ANNOTATION_FILTER_1 = r'/\s*\*[\w\W]*?\*\s*/\s*'
ANNOTATION_FILTER_2 = r'^--.*\s?'

# filter NULL character  '\n \t' in sql
NULL_CHARACTER_FILTER = r'\s+'

# remove data in insert sql
VALUE_BRACKET_FILETER = r'VALUES (\(.*\))'

# remove equal data in sql
WHERE_EQUAL_FILTER = r'= .*?\s'

LESS_EQUAL_FILTER = r'(<= .*? |<= .*$)'
GREATER_EQUAL_FILTER = r'(>= .*? |<= .*$)'
LESS_FILTER = r'(< .*? |< .*$)'
GREATER_FILTER = r'(> .*? |> .*$)'
EQUALS_FILTER = r'(= .*? |= .*$)'
LIMIT_DIGIT = r'LIMIT \d+'


def unify_sql(sql):
    """
    function: unify sql format
    """
    index = 0
    sql = re.sub(r'\n', r' ', sql)
    sql = re.sub(ANNOTATION_FILTER_1, r'', sql)
    sql = re.sub(ANNOTATION_FILTER_2, r'', sql)
    while index < len(sql):
        if sql[index] in split_flag:
            if sql[index:index + 2] in split_flag:
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


def sql_filter(sql):
    """
    function: replace the message which is not important in sql
    """
    sql = unify_sql(sql)

    sql = re.sub(r';', r'', sql)

    # ? represent date or time
    sql = re.sub(PURE_TIME_FILTER, r'?', sql)
    sql = re.sub(SINGLE_QUOTE_TIME_FILTER, r'?', sql)
    sql = re.sub(DOUBLE_QUOTE_TIME_FILTER, r'?', sql)

    # $ represent insert value
    if sql.startswith('INSERT'):
        sql = re.sub(VALUE_BRACKET_FILETER, r'VALUES ()', sql)

    # $$ represent select value
    if sql.startswith('SELECT') and ' = ' in sql:
        sql = re.sub(WHERE_EQUAL_FILTER, r'= $$ ', sql)

    # $$$ represent delete value
    if sql.startswith('DELETE') and ' = ' in sql:
        sql = re.sub(WHERE_EQUAL_FILTER, r'= $$$ ', sql)

    # & represent logical signal
    sql = re.sub(LESS_EQUAL_FILTER, r'<= & ', sql)
    sql = re.sub(LESS_FILTER, r'< & ', sql)
    sql = re.sub(GREATER_EQUAL_FILTER, r'>= & ', sql)
    sql = re.sub(GREATER_FILTER, r'> & ', sql)
    sql = re.sub(LIMIT_DIGIT, r'LIMIT &', sql)
    sql = re.sub(EQUALS_FILTER, r'= & ', sql)
    sql = re.sub(PURE_DIGIT_FILTER, r' &', sql)
    sql = re.sub(r'`', r'', sql)

    # && represent quote str
    sql = re.sub(SINGLE_QUOTE_FILTER, r'?', sql)
    sql = re.sub(DOUBLE_QUOTE_FILTER, r'?', sql)

    return sql


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


def extract_table(sql):
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


def get_sql_table_name(sql):
    """
    function: get table name in sql
    has many problems in code, especially in 'delete', 'update', 'insert into' sql
    """
    if sql.startswith('SELECT'):
        tables = extract_table(sql)
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


def get_table_column_name(sql):
    remove_sign = (r'=', r'<', r'>')
    tables = get_sql_table_name(sql)
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
    filtered_sql = sql_filter(sql)
    sql_template = filtered_sql
    tables, columns = get_table_column_name(sql_template)
    if filtered_sql.startswith('INSERT INTO'):
        table = tables[0]
        sql_template = re.sub(r'INTO ' + table + r' \(.*?\)', r'INTO tab ()', sql_template)

    for table in tables:
        sql_template = re.sub(r'(\s+{table}\.|\s+{table}\s+|\s+{table})'.format(table=table), r' tab ', sql_template)

    for column in columns:
        if column in ['*', '.', '+', '?']:
            continue
        sql_template = re.sub(r'\s+' + column + r'\s+', r' col ', sql_template)
    return filtered_sql, sql_template


def sql_similarity(sql1, sql2):
    """
    calculate similarity of sql
    """
    if sql1.split()[0] != sql2.split()[0]:
        return 0.0
    similarity_of_sql = list_distance(sql1.split(), sql2.split())
    return similarity_of_sql
