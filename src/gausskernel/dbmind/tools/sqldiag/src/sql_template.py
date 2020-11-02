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


# split flag in SQL
split_flag = ['!=', '<=', '>=', '==', '<', '>', '=', ',', '(', ')', '*', ';', '%', '+', ',']

# filter like (insert into aa (c1, c2) values (v1, v2) => insert into aa * values *)
BRACKET_FILTER = r'\(.*?\)'

# filter (123, 123.123)
PURE_DIGIT_FILTER = r'[\s]+\d+(\.\d+)?'

# filter ('123', '123.123')
QUOTE_DIGIT_FILTER = r'\'\d+(\.\d+)?\''

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


def replace_module(sql):
    """
    function: replace the message which is not important in sql
    """
    sql = unify_sql(sql)
    sql = sql.strip()
    sql = re.sub(PURE_TIME_FILTER, r'?', sql)
    sql = re.sub(SINGLE_QUOTE_TIME_FILTER, r'?', sql)
    sql = re.sub(DOUBLE_QUOTE_TIME_FILTER, r'?', sql)
    sql = re.sub(QUOTE_DIGIT_FILTER, r'?', sql)
    sql = re.sub(DOUBLE_QUOTE_DIGIT_FILTER, r'?', sql)
    sql = re.sub(PURE_DIGIT_FILTER, r' ?', sql)
    sql = re.sub(SINGLE_QUOTE_FILTER, r'?', sql)
    sql = re.sub(DOUBLE_QUOTE_FILTER, r'?', sql)
    sql = re.sub(ANNOTATION_FILTER_1, r'', sql)
    sql = re.sub(ANNOTATION_FILTER_2, r'', sql)
    sql = re.sub(NULL_CHARACTER_FILTER, r' ', sql)
    return sql


def unify_sql(sql):
    index = 0
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
    return ' '.join(new_sql)


def derive_template_of_sql(sql):
    """
    function: derive skeleton of sql
    """
    sql = replace_module(sql)
    return sql