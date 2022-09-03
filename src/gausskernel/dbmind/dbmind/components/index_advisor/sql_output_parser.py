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

import re
from typing import List

import sqlparse
from sqlparse.tokens import Punctuation

try:
    from utils import match_table_name, IndexItemFactory, ExistingIndex, AdvisedIndex
except ImportError:
    from .utils import match_table_name, IndexItemFactory, ExistingIndex, AdvisedIndex


def __get_columns_from_indexdef(indexdef):
    for content in sqlparse.parse(indexdef)[0].flatten():
        if content.ttype is Punctuation:
            return content.parent.value.strip()[1:-1]


def parse_existing_indexes_results(results, schema) -> List[ExistingIndex]:
    indexes = list()
    indexdef_list = []
    table = index = pkey = None
    for cur_tuple in results:
        if len(cur_tuple) == 1:
            continue
        else:
            temptable, tempindex, indexdef, temppkey = cur_tuple
            if temptable and tempindex:
                table, index, pkey = temptable, tempindex, temppkey
            if indexdef.endswith('+'):
                if len(indexdef_list) >= 1:
                    if indexdef.startswith('SUBPARTITION'):
                        indexdef_list.append(' ' * 8 + indexdef.strip(' +'))
                    else:
                        indexdef_list.append(' ' * 4 + indexdef.strip(' +'))
                else:
                    indexdef_list.append(indexdef.strip(' +'))
                continue
            elif indexdef_list and indexdef.startswith(')'):
                indexdef_list.append(indexdef.strip().strip('+').strip())
                indexdef = '\n'.join(indexdef_list)
                indexdef_list = []
            cur_columns = __get_columns_from_indexdef(indexdef)
            cur_index = ExistingIndex(
                schema, table, index, cur_columns, indexdef)
            if pkey:
                cur_index.set_is_primary_key(True)
            indexes.append(cur_index)
    return indexes


def parse_table_sql_results(table_sql_results):
    tables = []
    for cur_tuple in table_sql_results:
        text = cur_tuple[0]
        if 'tablename' in text or re.match(r'-+', text) or re.match(r'\(\d+ rows?\)', text) \
                or text.strip().startswith('SELECT '):
            continue
        tables.append(text.strip())
    return tables


def parse_explain_plan(results, query_num):
    i = 0
    found_plan = False
    hypo_index = False
    costs = []
    indexes_names = set()
    hypo_index_ids = []
    for cur_tuple in results:
        text = cur_tuple[0]
        if 'QUERY PLAN' in text or text == 'EXPLAIN':
            found_plan = True
        if 'ERROR' in text:
            if i >= query_num:
                raise ValueError("The size of queries is not correct!")
            costs.append(0)
            i += 1
        if 'hypopg_create_index' in text or text.startswith('SELECT'):
            hypo_index = True
        if found_plan and '(cost=' in text:
            if i >= query_num:
                raise ValueError("The size of queries is not correct!")
            query_cost = parse_plan_cost(text)
            costs.append(query_cost)
            found_plan = False
            i += 1
        if hypo_index:
            if 'btree' in text:
                hypo_index = False
                hypo_index_id = text.strip().strip('()').split(',')[0]
                hypo_index_ids.append(hypo_index_id)
        if 'Index' in text and 'Scan' in text:
            ind1, ind2 = re.search(r'Index.*Scan(.*)on ([^\s]+)',
                                   text.strip(), re.IGNORECASE).groups()
            if ind1.strip():
                indexes_names.add(ind1.strip().split(' ')[1])
            else:
                indexes_names.add(ind2)

    while i < query_num:
        costs.append(0)
        i += 1
    return costs, indexes_names, hypo_index_ids


def parse_plan_cost(line):
    """ Parse the explain plan to get the estimated cost by database optimizer. """
    cost = -1
    # like "Limit  (cost=19932.04..19933.29 rows=100 width=17)"
    pattern = re.compile(r'\(cost=([^)]*)\)', re.S)
    matched_res = re.search(pattern, line)
    if matched_res and len(matched_res.group(1).split()) == 3:
        _cost, _rows, _width = matched_res.group(1).split()
        # like cost=19932.04..19933.29
        cost = float(_cost.split('..')[-1])
    return cost


def parse_single_advisor_results(results) -> List[AdvisedIndex]:
    indexes = []
    for cur_tuple in results:
        res = cur_tuple[0]
        schema_idx = 0
        table_idx = 1
        index_type_idx = -1
        columns_slice = slice(2, -1)
        # like '(1 row)' or (2 rows)
        if res.strip().endswith('rows)') or res.strip().endswith(' row)'):
            continue
        # like ' (public,date_dim,d_year,global)' or ' (public,store_sales,"ss_sold_date_sk,ss_item_sk","")'
        if len(res) > 2 and res.strip()[0:1] == '(':
            items = res.strip().split(',')
            table = items[schema_idx][1:] + '.' + items[table_idx]
            columns = ','.join(items[columns_slice]).strip('\"')
            if columns == '':
                continue
            if items[index_type_idx].strip(') ') not in ['global', 'local']:
                index_type = ''
            else:
                index_type = items[index_type_idx].strip(') ')
            indexes.append(IndexItemFactory().get_index(table, columns, index_type))
    return indexes


def __add_valid_index(record, hypoid_table_column, valid_indexes: list):
    # like 'Index Scan using <134667>btree_global_item_i_manufact_id on item  (cost=0.00..68.53 rows=16 width=59)'
    tokens = record.split(' ')
    for token in tokens:
        if 'btree' in token:
            if 'btree_global_' in token:
                index_type = 'global'
            elif 'btree_local_' in token:
                index_type = 'local'
            else:
                index_type = ''
            hypo_index_id = re.search(
                r'\d+', token.split('_', 1)[0]).group()
            table_columns = hypoid_table_column.get(hypo_index_id)
            if not table_columns:
                continue
            table, columns = table_columns.split(':')
            index = IndexItemFactory().get_index(table, columns, index_type)
            if index not in valid_indexes:
                valid_indexes.append(index)


def get_checked_indexes(index_check_results, tables) -> list:
    valid_indexes = []
    hypoid_table_column = {}
    hypo_index_info_length = 4
    btree_idx = 0
    index_id_idx = 1
    table_idx = 2
    columns_idx = 3
    for cur_tuple in index_check_results:
        # like '(<134672>btree_local_customer_c_customer_sk,134672,customer,"(c_customer_sk)")'
        text = cur_tuple[0]
        if text.strip().startswith('(<') and 'btree' in text:
            if len(text.split(',', 3)) == hypo_index_info_length:
                hypo_index_info = text.split(',', 3)
                table_name = re.search(r'btree(_global|_local|)_(.*%s)' % hypo_index_info[table_idx],
                                       hypo_index_info[btree_idx]).group(2)
                match_flag, table_name = match_table_name(table_name, tables)
                if not match_flag:
                    return valid_indexes
                hypoid_table_column[hypo_index_info[index_id_idx]] = \
                    table_name + ':' + hypo_index_info[columns_idx].strip('"()')

        if 'Index' in text and 'Scan' in text and 'btree' in text:
            __add_valid_index(text, hypoid_table_column, valid_indexes)
    return valid_indexes
