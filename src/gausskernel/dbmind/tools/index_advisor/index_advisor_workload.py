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
import os
import sys
import argparse
import copy
import getpass
import random
import re
import json
import select
import logging

from DAO.gsql_execute import GSqlExecute
from mcts import MCTS

ENABLE_MULTI_NODE = False
SAMPLE_NUM = 5
MAX_INDEX_COLUMN_NUM = 5
MAX_INDEX_NUM = 10
MAX_INDEX_STORAGE = None
FULL_ARRANGEMENT_THRESHOLD = 20
NEGATIVE_RATIO_THRESHOLD = 0.2
SHARP = '#'
JSON_TYPE = False
DRIVER = None
BLANK = ' '
SQL_TYPE = ['select', 'delete', 'insert', 'update']
SQL_PATTERN = [r'\((\s*(\d+(\.\d+)?\s*)[,]?)+\)',  # match integer set in the IN collection
               r'([^\\])\'((\')|(.*?([^\\])\'))',  # match all content in single quotes
               r'(([^<>]\s*=\s*)|([^<>]\s+))(\d+)(\.\d+)?']  # match single integer
SQL_DISPLAY_PATTERN = [r'\((\s*(\d+(\.\d+)?\s*)[,]?)+\)',  # match integer set in the IN collection
                       r'\'((\')|(.*?\'))',  # match all content in single quotes
                       r'([^\_\d])\d+(\.\d+)?']  # match single integer
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


class CheckValid(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        ill_character = [" ", "|", ";", "&", "$", "<", ">", "`", "\\", "'", "\"",
                         "{", "}", "(", ")", "[", "]", "~", "*", "?", "!", "\n"]
        if not values.strip():
            return
        if any(ill_char in values for ill_char in ill_character):
            raise Exception("There are illegal characters in the %s." % self.dest)
        setattr(namespace, self.dest, values)


def read_input_from_pipe():
    """
    Read stdin input if there is "echo 'str1 str2' | python xx.py",
     return the input string
    """
    input_str = ""
    r_handle, _, _ = select.select([sys.stdin], [], [], 0)
    if not r_handle:
        return ""

    for item in r_handle:
        if item == sys.stdin:
            input_str = sys.stdin.read().strip()
    return input_str


class PwdAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        password = read_input_from_pipe()
        if password:
            logging.warning("Read password from pipe.")
        else:
            password = getpass.getpass("Password for database user:")
        setattr(namespace, self.dest, password)


class QueryItem:
    def __init__(self, sql, freq):
        self.statement = sql
        self.frequency = freq
        self.valid_index_list = []
        self.cost_list = []


class IndexItem:
    instances = {}

    @classmethod
    def get_index(cls, tbl, cols):
        if not (tbl, cols) in cls.instances:
            cls.instances[(tbl, cols)] = cls(tbl, cols)
        return cls.instances[(tbl, cols)]

    def __init__(self, tbl, cols):
        self.table = tbl
        self.columns = cols
        self.atomic_pos = 0
        self.benefit = 0
        self.storage = 0
        self.positive_pos = []
        self.ineffective_pos = []
        self.negative_pos = []
        self.total_sql_num = 0
        self.insert_sql_num = 0
        self.update_sql_num = 0
        self.delete_sql_num = 0
        self.select_sql_num = 0
        self.is_candidate = False


def green(text):
    return '\033[32m%s\033[0m' % text


def print_header_boundary(header):
    # Output a header first, which looks more beautiful.
    try:
        term_width = os.get_terminal_size().columns
        # The width of each of the two sides of the terminal.
        side_width = (term_width - len(header)) // 2
    except (AttributeError, OSError):
        side_width = 0
    title = SHARP * side_width + header + SHARP * side_width
    print(green(title))


def filter_low_benefit(candidate_indexes, multi_iter_mode, workload):
    remove_list = []
    for key, index in enumerate(candidate_indexes):
        sql_optimzed = 0
        if multi_iter_mode:
            cost_list_pos = index.atomic_pos
        else:
            cost_list_pos = key + 1
        for ind, pos in enumerate(index.positive_pos):
            sql_optimzed += 1 - workload[pos].cost_list[cost_list_pos] / workload[pos].cost_list[0]
        negative_ratio = ((index.insert_sql_num + index.delete_sql_num +
                          index.update_sql_num) / index.total_sql_num) if index.total_sql_num else 0
        # filter the candidate indexes that do not meet the conditions of optimization
        if not index.positive_pos:
            remove_list.append(key)
        elif sql_optimzed / len(index.positive_pos) < 0.1:
            remove_list.append(key)
        elif sql_optimzed / len(index.positive_pos) < NEGATIVE_RATIO_THRESHOLD < negative_ratio:
            remove_list.append(key)
    for item in sorted(remove_list, reverse=True):
        candidate_indexes.pop(item)


def display_recommend_result(workload, candidate_indexes, index_cost_total, multi_iter_mode,
                             display_info, integrate_indexes, history_invalid_indexes):
    cnt = 0
    index_current_storage = 0
    # filter candidate indexes with low benefit
    filter_low_benefit(candidate_indexes, multi_iter_mode, workload)
    # display determine result
    integrate_indexes['currentIndexes'] = dict()
    for key, index in enumerate(candidate_indexes):
        integrate_indexes['currentIndexes'][index.table] = \
            integrate_indexes['currentIndexes'].get(index.table, list())
        integrate_indexes['currentIndexes'][index.table].append(index.columns)
        # association history recommendation results
        if integrate_indexes['historyIndexes']:
            from DAO.execute_factory import ExecuteFactory
            ExecuteFactory.match_last_result(index.table, index.columns,
                                             integrate_indexes, history_invalid_indexes)
        if MAX_INDEX_STORAGE and (index_current_storage + index.storage) > MAX_INDEX_STORAGE:
            continue
        if MAX_INDEX_NUM and cnt == MAX_INDEX_NUM:
            break
        index_current_storage += index.storage
        table_name = index.table.split('.')[-1]
        index_name = 'idx_' + table_name + '_' + '_'.join(index.columns.split(', '))
        statement = 'CREATE INDEX ' + index_name + ' ON ' + index.table + '(' + index.columns + ');'
        print(statement)
        cnt += 1
        if multi_iter_mode:
            cost_list_pos = index.atomic_pos
        else:
            cost_list_pos = key + 1

        sql_info = {'sqlDetails': []}
        benefit_types = [index.ineffective_pos, index.positive_pos, index.negative_pos]
        for category, benefit_type in enumerate(benefit_types):
            sql_count = 0
            for item in benefit_type:
                sql_count += workload[item].frequency
            for ind, pos in enumerate(benefit_type):
                sql_detail = {}
                sql_template = workload[pos].statement
                for pattern in SQL_DISPLAY_PATTERN:
                    sql_template = re.sub(pattern, '?', sql_template)

                sql_detail['sqlTemplate'] = sql_template
                sql_detail['sql'] = workload[pos].statement
                sql_detail['sqlCount'] = int(round(sql_count))
                if category == 1:
                    sql_optimzed = (workload[pos].cost_list[0] -
                                    workload[pos].cost_list[cost_list_pos]) / \
                                   workload[pos].cost_list[cost_list_pos]
                    sql_detail['optimized'] = '%.3f' % sql_optimzed
                sql_detail['correlationType'] = category
                sql_info['sqlDetails'].append(sql_detail)
        workload_optimized = (1 - index_cost_total[cost_list_pos] / index_cost_total[0]) * 100
        sql_info['workloadOptimized'] = '%.2f' % (workload_optimized if workload_optimized > 1 else 1)
        sql_info['schemaName'] = index.table.split('.')[0]
        sql_info['tbName'] = table_name
        sql_info['columns'] = index.columns
        sql_info['statement'] = statement
        sql_info['dmlCount'] = round(index.total_sql_num)
        sql_info['selectRatio'] = round((index.select_sql_num * 100 /
                                        index.total_sql_num) if index.total_sql_num else 0, 2)
        sql_info['insertRatio'] = round((index.insert_sql_num * 100 /
                                        index.total_sql_num) if index.total_sql_num else 0, 2)
        sql_info['deleteRatio'] = round((index.delete_sql_num * 100 /
                                        index.total_sql_num) if index.total_sql_num else 0, 2)
        sql_info['updateRatio'] = round((100 - sql_info['selectRatio'] - sql_info['insertRatio']
                                        - sql_info['deleteRatio']) if index.total_sql_num else 0, 2)
        display_info['recommendIndexes'].append(sql_info)


def load_workload(file_path):
    wd_dict = {}
    workload = []
    global BLANK
    with open(file_path, 'r') as file:
        raw_text = ''.join(file.readlines())
        sqls = raw_text.split(';')
        for sql in sqls:
            if any(tp in sql.lower() for tp in SQL_TYPE):
                TWO_BLANKS = BLANK * 2
                while TWO_BLANKS in sql:
                    sql = sql.replace(TWO_BLANKS, BLANK)
                if sql not in wd_dict.keys():
                    wd_dict[sql] = 1
                else:
                    wd_dict[sql] += 1
    for sql, freq in wd_dict.items():
        workload.append(QueryItem(sql, freq))

    return workload


def get_workload_template(workload):
    templates = {}
    placeholder = r'@@@'

    for item in workload:
        sql_template = item.statement
        for pattern in SQL_PATTERN:
            sql_template = re.sub(pattern, placeholder, sql_template)
        if sql_template not in templates:
            templates[sql_template] = {}
            templates[sql_template]['cnt'] = 0
            templates[sql_template]['samples'] = []
        templates[sql_template]['cnt'] += item.frequency
        # reservoir sampling
        if len(templates[sql_template]['samples']) < SAMPLE_NUM:
            templates[sql_template]['samples'].append(item.statement)
        else:
            if random.randint(0, templates[sql_template]['cnt']) < SAMPLE_NUM:
                templates[sql_template]['samples'][random.randint(0, SAMPLE_NUM - 1)] = \
                    item.statement

    return templates


def workload_compression(input_path):
    compressed_workload = []
    total_num = 0
    if JSON_TYPE:
        with open(input_path, 'r') as file:
            templates = json.load(file)
    else:
        workload = load_workload(input_path)
        templates = get_workload_template(workload)

    for _, elem in templates.items():
        for sql in elem['samples']:
            compressed_workload.append(QueryItem(sql.strip(), elem['cnt'] / len(elem['samples'])))
        total_num += elem['cnt']
    return compressed_workload, total_num


# enumerate the column combinations for a suggested index
def get_indexable_columns(table_index_dict):
    query_indexable_columns = {}
    if len(table_index_dict) == 0:
        return query_indexable_columns

    for table in table_index_dict.keys():
        query_indexable_columns[table] = []
        for columns in table_index_dict[table]:
            indexable_columns = columns.split(',')
            for column in indexable_columns:
                query_indexable_columns[table].append(column)

    return query_indexable_columns


def get_valid_index_dict(table_index_dict, query, db):
    need_check = False
    query_indexable_columns = get_indexable_columns(table_index_dict)
    valid_index_dict = db.query_index_check(query.statement, query_indexable_columns,
                                            ENABLE_MULTI_NODE)

    for i in range(MAX_INDEX_COLUMN_NUM):
        for table in valid_index_dict.keys():
            for columns in valid_index_dict[table]:
                if columns.count(',') == i:
                    need_check = True
                    for single_column in query_indexable_columns[table]:
                        if single_column not in columns:
                            valid_index_dict[table].append(columns + ',' + single_column)
        if need_check:
            valid_index_dict = db.query_index_check(query.statement, valid_index_dict,
                                                    ENABLE_MULTI_NODE)
            need_check = False
        else:
            break
    return valid_index_dict


def generate_candidate_indexes(workload, workload_table_name, db):
    candidate_indexes = []
    index_dict = {}
    if DRIVER:
        db.init_conn_handle()
    for k, query in enumerate(workload):
        if not re.search(r'(\A|\s)select\s', query.statement.lower()):
            continue
        table_index_dict = db.query_index_advisor(query.statement, workload_table_name)
        valid_index_dict = get_valid_index_dict(table_index_dict, query, db)

        # record valid indexes for every sql of workload and generate candidate indexes
        for table in valid_index_dict.keys():
            if table not in index_dict.keys():
                index_dict[table] = {}
            for columns in valid_index_dict[table]:
                if len(workload[k].valid_index_list) >= FULL_ARRANGEMENT_THRESHOLD:
                    break
                workload[k].valid_index_list.append(IndexItem.get_index(table, columns))
                if columns in index_dict[table]:
                    index_dict[table][columns].append(k)
                else:
                    column_sql = {columns: [k]}
                    index_dict[table].update(column_sql)
    # filter redundant indexes for candidate indexes
    for table, column_sqls in index_dict.items():
        sorted_column_sqls = sorted(column_sqls.items(), key=lambda item: item[0])
        for i in range(len(sorted_column_sqls) - 1):
            if re.match(sorted_column_sqls[i][0], sorted_column_sqls[i+1][0]):
                sorted_column_sqls[i+1][1].extend(sorted_column_sqls[i][1])
            else:
                print("table: ", table, "columns: ", sorted_column_sqls[i][0])
                candidate_indexes.append(IndexItem.get_index(table, sorted_column_sqls[i][0],
                                                   ))
        print("table: ", table, "columns: ", sorted_column_sqls[-1][0])
        candidate_indexes.append(
            IndexItem.get_index(table, sorted_column_sqls[-1][0]))
    for index in candidate_indexes:
        index.is_candidate = True
    if DRIVER:
        db.close_conn()
    return candidate_indexes


def get_atomic_config_for_query(indexes, config, ind, atomic_configs):
    if ind == len(indexes):
        table_count = {}
        for index in config:
            if index.table not in table_count.keys():
                table_count[index.table] = 1
            else:
                table_count[index.table] += 1
            if len(table_count) > 2 or table_count[index.table] > 2:
                return
        atomic_configs.append(config)

        return

    get_atomic_config_for_query(indexes, copy.copy(config), ind + 1, atomic_configs)
    config.append(indexes[ind])
    get_atomic_config_for_query(indexes, copy.copy(config), ind + 1, atomic_configs)


def is_same_config(config1, config2):
    if len(config1) != len(config2):
        return False

    for index1 in config1:
        is_found = False
        for index2 in config2:
            if index1.table == index2.table and index1.columns == index2.columns:
                is_found = True
        if not is_found:
            return False

    return True


def generate_atomic_config(workload):
    atomic_config_total = []

    for query in workload:
        if len(query.valid_index_list) == 0:
            continue

        atomic_configs = []
        config = []
        get_atomic_config_for_query(query.valid_index_list, config, 0, atomic_configs)

        is_found = False
        for new_config in atomic_configs:
            for exist_config in atomic_config_total:
                if is_same_config(new_config, exist_config):
                    is_found = True
                    break
            if not is_found:
                atomic_config_total.append(new_config)
            is_found = False

    return atomic_config_total


# find the subsets of a given config in the atomic configs
def find_subsets_num(config, atomic_config_total):
    atomic_subsets_num = []
    is_exist = False
    cur_index_atomic_pos = -1
    for i, atomic_config in enumerate(atomic_config_total):
        if len(atomic_config) > len(config):
            continue
        # Record the atomic index position of the newly added index
        if len(atomic_config) == 1 and atomic_config[0].table == config[-1].table and \
                atomic_config[0].columns == config[-1].columns:
            cur_index_atomic_pos = i
        for atomic_index in atomic_config:
            is_exist = False
            for index in config:
                if atomic_index.table == index.table and atomic_index.columns == index.columns:
                    index.storage = atomic_index.storage
                    is_exist = True
                    break
            if not is_exist:
                break
        if is_exist:
            atomic_subsets_num.append(i)
    if cur_index_atomic_pos == -1:
        raise ValueError("No atomic configs found for current config!")
    return atomic_subsets_num, cur_index_atomic_pos


def get_index_num(index, atomic_config_total):
    for i, atomic_config in enumerate(atomic_config_total):
        if len(atomic_config) == 1 and atomic_config[0].table == index.table and \
                atomic_config[0].columns == index.columns:
            return i

    return -1


# infer the total cost of workload for a config according to the cost of atomic configs
def infer_workload_cost(workload, config, atomic_config_total):
    total_cost = 0
    is_computed = False
    atomic_subsets_num, cur_index_atomic_pos = find_subsets_num(config, atomic_config_total)
    if len(atomic_subsets_num) == 0:
        raise ValueError("No atomic configs found for current config!")
    if not config[-1].total_sql_num:
        is_computed = True
    for ind, obj in enumerate(workload):
        if max(atomic_subsets_num) >= len(obj.cost_list):
            raise ValueError("Wrong atomic config for current query!")
        # compute the cost for selection
        min_cost = obj.cost_list[0]
        for num in atomic_subsets_num:
            if num < len(obj.cost_list) and obj.cost_list[num] < min_cost:
                min_cost = obj.cost_list[num]
        total_cost += min_cost

        # record ineffective sql and negative sql for candidate indexes
        if is_computed:
            from DAO.execute_factory import ExecuteFactory
            ExecuteFactory.record_ineffective_negative_sql(config[-1], obj, ind)
    return total_cost, cur_index_atomic_pos


def get_whole_index(tables, db, detail_info, history_indexes, history_invalid_index):
    if DRIVER:
        db.init_conn_handle()
    whole_index, redundant_indexes = \
        db.check_useless_index(tables, history_indexes, history_invalid_index)
    if DRIVER:
        db.close_conn()
    print_header_boundary(" Created indexes ")
    detail_info['createdIndexes'] = []
    if not whole_index:
        print("No created index!")
    else:
        for index in whole_index:
            index_info = {'schemaName': index.schema, 'tbName': index.table,
                          'columns': index.columns, 'statement': index.indexdef + ';'}
            detail_info['createdIndexes'].append(index_info)
            print("%s: %s;" % (index.schema, index.indexdef))
    return whole_index, redundant_indexes


def display_last_recommend_result(integrate_indexes, history_invalid_indexes, input_path):
    # display historical effective indexes
    if integrate_indexes['historyIndexes']:
        print_header_boundary(" Historical effective indexes ")
        for table_name, index_list in integrate_indexes['historyIndexes'].items():
            for column in index_list:
                index_name = 'idx_' + table_name.split('.')[-1] + '_' + '_'.join(column.split(', '))
                statement = 'CREATE INDEX ' + index_name + ' ON ' + table_name + '(' + column + ');'
                print(statement)
    # display historical invalid indexes
    if history_invalid_indexes:
        print_header_boundary(" Historical invalid indexes ")
        for table_name, index_list in history_invalid_indexes.items():
            for column in index_list:
                index_name = 'idx_' + table_name.split('.')[-1] + '_' + '_'.join(column.split(', '))
                statement = 'CREATE INDEX ' + index_name + ' ON ' + table_name + '(' + column + ');'
                print(statement)
    # save integrate indexes result
    integrate_indexes_file = os.path.join(os.path.dirname(input_path), 'index_result.json')
    if integrate_indexes.get('currentIndexes'):
        for table, indexes in integrate_indexes['currentIndexes'].items():
            integrate_indexes['historyIndexes'][table] = \
                integrate_indexes['historyIndexes'].get(table, list())
            integrate_indexes['historyIndexes'][table].extend(indexes)
            integrate_indexes['historyIndexes'][table] = \
                list(set(integrate_indexes['historyIndexes'][table]))
    with open(integrate_indexes_file, 'w') as file:
        json.dump(integrate_indexes['historyIndexes'], file)


def check_unused_index_workload(whole_indexes, redundant_indexes, workload_indexes, detail_info):
    indexes_name = set(index.indexname for index in whole_indexes)
    unused_index = list(indexes_name.difference(workload_indexes))
    remove_list = []
    print_header_boundary(" Current workload useless indexes ")
    detail_info['uselessIndexes'] = []
    # useless index
    unused_index_columns = dict()
    has_unused_index = False
    for cur_index in unused_index:
        for index in whole_indexes:
            if cur_index == index.indexname:
                unused_index_columns[cur_index] = index.columns
                if 'UNIQUE INDEX' not in index.indexdef:
                    has_unused_index = True
                    statement = "DROP INDEX %s;" % index.indexname
                    print(statement)
                    useless_index = {"schemaName": index.schema, "tbName": index.table, "type": 3,
                                     "columns": index.columns, "statement": statement}
                    detail_info['uselessIndexes'].append(useless_index)
    if not has_unused_index:
        print("No useless index!")
    print_header_boundary(" Redundant indexes ")
    # filter redundant index
    for pos, index in enumerate(redundant_indexes):
        is_redundant = False
        for redundant_obj in index.redundant_obj:
            # redundant objects are not in the useless index set or
            # equal to the column value in the useless index must be redundant index
            index_exist = redundant_obj.indexname not in unused_index_columns.keys() or \
                          (unused_index_columns.get(redundant_obj.indexname) and
                           redundant_obj.columns == unused_index_columns[redundant_obj.indexname])
            if index_exist:
                is_redundant = True
        if not is_redundant:
            remove_list.append(pos)
    for item in sorted(remove_list, reverse=True):
        redundant_indexes.pop(item)

    if not redundant_indexes:
        print("No redundant index!")
    # redundant index
    for index in redundant_indexes:
        statement = "DROP INDEX %s.%s;" % \
              (index.schema, index.indexname)
        print(statement)
        existing_index = [item.indexname + ':' + item.columns for item in index.redundant_obj]
        redundant_index = {"schemaName": index.schema, "tbName": index.table, "type": 2,
                           "columns": index.columns, "statement": statement,
                           "existingIndex": existing_index}
        detail_info['uselessIndexes'].append(redundant_index)


def simple_index_advisor(input_path, max_index_num, integrate_indexes, db):
    workload, workload_count = workload_compression(input_path)
    print_header_boundary(" Generate candidate indexes ")
    ori_indexes_name = set()
    history_invalid_indexes = {}
    workload_table_name = dict()
    display_info = {'workloadCount': workload_count, 'recommendIndexes': []}
    candidate_indexes = generate_candidate_indexes(workload, workload_table_name, db)
    if DRIVER:
        db.init_conn_handle()
    if len(candidate_indexes) == 0:
        print("No candidate indexes generated!")
        db.estimate_workload_cost_file(workload, ori_indexes_name=ori_indexes_name)
        if DRIVER:
            db.close_conn()
        return ori_indexes_name, workload_table_name, display_info, history_invalid_indexes

    print_header_boundary(" Determine optimal indexes ")
    ori_total_cost = db.estimate_workload_cost_file(workload, ori_indexes_name=ori_indexes_name)
    index_cost_total = [ori_total_cost]
    for _, obj in enumerate(candidate_indexes):
        new_total_cost = db.estimate_workload_cost_file(workload, [obj])
        obj.benefit = ori_total_cost - new_total_cost
        if obj.benefit > 0:
            index_cost_total.append(new_total_cost)
    if DRIVER:
        db.close_conn()
    if len(index_cost_total) == 1:
        print("No optimal indexes generated!")
        return ori_indexes_name, workload_table_name, display_info, history_invalid_indexes
    global MAX_INDEX_NUM
    MAX_INDEX_NUM = max_index_num
    # match the last recommendation result
    display_recommend_result(workload, candidate_indexes, index_cost_total, False, display_info,
                             integrate_indexes, history_invalid_indexes)
    return ori_indexes_name, workload_table_name, display_info, history_invalid_indexes


def greedy_determine_opt_config(workload, atomic_config_total, candidate_indexes, origin_sum_cost):
    opt_config = []
    index_num_record = set()
    min_cost = origin_sum_cost
    for i in range(len(candidate_indexes)):
        if i == 1 and min_cost == origin_sum_cost:
            break
        cur_min_cost = origin_sum_cost
        cur_index = None
        cur_index_num = -1
        for k, index in enumerate(candidate_indexes):
            if k in index_num_record:
                continue
            cur_config = copy.copy(opt_config)
            cur_config.append(index)
            cur_estimated_cost, cur_index_atomic_pos = \
                infer_workload_cost(workload, cur_config, atomic_config_total)
            if cur_estimated_cost < cur_min_cost:
                cur_min_cost = cur_estimated_cost
                cur_index = index
                cur_index.atomic_pos = cur_index_atomic_pos
                cur_index_num = k
        if cur_index and cur_min_cost < min_cost:
            if MAX_INDEX_STORAGE and sum([obj.storage for obj in opt_config]) + \
                    cur_index.storage > MAX_INDEX_STORAGE:
                candidate_indexes.remove(cur_index)
                continue
            if len(opt_config) == MAX_INDEX_NUM:
                break
            min_cost = cur_min_cost
            opt_config.append(cur_index)
            index_num_record.add(cur_index_num)
        else:
            break

    return opt_config


def complex_index_advisor(input_path, integrate_indexes, db):
    workload, workload_count = workload_compression(input_path)
    print_header_boundary(" Generate candidate indexes ")
    history_invalid_indexes = {}
    ori_indexes_name = set()
    workload_table_name = dict()
    display_info = {'workloadCount': workload_count, 'recommendIndexes': []}
    candidate_indexes = generate_candidate_indexes(workload, workload_table_name, db)
    if DRIVER:
        db.init_conn_handle()
    if len(candidate_indexes) == 0:
        print("No candidate indexes generated!")
        db.estimate_workload_cost_file(workload, ori_indexes_name=ori_indexes_name)
        if DRIVER:
            db.close_conn()
        return ori_indexes_name, workload_table_name, display_info, history_invalid_indexes

    print_header_boundary(" Determine optimal indexes ")
    atomic_config_total = generate_atomic_config(workload)
    if atomic_config_total and len(atomic_config_total[0]) != 0:
        raise ValueError("The empty atomic config isn't generated!")
    index_cost_total = []
    for atomic_config in atomic_config_total:
        index_cost_total.append(db.estimate_workload_cost_file(workload, atomic_config,
                                                               ori_indexes_name))
    if DRIVER:
        db.close_conn()
    if MAX_INDEX_STORAGE:
        opt_config = MCTS(workload, atomic_config_total, candidate_indexes, MAX_INDEX_STORAGE)
    else:
        opt_config = greedy_determine_opt_config(workload, atomic_config_total,
                                                 candidate_indexes, index_cost_total[0])
    if len(opt_config) == 0:
        print("No optimal indexes generated!")
        return ori_indexes_name, workload_table_name, display_info, history_invalid_indexes
    # match the last invalid recommendation result
    display_recommend_result(workload, opt_config, index_cost_total, True, display_info,
                             integrate_indexes, history_invalid_indexes)
    return ori_indexes_name, workload_table_name, display_info, history_invalid_indexes


def get_last_indexes_result(input_path):
    last_indexes_result_file = os.path.join(os.path.dirname(input_path), 'index_result.json')
    integrate_indexes = {'historyIndexes': dict()}
    if os.path.exists(last_indexes_result_file):
        try:
            with open(last_indexes_result_file, 'r') as file:
                integrate_indexes['historyIndexes'] = json.load(file)
        except json.JSONDecodeError:
            return integrate_indexes
    return integrate_indexes


def main():
    arg_parser = argparse.ArgumentParser(description='Generate index set for workload.')
    arg_parser.add_argument("p", help="Port of database", type=int)
    arg_parser.add_argument("d", help="Name of database", action=CheckValid)
    arg_parser.add_argument("--h", help="Host for database",  action=CheckValid)
    arg_parser.add_argument("-U", help="Username for database log-in", action=CheckValid)
    arg_parser.add_argument("-W", help="Password for database user", nargs="?", action=PwdAction)
    arg_parser.add_argument("f", help="File containing workload queries (One query per line)")
    arg_parser.add_argument("--schema", help="Schema name for the current business data",
                            required=True, action=CheckValid)
    arg_parser.add_argument("--max_index_num", help="Maximum number of suggested indexes", type=int)
    arg_parser.add_argument("--max_index_storage",
                            help="Maximum storage of suggested indexes/MB", type=int)
    arg_parser.add_argument("--multi_iter_mode", action='store_true',
                            help="Whether to use multi-iteration algorithm", default=False)
    arg_parser.add_argument("--multi_node", action='store_true',
                            help="Whether to support distributed scenarios", default=False)
    arg_parser.add_argument("--json", action='store_true',
                            help="Whether the workload file format is json", default=False)
    arg_parser.add_argument("--driver", action='store_true',
                            help="Whether to employ python-driver", default=False)
    arg_parser.add_argument("--show_detail", action='store_true',
                            help="Whether to show detailed sql information", default=False)
    args = arg_parser.parse_args()

    global MAX_INDEX_NUM, ENABLE_MULTI_NODE, MAX_INDEX_STORAGE, JSON_TYPE, DRIVER
    if args.max_index_num is not None and args.max_index_num <= 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" %
                                         args.max_index_num)
    if args.max_index_storage is not None and args.max_index_storage <= 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" %
                                         args.max_index_storage)

    JSON_TYPE = args.json
    MAX_INDEX_NUM = args.max_index_num or 10
    ENABLE_MULTI_NODE = args.multi_node
    MAX_INDEX_STORAGE = args.max_index_storage
    if args.U and args.U != getpass.getuser() and not args.W:
        raise ValueError('Enter the \'-W\' parameter for user '
                         + args.U + ' when executing the script.')
    # Initialize the connection
    if args.driver:
        try:
            from DAO.driver_execute import DriverExecute
            db = DriverExecute(args.d, args.U, args.W, args.h, args.p, args.schema,
                               args.multi_node, args.max_index_storage)
        except ImportError:
            logging.warning('Python driver import failed, '
                            'the gsql mode will be selected to connect to the database.')
            db = GSqlExecute(args.d, args.U, args.W, args.h, args.p, args.schema,
                             args.multi_node, args.max_index_storage)
            db.init_conn_handle()
            args.driver = None
    else:
        db = GSqlExecute(args.d, args.U, args.W, args.h, args.p, args.schema,
                         args.multi_node, args.max_index_storage)
        db.init_conn_handle()
    DRIVER = args.driver
    integrate_indexes = get_last_indexes_result(args.f)
    if args.multi_iter_mode:
        workload_indexes, tables, detail_info, history_invalid_indexes = \
            complex_index_advisor(args.f, integrate_indexes, db)
    else:
        workload_indexes, tables, detail_info, history_invalid_indexes = \
            simple_index_advisor(args.f, args.max_index_num, integrate_indexes, db)

    whole_indexes, redundant_indexes = \
        get_whole_index(tables, db, detail_info,
                        integrate_indexes['historyIndexes'], history_invalid_indexes)
    # check the unused indexes of the current workload based on the whole index
    check_unused_index_workload(whole_indexes, redundant_indexes, workload_indexes, detail_info)
    # display the results of the last index recommendation
    display_last_recommend_result(integrate_indexes, history_invalid_indexes, args.f)
    if args.show_detail:
        print_header_boundary(" Display detail information ")
        sql_info = json.dumps(detail_info, indent=4, separators=(',', ':'))
        print(sql_info)


if __name__ == '__main__':
    main()

