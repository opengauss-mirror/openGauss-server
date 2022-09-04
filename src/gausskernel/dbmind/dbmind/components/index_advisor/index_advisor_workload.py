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

import argparse
import copy
import getpass
import json
import logging
import os
import random
import re
import sys
import select
from itertools import groupby, chain, combinations
from typing import Tuple, List

import sqlparse

try:
    from .sql_output_parser import parse_single_advisor_results, parse_explain_plan, \
        get_checked_indexes, parse_table_sql_results, parse_existing_indexes_results
    from .sql_generator import get_single_advisor_sql, get_index_check_sqls, get_existing_index_sql, \
        get_workload_cost_sqls
    from .executors.common import BaseExecutor
    from .executors.gsql_executor import GsqlExecutor
    from .mcts import MCTS
    from .utils import match_table_name, IndexItemFactory, \
        AdvisedIndex, ExistingIndex, QueryItem, WorkLoad, QueryType, IndexType, COLUMN_DELIMITER, \
        lookfor_subsets_configs, \
        match_columns, infer_workload_benefit
except ImportError:
    from sql_output_parser import parse_single_advisor_results, parse_explain_plan, \
        get_checked_indexes, parse_table_sql_results, parse_existing_indexes_results
    from sql_generator import get_single_advisor_sql, get_index_check_sqls, get_existing_index_sql, \
        get_workload_cost_sqls
    from executors.common import BaseExecutor
    from executors.gsql_executor import GsqlExecutor
    from mcts import MCTS
    from utils import match_table_name, IndexItemFactory, \
        AdvisedIndex, ExistingIndex, QueryItem, WorkLoad, QueryType, IndexType, COLUMN_DELIMITER, \
        lookfor_subsets_configs, \
        match_columns, infer_workload_benefit

SAMPLE_NUM = 5
MAX_INDEX_COLUMN_NUM = 4
MAX_INDEX_NUM = None
MAX_INDEX_STORAGE = None
FULL_ARRANGEMENT_THRESHOLD = 20
NEGATIVE_RATIO_THRESHOLD = 0.2
SHARP = '#'
JSON_TYPE = False
BLANK = ' '
SQL_TYPE = ['select', 'delete', 'insert', 'update']
NUMBER_SET_PATTERN = r'\((\s*(\-|\+)?\d+(\.\d+)?\s*)(,\s*(\-|\+)?\d+(\.\d+)?\s*)*[,]?\)'
SQL_PATTERN = [r'([^\\])\'((\')|(.*?([^\\])\'))',  # match all content in single quotes
               NUMBER_SET_PATTERN,  # match integer set in the IN collection
               r'(([^<>]\s*=\s*)|([^<>]\s+))(\d+)(\.\d+)?']  # match single integer
SQL_DISPLAY_PATTERN = [r'\'((\')|(.*?\'))',  # match all content in single quotes
                       NUMBER_SET_PATTERN,  # match integer set in the IN collection
                       r'([^\_\d])\d+(\.\d+)?']  # match single integer
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
os.umask(0o0077)


def path_type(path):
    realpath = os.path.realpath(path)
    if os.path.exists(realpath):
        return realpath
    raise argparse.ArgumentTypeError('%s is not a valid path.' % path)


class CheckWordValid(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        ill_character = [" ", "|", ";", "&", "$", "<", ">", "`", "\\", "'", "\"",
                         "{", "}", "(", ")", "[", "]", "~", "*", "?", "!", "\n"]
        if not values.strip():
            return
        if any(ill_char in values for ill_char in ill_character):
            parser.error('There are illegal characters in your input.')
        setattr(namespace, self.dest, values)


def read_input_from_pipe():
    """
    Read stdin input if there is "echo 'str1 str2' | python xx.py", return the input string.
    """
    input_str = ""
    r_handle, _, _ = select.select([sys.stdin], [], [], 0)
    if not r_handle:
        return ""

    for item in r_handle:
        if item == sys.stdin:
            input_str = sys.stdin.read().strip()
    return input_str


def get_password():
    password = read_input_from_pipe()
    if password:
        logging.warning("Read password from pipe.")
    else:
        password = getpass.getpass("Password for database user:")
    if not password:
        raise ValueError('Please input the password')
    return password


def get_positive_sql_count(candidate_indexes: List[AdvisedIndex], workload: WorkLoad):
    positive_sql_count = 0
    for query in workload.get_queries():
        for index in candidate_indexes:
            if workload.is_positive_query(index, query):
                positive_sql_count += query.get_frequency()
                break
    return int(positive_sql_count)


def print_statement(index_list: List[Tuple[str]], schema_table: str):
    for columns, index_type in index_list:
        index_name = 'idx_%s_%s%s' % (schema_table.split('.')[-1],
                                      (index_type + '_' if index_type else ''),
                                      '_'.join(columns.split(COLUMN_DELIMITER)))
        statement = 'CREATE INDEX %s ON %s%s%s;' % (index_name, schema_table,
                                                    '(' + columns + ')',
                                                    (' ' + index_type if index_type else ''))
        print(statement)


class IndexAdvisor:
    def __init__(self, executor: BaseExecutor, workload: WorkLoad, multi_iter_mode: bool):
        self.executor = executor
        self.workload = workload
        self.workload_used_index = set()
        self.multi_iter_mode = multi_iter_mode

        self.determine_indexes = []
        self.integrate_indexes = {}

        self.display_detail_info = {}

    def complex_index_advisor(self, candidate_indexes: List[AdvisedIndex]):
        atomic_config_total = generate_sorted_atomic_config(self.workload.get_queries(), candidate_indexes)
        if atomic_config_total and len(atomic_config_total[0]) != 0:
            raise ValueError("The empty atomic config isn't generated!")
        for atomic_config in atomic_config_total:
            estimate_workload_cost_file(self.executor, self.workload, atomic_config)
        self.workload.set_index_benefit()
        if MAX_INDEX_STORAGE:
            opt_config = MCTS(self.workload, atomic_config_total, candidate_indexes,
                              MAX_INDEX_STORAGE, MAX_INDEX_NUM)
        else:
            opt_config = greedy_determine_opt_config(self.workload, atomic_config_total,
                                                     candidate_indexes)
        self.filter_redundant_indexes_with_diff_types(opt_config)
        self.display_detail_info['positive_stmt_count'] = get_positive_sql_count(candidate_indexes,
                                                                                 self.workload)
        if len(opt_config) == 0:
            print("No optimal indexes generated!")
            return None
        return opt_config

    def simple_index_advisor(self, candidate_indexes: List[AdvisedIndex]):
        estimate_workload_cost_file(self.executor, self.workload)
        for index in candidate_indexes:
            estimate_workload_cost_file(self.executor, self.workload, (index,))
        self.workload.set_index_benefit()
        self.filter_redundant_indexes_with_diff_types(candidate_indexes)
        if not candidate_indexes:
            print("No optimal indexes generated!")
            return None

        self.display_detail_info['positive_stmt_count'] = get_positive_sql_count(candidate_indexes,
                                                                                 self.workload)
        return candidate_indexes

    def filter_low_benefit_index(self, opt_indexes: List[AdvisedIndex]):
        for key, index in enumerate(opt_indexes):
            sql_optimized = 0
            negative_sql_ratio = 0
            insert_queries, delete_queries, update_queries, select_queries, \
            positive_queries, ineffective_queries, negative_queries = self.workload.get_index_related_queries(index)
            sql_num = self.workload.get_index_sql_num(index)
            # Calculate the average benefit of each positive SQL.
            for query in positive_queries:
                sql_optimized += (1 - self.workload.get_indexes_cost_of_query(query, (index,)) /
                                  self.workload.get_origin_cost_of_query(query)) * query.get_frequency()
            total_queries_num = sql_num['negative'] + sql_num['ineffective'] + sql_num['positive']
            if total_queries_num:
                negative_sql_ratio = sql_num['negative'] / total_queries_num
            # Filter the candidate indexes that do not meet the conditions of optimization.
            if not positive_queries:
                continue
            if sql_optimized / sql_num['positive'] < 0.1:
                continue
            if sql_optimized / sql_num['positive'] < \
                    NEGATIVE_RATIO_THRESHOLD < negative_sql_ratio:
                continue
            self.determine_indexes.append(index)

    def record_info(self, index: AdvisedIndex, sql_info, table_name: str, statement: str):
        sql_num = self.workload.get_index_sql_num(index)
        total_sql_num = int(sql_num['positive'] + sql_num['ineffective'] + sql_num['negative'])
        workload_optimized = index.benefit / self.workload.get_total_origin_cost() * 100
        sql_info['workloadOptimized'] = '%.2f' % \
                                        (workload_optimized if workload_optimized > 1 else 1)
        sql_info['schemaName'] = index.get_table().split('.')[0]
        sql_info['tbName'] = table_name
        sql_info['columns'] = index.get_columns()
        sql_info['index_type'] = index.get_index_type()
        sql_info['statement'] = statement
        sql_info['dmlCount'] = total_sql_num
        sql_info['selectRatio'] = 1
        sql_info['insertRatio'] = sql_info['deleteRatio'] = sql_info['updateRatio'] = 0
        if total_sql_num:
            sql_info['selectRatio'] = round(
                (sql_num['select']) * 100 / total_sql_num, 2)
            sql_info['insertRatio'] = round(
                sql_num['insert'] * 100 / total_sql_num, 2)
            sql_info['deleteRatio'] = round(
                sql_num['delete'] * 100 / total_sql_num, 2)
            sql_info['updateRatio'] = round(
                100 - sql_info['selectRatio'] - sql_info['insertRatio'] - sql_info['deleteRatio'], 2)
        sql_info['associationIndex'] = index.association_indexes
        self.display_detail_info['recommendIndexes'].append(sql_info)

    def compute_index_optimization_info(self, index: AdvisedIndex, table_name: str, statement: str):
        sql_info = {'sqlDetails': []}
        insert_queries, delete_queries, update_queries, select_queries, positive_queries, ineffective_queries, negative_queries = \
            self.workload.get_index_related_queries(index)

        for category, queries in zip([QueryType.INEFFECTIVE, QueryType.POSITIVE, QueryType.NEGATIVE],
                                     [ineffective_queries, positive_queries, negative_queries]):
            sql_count = int(sum(query.get_frequency() for query in queries))
            # Record 5 ineffective or negative queries.
            if category in [QueryType.INEFFECTIVE, QueryType.NEGATIVE]:
                queries = queries[:5]
            for query in queries:
                sql_detail = {}
                sql_template = query.get_statement()
                for pattern in SQL_DISPLAY_PATTERN:
                    sql_template = re.sub(pattern, '?', sql_template)

                sql_detail['sqlTemplate'] = sql_template
                sql_detail['sql'] = query.get_statement()
                sql_detail['sqlCount'] = int(round(sql_count))

                if category == QueryType.POSITIVE:
                    sql_optimized = (self.workload.get_origin_cost_of_query(query)
                                     - self.workload.get_indexes_cost_of_query(query, tuple([index]))) \
                                    / self.workload.get_indexes_cost_of_query(query, tuple([index])) * 100
                    sql_detail['optimized'] = '%.1f' % sql_optimized
                sql_detail['correlationType'] = category.value
                sql_info['sqlDetails'].append(sql_detail)
        self.record_info(index, sql_info, table_name, statement)

    def display_advise_indexes_info(self, show_detail: bool):
        self.display_detail_info['workloadCount'] = int(
            sum(query.get_frequency() for query in self.workload.get_queries()))
        index_current_storage = 0
        cnt = 0
        self.display_detail_info['recommendIndexes'] = []
        for key, index in enumerate(self.determine_indexes):
            # constraints for Top-N algorithm
            if MAX_INDEX_STORAGE and (index_current_storage + index.get_storage()) > MAX_INDEX_STORAGE:
                continue
            if MAX_INDEX_NUM and cnt == MAX_INDEX_NUM:
                break
            if not self.multi_iter_mode and index.benefit <= 0:
                continue
            index_current_storage += index.get_storage()
            cnt += 1
            # display determine indexes
            table_name = index.get_table().split('.')[-1]
            index_name = 'idx_%s_%s%s' % (table_name, (index.get_index_type()
                                                       + '_' if index.get_index_type() else ''),
                                          '_'.join(index.get_columns().split(COLUMN_DELIMITER))
                                          )
            statement = 'CREATE INDEX %s ON %s%s%s;' % (index_name, index.get_table(),
                                                        '(' + index.get_columns() + ')',
                                                        (' ' + index.get_index_type() if index.get_index_type() else '')
                                                        )
            print(statement)
            if show_detail:
                # Record detailed SQL optimization information for each index.
                self.compute_index_optimization_info(
                    index, table_name, statement)

    def generate_incremental_index(self, history_advise_indexes):
        self.integrate_indexes = copy.copy(history_advise_indexes)
        self.integrate_indexes['currentIndexes'] = {}
        for key, index in enumerate(self.determine_indexes):
            self.integrate_indexes['currentIndexes'][index.get_table()] = \
                self.integrate_indexes['currentIndexes'].get(index.get_table(), [])
            self.integrate_indexes['currentIndexes'][index.get_table()].append(
                (index.get_columns(), index.get_index_type()))

    def generate_redundant_useless_indexes(self, history_invalid_indexes):
        created_indexes = fetch_created_indexes(self.executor)
        record_history_invalid_indexes(self.integrate_indexes['historyIndexes'], history_invalid_indexes,
                                       created_indexes)
        print_header_boundary(" Created indexes ")
        self.display_detail_info['createdIndexes'] = []
        if not created_indexes:
            print("No created indexes!")
        else:
            self.record_created_indexes(created_indexes)
            for index in created_indexes:
                print("%s: %s;" % (index.get_schema(), index.get_indexdef()))
        display_useless_redundant_indexes(created_indexes, self.workload_used_index, self.display_detail_info)

    def record_created_indexes(self, created_indexes):
        for index in created_indexes:
            index_info = {'schemaName': index.get_schema(), 'tbName': index.get_table(),
                          'columns': index.get_columns(), 'statement': index.get_indexdef() + ';'}
            self.display_detail_info['createdIndexes'].append(index_info)

    def display_incremental_index(self, history_invalid_indexes,
                                  workload_file_path):

        # Display historical effective indexes.
        if self.integrate_indexes['historyIndexes']:
            print_header_boundary(" Historical effective indexes ")
            for table_name, index_list in self.integrate_indexes['historyIndexes'].items():
                print_statement(index_list, table_name)
        # Display historical invalid indexes.
        if history_invalid_indexes:
            print_header_boundary(" Historical invalid indexes ")
            for table_name, index_list in history_invalid_indexes.items():
                print_statement(index_list, table_name)
        # Save integrate indexes result.
        if not isinstance(workload_file_path, dict):
            integrate_indexes_file = os.path.join(os.path.realpath(os.path.dirname(workload_file_path)),
                                                  'index_result.json')
            for table, indexes in self.integrate_indexes['currentIndexes'].items():
                self.integrate_indexes['historyIndexes'][table] = \
                    self.integrate_indexes['historyIndexes'].get(table, [])
                self.integrate_indexes['historyIndexes'][table].extend(indexes)
                self.integrate_indexes['historyIndexes'][table] = \
                    list(
                        set(map(tuple, (self.integrate_indexes['historyIndexes'][table]))))
            with open(integrate_indexes_file, 'w') as file:
                json.dump(self.integrate_indexes['historyIndexes'], file)

    @staticmethod
    def filter_redundant_indexes_with_diff_types(candidate_indexes: List[AdvisedIndex]):
        sorted_indexes = sorted(candidate_indexes, key=lambda x: (x.get_table(), x.get_columns()))
        for table, _index_group in groupby(sorted_indexes, key=lambda x: x.get_table()):
            index_group = list(_index_group)
            for idx in range(len(index_group) - 1):
                cur_index = index_group[idx]
                next_index = index_group[idx + 1]
                if match_columns(cur_index.get_columns(), next_index.get_columns()):
                    if cur_index.benefit == next_index.benefit:
                        if cur_index.get_index_type() == 'global':
                            candidate_indexes.remove(next_index)
                            index_group[idx + 1] = index_group[idx]
                        else:
                            candidate_indexes.remove(cur_index)
                    else:
                        if cur_index.benefit < next_index.benefit:
                            candidate_indexes.remove(cur_index)
                        else:
                            candidate_indexes.remove(next_index)
                            index_group[idx + 1] = index_group[idx]


def green(text):
    return '\033[32m%s\033[0m' % text


def print_header_boundary(header):
    # Output a header first, which looks more beautiful.
    try:
        term_width = os.get_terminal_size().columns
        # Get the width of each of the two sides of the terminal.
        side_width = (term_width - len(header)) // 2
    except (AttributeError, OSError):
        side_width = 0
    title = SHARP * side_width + header + SHARP * side_width
    print(green(title))


def load_workload(file_path):
    wd_dict = {}
    workload = []
    global BLANK
    with open(file_path, 'r', errors='ignore') as file:
        raw_text = ''.join(file.readlines())
        sqls = sqlparse.split(raw_text)
        for sql in sqls:
            if any(re.search(r'((\A|[\s(,])%s[\s*(])' % tp, sql.lower()) for tp in SQL_TYPE):
                TWO_BLANKS = BLANK * 2
                while TWO_BLANKS in sql:
                    sql = sql.replace(TWO_BLANKS, BLANK)
                if sql.strip() not in wd_dict.keys():
                    wd_dict[sql.strip()] = 1
                else:
                    wd_dict[sql.strip()] += 1
    for sql, freq in wd_dict.items():
        workload.append(QueryItem(sql, freq))

    return workload


def get_workload_template(workload):
    templates = {}
    placeholder = r'@@@'

    for item in workload:
        sql_template = item.get_statement()
        for pattern in SQL_PATTERN:
            sql_template = re.sub(pattern, placeholder, sql_template)
        if sql_template not in templates:
            templates[sql_template] = {}
            templates[sql_template]['cnt'] = 0
            templates[sql_template]['samples'] = []
        templates[sql_template]['cnt'] += item.get_frequency()
        # reservoir sampling
        if len(templates[sql_template]['samples']) < SAMPLE_NUM:
            templates[sql_template]['samples'].append(item.get_statement())
        else:
            if random.randint(0, templates[sql_template]['cnt']) < SAMPLE_NUM:
                templates[sql_template]['samples'][random.randint(0, SAMPLE_NUM - 1)] = \
                    item.get_statement()

    return templates


def workload_compression(input_path):
    compressed_workload = []
    if isinstance(input_path, dict):
        templates = input_path
    elif JSON_TYPE:
        with open(input_path, 'r', errors='ignore') as file:
            templates = json.load(file)
    else:
        workload = load_workload(input_path)
        templates = get_workload_template(workload)

    for _, elem in templates.items():
        for sql in elem['samples']:
            compressed_workload.append(
                QueryItem(sql.strip(), elem['cnt'] / len(elem['samples'])))

    return compressed_workload


def generate_single_column_indexes(advised_indexes: List[AdvisedIndex]):
    """ Generate single column indexes. """
    single_column_indexes = []
    if len(advised_indexes) == 0:
        return single_column_indexes

    for index in advised_indexes:
        table = index.get_table()
        columns = index.get_columns()
        index_type = index.get_index_type()
        for column in columns.split(COLUMN_DELIMITER):
            single_column_index = IndexItemFactory().get_index(table, column, index_type)
            if single_column_index not in single_column_indexes:
                single_column_indexes.append(single_column_index)
    return single_column_indexes


def add_more_column_index(indexes, table, columns_info, single_col_info):
    columns, columns_index_type = columns_info
    single_column, single_index_type = single_col_info
    if columns_index_type.strip('"') != single_index_type.strip('"'):
        add_more_column_index(indexes, table, (columns, 'local'),
                              (single_column, 'local'))
        add_more_column_index(indexes, table, (columns, 'global'),
                              (single_column, 'global'))
    else:
        current_columns_index = IndexItemFactory().get_index(table, columns + COLUMN_DELIMITER + single_column,
                                                             columns_index_type)
        if current_columns_index in indexes:
            return
        # To make sure global is behind local
        if single_index_type == 'local':
            global_columns_index = IndexItemFactory().get_index(table, columns + COLUMN_DELIMITER + single_column,
                                                                'global')
            if global_columns_index in indexes:
                global_pos = indexes.index(global_columns_index)
                indexes[global_pos] = current_columns_index
                current_columns_index = global_columns_index
        indexes.append(current_columns_index)


def query_index_advise(executor, query):
    """ Call the single-indexes-advisor in the database. """

    sql = get_single_advisor_sql(query)
    results = executor.execute_sqls([sql])
    advised_indexes = parse_single_advisor_results(results)

    return advised_indexes


def get_index_storage(executor, hypo_index_id):
    index_size_sqls = ['select * from pg_catalog.hypopg_estimate_size(%s);' % hypo_index_id]
    results = executor.execute_sqls(index_size_sqls)
    for cur_tuple in results:
        if re.match(r'\d+', str(cur_tuple[0]).strip()):
            return float(str(cur_tuple[0]).strip()) / 1024 / 1024


def estimate_workload_cost_file(executor, workload, indexes=None):
    select_queries = []
    select_queries_pos = []
    query_costs = [0] * len(workload.get_queries())
    for idx, query in enumerate(workload.get_queries()):
        select_queries.append(query.get_statement())
        select_queries_pos.append(idx)
    sqls = get_workload_cost_sqls(select_queries, indexes, is_multi_node(executor))
    results = executor.execute_sqls(sqls)
    # Parse the result of explain plans.
    costs, indexes_names, hypo_index_ids = parse_explain_plan(results, len(select_queries))
    # Update query cost for select queries and positive_pos for indexes.
    for cost, query_pos in zip(costs, select_queries_pos):
        query_costs[query_pos] = cost * workload.get_queries()[query_pos].get_frequency()
    workload.add_indexes(indexes, query_costs, indexes_names)
    # Reset hypopg indexes.
    if indexes:
        for index, hypo_index_id in zip(indexes, hypo_index_ids):
            storage = get_index_storage(executor, hypo_index_id)
            index.set_storage(storage)

    executor.execute_sqls(['SELECT pg_catalog.hypopg_reset_index();'])


def is_multi_node(executor: BaseExecutor):
    sql = "select pg_catalog.count(*) from pgxc_node where node_type='C';"
    for cur_tuple in executor.execute_sqls([sql]):
        if str(cur_tuple[0]).isdigit():
            return int(cur_tuple[0]) > 0


def query_index_check(executor, query, indexes):
    """ Obtain valid indexes based on the optimizer. """
    valid_indexes = {}
    if len(indexes) == 0:
        return valid_indexes

    index_check_results = executor.execute_sqls(get_index_check_sqls(query, indexes, is_multi_node(executor)))
    valid_indexes = get_checked_indexes(index_check_results, set(index.get_table() for index in indexes))
    return valid_indexes


def get_valid_indexes(advised_indexes, statement, executor):
    need_check = False
    single_column_indexes = generate_single_column_indexes(advised_indexes)
    valid_indexes = query_index_check(executor, statement, single_column_indexes)

    for i in range(MAX_INDEX_COLUMN_NUM):
        for table, index_group in groupby(valid_indexes, key=lambda x: x.get_table()):
            for index in index_group:
                columns = index.get_columns()
                index_type = index.get_index_type()
                if columns.count(',') != i:
                    continue
                need_check = True
                for single_column_index in single_column_indexes:
                    _table = single_column_index.get_table()
                    if _table != table:
                        continue
                    single_column = single_column_index.get_columns()
                    single_index_type = single_column_index.get_index_type()
                    if single_column not in columns.split(COLUMN_DELIMITER):
                        add_more_column_index(valid_indexes, table, (columns, index_type),
                                              (single_column, single_index_type))
        if need_check:
            valid_indexes = query_index_check(executor, statement, valid_indexes)
            need_check = False
        else:
            break
    return valid_indexes


def get_redundant_created_indexes(indexes: List[ExistingIndex], unused_indexes: List[ExistingIndex]):
    sorted_indexes = sorted(indexes, key=lambda i: (i.get_table(), len(i.get_columns().split(COLUMN_DELIMITER))))
    redundant_indexes = []
    for table, index_group in groupby(sorted_indexes, key=lambda i: i.get_table()):
        cur_table_indexes = list(index_group)
        for pos, index in enumerate(cur_table_indexes[:-1]):
            is_redundant = False
            for next_index in cur_table_indexes[pos + 1:]:
                if match_columns(index.get_columns(), next_index.get_columns()):
                    is_redundant = True
                    index.redundant_objs.append(next_index)
            if is_redundant:
                redundant_indexes.append(index)
    remove_list = []
    for pos, index in enumerate(redundant_indexes):
        is_redundant = False
        for redundant_obj in index.redundant_objs:
            # Redundant objects are not in the useless index set, or
            # both redundant objects and redundant index in the useless index must be redundant index.
            index_exist = redundant_obj not in unused_indexes or \
                          (redundant_obj in unused_indexes and index in unused_indexes)
            if index_exist:
                is_redundant = True
        if not is_redundant:
            remove_list.append(pos)
    for item in sorted(remove_list, reverse=True):
        redundant_indexes.pop(item)
    return redundant_indexes


def record_history_invalid_indexes(history_indexes, history_invalid_indexes, indexes):
    for index in indexes:
        # Update historical indexes validity.
        schema_table = index.get_schema_table()
        cur_columns = index.get_columns()
        if not history_indexes.get(schema_table):
            continue
        for column in history_indexes.get(schema_table, dict()):
            history_index_column = list(map(str.strip, column[0].split(',')))
            existed_index_column = list(map(str.strip, cur_columns[0].split(',')))
            if len(history_index_column) > len(existed_index_column):
                continue
            if history_index_column == existed_index_column[0:len(history_index_column)]:
                history_indexes[schema_table].remove(column)
                history_invalid_indexes[schema_table] = history_invalid_indexes.get(
                    schema_table, list())
                history_invalid_indexes[schema_table].append(column)
                if not history_indexes[schema_table]:
                    del history_indexes[schema_table]


def fetch_created_indexes(executor):
    schemas = [elem.lower()
               for elem in filter(None, executor.get_schema().split(','))]
    created_indexes = []
    for schema in schemas:
        sql = "select tablename from pg_tables where schemaname = '%s'" % schema
        res = executor.execute_sqls([sql])
        if not res:
            continue
        tables = parse_table_sql_results(res)
        if not tables:
            continue
        sql = get_existing_index_sql(schema, tables)
        res = executor.execute_sqls([sql])
        if not res:
            continue
        _created_indexes = parse_existing_indexes_results(res, schema)
        created_indexes.extend(_created_indexes)

    return created_indexes


def print_candidate_indexes(candidate_indexes):
    for index in candidate_indexes:
        table = index.get_table()
        columns = index.get_columns()
        index_type = index.get_index_type()
        if index.get_index_type():
            print("table: ", table, "columns: ", columns, "type: ", index_type)
        else:
            print("table: ", table, "columns: ", columns)


def index_sort_func(index):
    """ Sort indexes function. """
    if index.get_index_type() == 'global':
        return index.get_table(), 0, index.get_columns()
    else:
        return index.get_table(), 1, index.get_columns()


def filter_redundant_indexes_with_same_type(indexes: List[AdvisedIndex]):
    """ Filter redundant indexes with same index_type. """
    candidate_indexes = []
    for table, table_group_indexes in groupby(sorted(indexes, key=lambda x: x.get_table()),
                                              key=lambda x: x.get_table()):
        for index_type, index_type_group_indexes in groupby(
                sorted(table_group_indexes, key=lambda x: x.get_index_type()), key=lambda x: x.get_index_type()):
            column_sorted_indexes = sorted(index_type_group_indexes, key=lambda x: x.get_columns())
            for i in range(len(column_sorted_indexes) - 1):
                if match_columns(column_sorted_indexes[i].get_columns(), column_sorted_indexes[i + 1].get_columns()):
                    continue
                else:
                    index = column_sorted_indexes[i]
                    candidate_indexes.append(index)
            candidate_indexes.append(column_sorted_indexes[-1])
    candidate_indexes.sort(key=index_sort_func)

    return candidate_indexes


def add_query_indexes(indexes: List[AdvisedIndex], queries: List[QueryItem], pos):
    for table, index_group in groupby(indexes, key=lambda x: x.get_table()):
        _indexes = sorted(list(index_group), key=lambda x: -len(x.get_columns()))
        for _index in _indexes:
            if len(queries[pos].get_indexes()) >= FULL_ARRANGEMENT_THRESHOLD:
                break
            queries[pos].add_index(_index)


def generate_candidate_indexes(workload: WorkLoad, executor: BaseExecutor):
    print_header_boundary(" Generate candidate indexes ")

    all_indexes = []
    with executor.session():
        for pos, query in enumerate(workload.get_queries()):
            advised_indexes = query_index_advise(executor, query.get_statement())
            valid_indexes = get_valid_indexes(advised_indexes, query.get_statement(), executor)
            add_query_indexes(valid_indexes, workload.get_queries(), pos)
            for index in valid_indexes:
                if index not in all_indexes:
                    all_indexes.append(index)

        # Filter redundant indexes.
        candidate_indexes = filter_redundant_indexes_with_same_type(all_indexes)
        print_candidate_indexes(candidate_indexes)

        if len(candidate_indexes) == 0:
            print("No candidate indexes generated!")
            estimate_workload_cost_file(executor, workload)

    return candidate_indexes


def powerset(iterable):
    """ powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3) """
    s = list(iterable)
    return chain.from_iterable(combinations(s, r) for r in range(len(s) + 1))


def generate_sorted_atomic_config(queries: List[QueryItem],
                                  candidate_indexes: List[AdvisedIndex]) -> List[Tuple[AdvisedIndex]]:
    atomic_config_total = []

    for query in queries:
        if len(query.get_indexes()) == 0:
            continue

        indexes = []
        for idx, (table, group) in enumerate(groupby(query.get_sorted_indexes(), lambda x: x.get_table())):
            # The max number of table is 2.
            if idx > 1:
                break
            # The max index number for each table is 2.
            indexes.extend(list(group)[:2])
        atomic_configs = powerset(indexes)
        for new_config in atomic_configs:
            if new_config not in atomic_config_total:
                atomic_config_total.append(new_config)
    # Make sure atomic_config_total contains candidate_indexes.
    for index in candidate_indexes:
        if (index,) not in atomic_config_total:
            atomic_config_total.append((index,))
    return atomic_config_total


def display_redundant_indexes(redundant_indexes: List[ExistingIndex]):
    if not redundant_indexes:
        print("No redundant indexes!")
    # Display redundant indexes.
    for index in redundant_indexes:
        statement = "DROP INDEX %s.%s;" % (index.get_schema(), index.get_indexname())
        print(statement)


def record_redundant_indexes(redundant_indexes: List[ExistingIndex], detail_info):
    for index in redundant_indexes:
        statement = "DROP INDEX %s.%s;" % (index.get_schema(), index.get_indexname())
        existing_index = [item.get_indexname() + ':' +
                          item.get_columns() for item in index.redundant_objs]
        redundant_index = {"schemaName": index.get_schema(), "tbName": index.get_table(),
                           "type": IndexType.REDUNDANT.value,
                           "columns": index.get_columns(), "statement": statement,
                           "existingIndex": existing_index}
        detail_info['uselessIndexes'].append(redundant_index)


def display_useless_redundant_indexes(created_indexes, workload_indexnames, detail_info):
    unused_indexes = [index for index in created_indexes if index.get_indexname() not in workload_indexnames]
    print_header_boundary(" Current workload useless indexes ")
    detail_info['uselessIndexes'] = []
    has_unused_index = False

    for cur_index in unused_indexes:
        if 'UNIQUE INDEX' not in cur_index.get_indexdef():
            has_unused_index = True
            statement = "DROP INDEX %s;" % cur_index.get_indexname()
            print(statement)
            useless_index = {"schemaName": cur_index.get_schema(), "tbName": cur_index.get_table(),
                             "type": IndexType.INVALID.value,
                             "columns": cur_index.get_columns(), "statement": statement}
            detail_info['uselessIndexes'].append(useless_index)

    if not has_unused_index:
        print("No useless indexes!")
    print_header_boundary(" Redundant indexes ")
    redundant_indexes = get_redundant_created_indexes(created_indexes, unused_indexes)
    display_redundant_indexes(redundant_indexes)
    record_redundant_indexes(redundant_indexes, detail_info)


def greedy_determine_opt_config(workload: WorkLoad, atomic_config_total: List[Tuple[AdvisedIndex]],
                                candidate_indexes: List[AdvisedIndex]):
    opt_config = []
    for i in range(len(candidate_indexes)):
        cur_max_benefit = 0
        cur_index = None
        for index in candidate_indexes:
            cur_config = copy.copy(opt_config)
            cur_config.append(index)
            cur_estimated_benefit = infer_workload_benefit(workload, cur_config, atomic_config_total)
            if cur_estimated_benefit > cur_max_benefit:
                cur_max_benefit = cur_estimated_benefit
                cur_index = index
        if cur_index:
            if len(opt_config) == MAX_INDEX_NUM:
                break
            opt_config.append(cur_index)
            candidate_indexes.remove(cur_index)
        else:
            break

    return opt_config


def get_last_indexes_result(input_path):
    last_indexes_result_file = os.path.join(os.path.realpath(
        os.path.dirname(input_path)), 'index_result.json')
    integrate_indexes = {'historyIndexes': {}}
    if os.path.exists(last_indexes_result_file):
        try:
            with open(last_indexes_result_file, 'r', errors='ignore') as file:
                integrate_indexes['historyIndexes'] = json.load(file)
        except json.JSONDecodeError:
            return integrate_indexes
    return integrate_indexes


def index_advisor_workload(history_advise_indexes, executor: BaseExecutor, workload_file_path,
                           multi_iter_mode: bool, show_detail: bool):
    queries = workload_compression(workload_file_path)
    workload = WorkLoad(queries)
    candidate_indexes = generate_candidate_indexes(workload, executor)
    index_advisor = IndexAdvisor(executor, workload, multi_iter_mode)
    if candidate_indexes:
        print_header_boundary(" Determine optimal indexes ")
        with executor.session():
            if multi_iter_mode:
                opt_indexes = index_advisor.complex_index_advisor(candidate_indexes)
            else:
                opt_indexes = index_advisor.simple_index_advisor(candidate_indexes)
        if opt_indexes:
            index_advisor.filter_low_benefit_index(opt_indexes)
    index_advisor.display_advise_indexes_info(show_detail)

    index_advisor.generate_incremental_index(history_advise_indexes)
    history_invalid_indexes = {}
    with executor.session():
        index_advisor.generate_redundant_useless_indexes(history_invalid_indexes)
    index_advisor.display_incremental_index(
        history_invalid_indexes, workload_file_path)
    if show_detail:
        print_header_boundary(" Display detail information ")
        sql_info = json.dumps(
            index_advisor.display_detail_info, indent=4, separators=(',', ':'))
        print(sql_info)
    return index_advisor.display_detail_info


def check_parameter(args):
    global MAX_INDEX_NUM, MAX_INDEX_STORAGE, JSON_TYPE
    if args.max_index_num is not None and args.max_index_num <= 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" %
                                         args.max_index_num)
    if args.max_index_storage is not None and args.max_index_storage <= 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" %
                                         args.max_index_storage)
    JSON_TYPE = args.json
    MAX_INDEX_NUM = args.max_index_num
    MAX_INDEX_STORAGE = args.max_index_storage
    # Check if the password contains illegal characters.
    is_legal = re.search(r'^[A-Za-z0-9~!@#%^*\-_=+?,.]+$', args.W)
    if not is_legal:
        raise ValueError("The password contains illegal characters.")


def main(argv):
    arg_parser = argparse.ArgumentParser(
        description='Generate index set for workload.')
    arg_parser.add_argument("db_port", help="Port of database", type=int)
    arg_parser.add_argument("database", help="Name of database", action=CheckWordValid)
    arg_parser.add_argument(
        "--db-host", "--h", help="Host for database", action=CheckWordValid)
    arg_parser.add_argument(
        "-U", "--db-user", help="Username for database log-in", action=CheckWordValid)
    arg_parser.add_argument(
        "file", type=path_type, help="File containing workload queries (One query per line)", action=CheckWordValid)
    arg_parser.add_argument("--schema", help="Schema name for the current business data",
                            required=True, action=CheckWordValid)
    arg_parser.add_argument(
        "--max-index-num", "--max_index_num", help="Maximum number of suggested indexes", type=int)
    arg_parser.add_argument("--max-index-storage", "--max_index_storage",
                            help="Maximum storage of suggested indexes/MB", type=int)
    arg_parser.add_argument("--multi-iter-mode", "--multi_iter_mode", action='store_true',
                            help="Whether to use multi-iteration algorithm", default=False)
    arg_parser.add_argument("--multi-node", "--multi_node", action='store_true',
                            help="Whether to support distributed scenarios", default=False)
    arg_parser.add_argument("--json", action='store_true',
                            help="Whether the workload file format is json", default=False)
    arg_parser.add_argument("--driver", action='store_true',
                            help="Whether to employ python-driver", default=False)
    arg_parser.add_argument("--show-detail", "--show_detail", action='store_true',
                            help="Whether to show detailed sql information", default=False)
    args = arg_parser.parse_args(argv)

    args.W = get_password()
    check_parameter(args)
    # Initialize the connection.
    if args.driver:
        try:
            import psycopg2
            try:
                from .executors.driver_executor import DriverExecutor
            except ImportError:
                from executors.driver_executor import DriverExecutor
            executor = DriverExecutor(args.database, args.db_user, args.W, args.db_host, args.db_port, args.schema)
        except ImportError:
            logging.warning('Python driver import failed, '
                            'the gsql mode will be selected to connect to the database.')

            executor = GsqlExecutor(args.database, args.db_user, args.W, args.db_host, args.db_port, args.schema)
            args.driver = None
    else:
        executor = GsqlExecutor(args.database, args.db_user, args.W, args.db_host, args.db_port, args.schema)
    index_advisor_workload(get_last_indexes_result(args.file), executor, args.file,
                           args.multi_iter_mode, args.show_detail)


if __name__ == '__main__':
    main(sys.argv[1:])
