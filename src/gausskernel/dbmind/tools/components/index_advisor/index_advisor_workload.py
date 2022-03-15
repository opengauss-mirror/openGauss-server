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

try:
    from .dao.gsql_execute import GSqlExecute
    from .dao.execute_factory import ExecuteFactory
except ImportError:
    from dao.gsql_execute import GSqlExecute
    from dao.execute_factory import ExecuteFactory

ENABLE_MULTI_NODE = False
SAMPLE_NUM = 5
MAX_INDEX_COLUMN_NUM = 4
MAX_INDEX_NUM = None
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
            raise Exception(
                "There are illegal characters in the %s." % self.dest)
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


def get_password():
    password = read_input_from_pipe()
    if password:
        logging.warning("Read password from pipe.")
    else:
        password = getpass.getpass("Password for database user:")
    if not password:
        raise ValueError('Please input the password')
    return password


class QueryItem:
    def __init__(self, sql, freq):
        self.statement = sql
        self.frequency = freq
        self.valid_index_list = []
        self.cost_list = []


class IndexItem:
    def __init__(self, tbl, cols, index_type=None):
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
        self.index_type = index_type
        self.is_candidate = False

    def __str__(self):
        return f'{self.table} {self.columns} {self.index_type}'


def singleton(cls):
    instances = {}
    def _singleton(*args, **kwargs):
        if not cls in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]
    return _singleton


@singleton
class IndexItemFactory:
    def __init__(self):
        self.indexes = {}

    def get_index(self, tbl, cols, index_type):
        if not (tbl, tuple(cols), index_type) in self.indexes:
            self.indexes[(tbl, tuple(cols), index_type)] = IndexItem(tbl, cols, index_type=index_type)
        return self.indexes[(tbl, tuple(cols), index_type)]


class IndexAdvisor:
    def __init__(self, db, workload_info, multi_iter_mode):
        self.db = db
        self.workload_info = workload_info
        self.workload_used_index = set()
        self.multi_iter_mode = multi_iter_mode

        self.determine_indexes = []
        self.integrate_indexes = {}
        self.index_cost_total = []

        self.display_detail_info = {}

    def retain_lower_cost_index(self, candidate_indexes):
        remove_indexes = []
        for i in range(len(candidate_indexes)-1):
            if candidate_indexes[i].table != candidate_indexes[i+1].table:
                continue
            if candidate_indexes[i].columns == candidate_indexes[i+1].columns:
                if self.index_cost_total[candidate_indexes[i].atomic_pos] <= \
                   self.index_cost_total[candidate_indexes[i+1].atomic_pos]:
                    remove_indexes.append(i+1)
                else:
                    remove_indexes.append(i)
        for index in remove_indexes[::-1]:
            candidate_indexes.pop(index)

    def complex_index_advisor(self):
        self.display_detail_info['workloadCount'] = self.workload_info[1]
        print_header_boundary(" Generate candidate indexes ")
        candidate_indexes = generate_candidate_indexes(
            self.workload_info[0], self.db)
        if DRIVER:
            self.db.init_conn_handle()
        if len(candidate_indexes) == 0:
            print("No candidate indexes generated!")
            self.db.estimate_workload_cost_file(self.workload_info[0],
                                                ori_indexes_name=self.workload_used_index)
            if DRIVER:
                self.db.close_conn()
            return None

        print_header_boundary(" Determine optimal indexes ")
        atomic_config_total = generate_atomic_config(self.workload_info[0])
        if atomic_config_total and len(atomic_config_total[0]) != 0:
            raise ValueError("The empty atomic config isn't generated!")
        for atomic_config in atomic_config_total:
            self.index_cost_total.append(
                self.db.estimate_workload_cost_file(self.workload_info[0], atomic_config,
                                                    self.workload_used_index))
        if DRIVER:
            self.db.close_conn()

        opt_config = greedy_determine_opt_config(self.workload_info[0], atomic_config_total,
                                                 candidate_indexes, self.index_cost_total[0])
        self.retain_lower_cost_index(candidate_indexes)
        if len(opt_config) == 0:
            print("No optimal indexes generated!")
            return None
        return opt_config

    def retain_high_benefit_index(self, candidate_indexes):
        remove_indexes = []
        for i in range(len(candidate_indexes)-1):
            candidate_indexes[i].cost_pos = i + 1
            if candidate_indexes[i].table != candidate_indexes[i+1].table:
                continue
            if candidate_indexes[i].columns == candidate_indexes[i+1].columns:
                if candidate_indexes[i].benefit >= candidate_indexes[i+1].benefit:
                    remove_indexes.append(i+1)
                else:
                    remove_indexes.append(i)
        candidate_indexes[len(candidate_indexes)-1].cost_pos = len(candidate_indexes)
        for index in remove_indexes[::-1]:
            candidate_indexes.pop(index)

    def simple_index_advisor(self):
        self.display_detail_info['workloadCount'] = self.workload_info[1]
        print_header_boundary(" Generate candidate indexes ")
        candidate_indexes = generate_candidate_indexes(
            self.workload_info[0], self.db)
        if DRIVER:
            self.db.init_conn_handle()
        if len(candidate_indexes) == 0:
            print("No candidate indexes generated!")
            self.db.estimate_workload_cost_file(self.workload_info[0],
                                                ori_indexes_name=self.workload_used_index)
            if DRIVER:
                self.db.close_conn()
            return None

        print_header_boundary(" Determine optimal indexes ")
        ori_total_cost = \
            self.db.estimate_workload_cost_file(self.workload_info[0],
                                                ori_indexes_name=self.workload_used_index)
        self.index_cost_total.append(ori_total_cost)
        for obj in candidate_indexes:
            new_total_cost = self.db.estimate_workload_cost_file(
                self.workload_info[0], [obj])
            obj.benefit = ori_total_cost - new_total_cost
            self.index_cost_total.append(new_total_cost)
        self.retain_high_benefit_index(candidate_indexes)
        if DRIVER:
            self.db.close_conn()
        if len(self.index_cost_total) == 1:
            print("No optimal indexes generated!")
            return None
        global MAX_INDEX_NUM
        MAX_INDEX_NUM = MAX_INDEX_NUM or 10
        return candidate_indexes

    def filter_low_benefit_index(self, opt_indexes):
        for key, index in enumerate(opt_indexes):
            sql_optimzed = 0
            if self.multi_iter_mode:
                cost_list_pos = index.atomic_pos
            else:
                cost_list_pos = index.cost_pos
            # calculate the average benefit of each positive SQL
            for pos in index.positive_pos:
                sql_optimzed += 1 - self.workload_info[0][pos].cost_list[cost_list_pos] / \
                    self.workload_info[0][pos].cost_list[0]
            negative_sql_ratio = 0
            if index.total_sql_num:
                negative_sql_ratio = (index.insert_sql_num + index.delete_sql_num +
                                      index.update_sql_num) / index.total_sql_num
            # filter the candidate indexes that do not meet the conditions of optimization
            if not index.positive_pos:
                continue
            if sql_optimzed / len(index.positive_pos) < 0.1:
                continue
            if sql_optimzed / len(index.positive_pos) < \
                    NEGATIVE_RATIO_THRESHOLD < negative_sql_ratio:
                continue
            self.determine_indexes.append(index)

    def record_info(self, index, sql_info, cost_list_pos, table_name, statement):
        workload_optimized = (1 - self.index_cost_total[cost_list_pos] /
                              self.index_cost_total[0]) * 100
        sql_info['workloadOptimized'] = '%.2f' % \
                                        (workload_optimized if workload_optimized > 1 else 1)
        sql_info['schemaName'] = index.table.split('.')[0]
        sql_info['tbName'] = table_name
        sql_info['columns'] = index.columns
        sql_info['index_type'] = index.index_type
        sql_info['statement'] = statement
        sql_info['dmlCount'] = round(index.total_sql_num)
        sql_info['selectRatio'] = 1
        sql_info['insertRatio'] = sql_info['deleteRatio'] = sql_info['updateRatio'] = 0
        if index.total_sql_num:
            sql_info['selectRatio'] = round(
                index.select_sql_num * 100 / index.total_sql_num, 2)
            sql_info['insertRatio'] = round(
                index.insert_sql_num * 100 / index.total_sql_num, 2)
            sql_info['deleteRatio'] = round(
                index.delete_sql_num * 100 / index.total_sql_num, 2)
            sql_info['updateRatio'] = round(100 - sql_info['selectRatio'] - sql_info['insertRatio']
                                            - sql_info['deleteRatio'], 2)
        self.display_detail_info['recommendIndexes'].append(sql_info)

    def computer_index_optimization_info(self, index, table_name, statement, opt_indexes):
        if self.multi_iter_mode:
            cost_list_pos = index.atomic_pos
        else:
            cost_list_pos = index.cost_pos
        sql_info = {'sqlDetails': []}
        benefit_types = [index.ineffective_pos,
                         index.positive_pos, index.negative_pos]
        for category, sql_pos in enumerate(benefit_types):
            sql_count = 0
            for item in sql_pos:
                sql_count += self.workload_info[0][item].frequency
            for pos in sql_pos:
                sql_detail = {}
                sql_template = self.workload_info[0][pos].statement
                for pattern in SQL_DISPLAY_PATTERN:
                    sql_template = re.sub(pattern, '?', sql_template)

                sql_detail['sqlTemplate'] = sql_template
                sql_detail['sql'] = self.workload_info[0][pos].statement
                sql_detail['sqlCount'] = int(round(sql_count))
                if category == 1:
                    sql_optimzed = (self.workload_info[0][pos].cost_list[0] -
                                    self.workload_info[0][pos].cost_list[cost_list_pos]) / \
                        self.workload_info[0][pos].cost_list[cost_list_pos]
                    sql_detail['optimized'] = '%.3f' % sql_optimzed
                sql_detail['correlationType'] = category
                sql_info['sqlDetails'].append(sql_detail)
        self.record_info(index, sql_info, cost_list_pos, table_name, statement)

    def display_advise_indexes_info(self, opt_indexes, show_detail):
        index_current_storage = 0
        cnt = 0
        self.display_detail_info['recommendIndexes'] = []
        for key, index in enumerate(self.determine_indexes):
            # constraints for Top-N algorithm
            if MAX_INDEX_STORAGE and (index_current_storage + index.storage) > MAX_INDEX_STORAGE:
                continue
            if MAX_INDEX_NUM and cnt == MAX_INDEX_NUM:
                break
            if not self.multi_iter_mode and index.benefit <= 0:
                continue
            index_current_storage += index.storage
            cnt += 1
            # display determine indexes
            table_name = index.table.split('.')[-1]
            index_name = 'idx_%s_%s%s' % (table_name, (index.index_type
                                                      + '_' if index.index_type else '') \
                                          ,'_'.join(index.columns.split(', ')))
            statement = 'CREATE INDEX %s ON %s%s%s;' % (index_name, index.table,
                                           '(' + index.columns + ')',
                                           (' '+index.index_type if index.index_type else ''))
            print(statement)
            if show_detail:
                # record detailed SQL optimization information for each index
                self.computer_index_optimization_info(
                    index, table_name, statement, opt_indexes)

    def generate_incremental_index(self, history_advise_indexes):
        self.integrate_indexes = copy.copy(history_advise_indexes)
        self.integrate_indexes['currentIndexes'] = {}
        for key, index in enumerate(self.determine_indexes):
            self.integrate_indexes['currentIndexes'][index.table] = \
                self.integrate_indexes['currentIndexes'].get(index.table, [])
            self.integrate_indexes['currentIndexes'][index.table].append(
                (index.columns, index.index_type))

    def generate_redundant_useless_indexes(self, history_invalid_indexes):
        whole_indexes, redundant_indexes = get_whole_index(self.db,
                                                           self.integrate_indexes['historyIndexes'],
                                                           history_invalid_indexes,
                                                           self.display_detail_info)
        display_useless_redundant_indexes(whole_indexes, redundant_indexes,
                                          self.workload_used_index, self.display_detail_info)

    def display_incremental_index(self, history_invalid_indexes,
                                  workload_file_path):
        def rm_schema(table_name):
            return table_name.split('.')[-1]
        # display historical effective indexes
        if self.integrate_indexes['historyIndexes']:
            print_header_boundary(" Historical effective indexes ")
            for table_name, index_list in self.integrate_indexes['historyIndexes'].items():
                for column in index_list:
                    index_name = 'idx_%s_%s%s' % (rm_schema(table_name),
                        (column[1] + '_' if column[1] else ''),
                         '_'.join(column[0].split(', ')))
                    statement = 'CREATE INDEX %s ON %s%s%s;' % (index_name, table_name,
                        '(' + column[0] + ')', (' ' + column[1] if column[1] else ''))
                    print(statement)
        # display historical invalid indexes
        if history_invalid_indexes:
            print_header_boundary(" Historical invalid indexes ")
            for table_name, index_list in history_invalid_indexes.items():
                for column in index_list:
                    index_name = 'idx_%s_%s%s' % (rm_schema(table_name),
                        (column[1] + '_' if column[1] else ''),
                        '_'.join(column[0].split(', ')))
                    statement = 'CREATE INDEX %s ON %s%s%s;' % (index_name, table_name,
                        '(' + column[0] + ')', (' ' + column[1] if column[1] else ''))
                    print(statement)
        # save integrate indexes result
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


def load_workload(file_path):
    wd_dict = {}
    workload = []
    global BLANK
    with open(file_path, 'r') as file:
        raw_text = ''.join(file.readlines())
        sqls = raw_text.split(';')
        for sql in sqls:
            if any(re.search(r'((\A|[\s\(,])%s[\s*\(])' % tp, sql.lower()) for tp in SQL_TYPE):
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
            compressed_workload.append(
                QueryItem(sql.strip(), elem['cnt'] / len(elem['samples'])))
        total_num += elem['cnt']
    return compressed_workload, total_num


# enumerate the column combinations for a suggested index
def get_indexable_columns(table_index_dict):
    query_indexable_columns = {}
    if len(table_index_dict) == 0:
        return query_indexable_columns

    for table in table_index_dict.keys():
        query_indexable_columns[table] = []
        for columns_tuple in table_index_dict[table]:
            indexable_columns = columns_tuple[0].split(',')
            for column in indexable_columns:
                for ind, item in enumerate(query_indexable_columns[table]):
                    if column != item[0]:
                        continue
                    if columns_tuple[1] == item[1]:
                        query_indexable_columns[table].pop(ind)
                        break
                query_indexable_columns[table].append(
                    (column, columns_tuple[1]))

    return query_indexable_columns


def add_column(valid_index_dict, table, columns_info, single_col_info):
    columns, columns_index_type = columns_info
    single_column, single_index_type = single_col_info
    if columns_index_type.strip('"') != single_index_type.strip('"'):
        add_column(valid_index_dict, table, (columns, 'local'),
                   (single_column, 'local'))
        add_column(valid_index_dict, table, (columns, 'global'),
                   (single_column, 'global'))
    else:
        current_columns_tuple = (
            columns + ',' + single_column, columns_index_type)
        if current_columns_tuple in valid_index_dict[table]:
            return
        if single_index_type == 'local':
            global_columns_tuple = (columns + ',' + single_column, 'global')
            if global_columns_tuple in valid_index_dict[table]:
                global_pos = valid_index_dict[table].index(
                    global_columns_tuple)
                valid_index_dict[table][global_pos] = current_columns_tuple
                current_columns_tuple = global_columns_tuple
        valid_index_dict[table].append(current_columns_tuple)


def get_valid_index_dict(table_index_dict, query, db):
    need_check = False
    query_indexable_columns = get_indexable_columns(table_index_dict)
    valid_index_dict = db.query_index_check(query.statement, query_indexable_columns,
                                            ENABLE_MULTI_NODE)

    for i in range(MAX_INDEX_COLUMN_NUM):
        for table in valid_index_dict.keys():
            for columns, index_type in valid_index_dict[table]:
                if columns.count(',') != i:
                    continue
                need_check = True
                for single_column, single_index_type in query_indexable_columns[table]:
                    if single_column not in columns:
                        add_column(valid_index_dict, table, (columns, index_type),
                                   (single_column, single_index_type))
        if need_check:
            valid_index_dict = db.query_index_check(query.statement, valid_index_dict,
                                                    ENABLE_MULTI_NODE)
            need_check = False
        else:
            break
    return valid_index_dict


def print_candidate_indexes(column_sqls, table, candidate_indexes):
    if column_sqls[0][1]:
        print("table: ", table, "columns: ",column_sqls[0][0],
              "type: ", column_sqls[0][1])
    else:
        print("table: ", table, "columns: ",column_sqls[0][0])
    if (table, tuple(column_sqls[0][0]), column_sqls[0][1]) not in IndexItemFactory().indexes:
        index = IndexItemFactory().get_index(table, column_sqls[0][0], 'local')
        index.index_type = 'global'
    else:
        index = IndexItemFactory().get_index(table, column_sqls[0][0], column_sqls[0][1])
    index.is_candidate = True
    candidate_indexes.append(index)


def filter_redundant_indexes(index_dict):
    candidate_indexes = []
    for table, column_sqls in index_dict.items():
        # sorted using index_type and columns
        sorted_column_sqls = sorted(
            column_sqls.items(), key=lambda item: (item[0][1], item[0][0]))
        merged_column_sqls = []
        # merge sqls 
        for i in range(len(sorted_column_sqls) - 1):
            if re.match(sorted_column_sqls[i][0][0] + ',', sorted_column_sqls[i+1][0][0]) and \
                    sorted_column_sqls[i][0][1] == sorted_column_sqls[i+1][0][1]:
                sorted_column_sqls[i+1][1].extend(sorted_column_sqls[i][1])
            else:
                merged_column_sqls.append(sorted_column_sqls[i])
        else:
            merged_column_sqls.append(sorted_column_sqls[-1])
        # sort using columns
        merged_column_sqls.sort(key=lambda item: item[0][0])
        for i in range(len(merged_column_sqls)-1):
            # same columns 
            if merged_column_sqls[i][0][0] == \
                merged_column_sqls[i+1][0][0]:
                print_candidate_indexes(merged_column_sqls[i],
                                       table,
                                       candidate_indexes)
                continue
            # left match for the partation table
            if re.match(merged_column_sqls[i][0][0] + ',',
                        merged_column_sqls[i+1][0][0]):
                merged_column_sqls[i+1][1].extend(
                   merged_column_sqls[i][1])
                merged_column_sqls[i+1] = ((merged_column_sqls[i+1][0][0], 'global'),
                                           merged_column_sqls[i+1][1])
                continue
            print_candidate_indexes(merged_column_sqls[i], table, candidate_indexes)
        else:
            print_candidate_indexes(merged_column_sqls[-1], table, candidate_indexes)
    return candidate_indexes


def filter_duplicate_indexes(valid_index_dict, index_dict, workload, pos):
    for table in valid_index_dict.keys():
        if table not in index_dict.keys():
            index_dict[table] = {}
        valid_index_dict[table].sort(key=lambda x: -len(x[0]))
        for columns, index_type in valid_index_dict[table]:
            if len(workload[pos].valid_index_list) >= FULL_ARRANGEMENT_THRESHOLD:
                break
            if (columns, index_type) in index_dict[table]:
                index_dict[table][(columns, index_type)].append(pos)
            else:
                column_sql = {(columns, index_type): [pos]}
                index_dict[table].update(column_sql)
            workload[pos].valid_index_list.append(
                    IndexItemFactory().get_index(table, columns, index_type=index_type))


def generate_candidate_indexes(workload, db):
    index_dict = {}
    if DRIVER:
        db.init_conn_handle()
    for k, query in enumerate(workload):
        if not re.search(r'(\A|\s)select\s', query.statement.lower()):
            continue
        table_index_dict = db.query_index_advisor(query.statement)
        valid_index_dict = get_valid_index_dict(table_index_dict, query, db)
        # filter duplicate indexes
        filter_duplicate_indexes(valid_index_dict, index_dict, workload, k)

    # filter redundant indexes
    candidate_indexes = filter_redundant_indexes(index_dict)
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

    get_atomic_config_for_query(
        indexes, copy.copy(config), ind + 1, atomic_configs)
    config.append(indexes[ind])
    get_atomic_config_for_query(
        indexes, copy.copy(config), ind + 1, atomic_configs)


def is_same_config(config1, config2):
    if len(config1) != len(config2):
        return False

    for index1 in config1:
        is_found = False
        for index2 in config2:
            if index1.table == index2.table and index1.columns == index2.columns \
                    and index1.index_type == index2.index_type:
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
        atomic_config = []
        get_atomic_config_for_query(
            query.valid_index_list, atomic_config, 0, atomic_configs)

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


def atomic_config_is_valid(atomic_config, config):
    is_exist = False
    is_same = False
    if len(atomic_config) == 1:
        is_same = (config[-1] is atomic_config[0])
    for atomic_index in atomic_config:
        is_exist = False
        for index in config:
            if index is atomic_index:
                index.storage = atomic_index.storage
                is_exist = True
                break
        if not is_exist:
            break
    return is_exist, is_same


# find the subsets of a given config in the atomic configs
def find_subsets_num(config, atomic_config_total):
    atomic_subsets_num = []
    cur_index_atomic_pos = -1
    for i, atomic_config in enumerate(atomic_config_total):
        if len(atomic_config) > len(config):
            continue
        is_exist, is_same = atomic_config_is_valid(atomic_config, config)
        if is_same:
            cur_index_atomic_pos = i
        if is_exist:
            atomic_subsets_num.append(i)
    if cur_index_atomic_pos == -1:
        raise ValueError("No atomic configs found for current config!")
    return atomic_subsets_num, cur_index_atomic_pos


# infer the total cost of workload for a config according to the cost of atomic configs
def infer_workload_cost(workload, config, atomic_config_total):
    total_cost = 0
    is_computed = False
    atomic_subsets_num, cur_index_atomic_pos = find_subsets_num(
        config, atomic_config_total)
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
            ExecuteFactory.record_ineffective_negative_sql(
                config[-1], obj, ind)
    return total_cost, cur_index_atomic_pos


def get_whole_index(db, history_indexes, history_invalid_index, detail_info):
    if DRIVER:
        db.init_conn_handle()
    whole_index, redundant_indexes = \
        db.check_useless_index(history_indexes, history_invalid_index)
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


def display_redundant_indexes(redundant_indexes, unused_index_columns, remove_list, detail_info):
    for pos, index in enumerate(redundant_indexes):
        is_redundant = False
        for redundant_obj in index.redundant_obj:
            # redundant objects are not in the useless index set or
            # both redundant objects and redundant index in useless index must be redundant index
            index_exist = redundant_obj.indexname not in unused_index_columns.keys() or \
                (unused_index_columns.get(redundant_obj.indexname) and
                 unused_index_columns.get(index.indexname))
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
        existing_index = [item.indexname + ':' +
                          item.columns for item in index.redundant_obj]
        redundant_index = {"schemaName": index.schema, "tbName": index.table, "type": 2,
                           "columns": index.columns, "statement": statement,
                           "existingIndex": existing_index}
        detail_info['uselessIndexes'].append(redundant_index)


def display_useless_redundant_indexes(whole_indexes, redundant_indexes,
                                      workload_indexes, detail_info):
    indexes_name = set(index.indexname for index in whole_indexes)
    unused_index = list(indexes_name.difference(workload_indexes))
    remove_list = []
    print_header_boundary(" Current workload useless indexes ")
    detail_info['uselessIndexes'] = []
    # useless index not contain unique index
    unused_index_columns = {}
    has_unused_index = False
    for cur_index in unused_index:
        for index in whole_indexes:
            if cur_index != index.indexname:
                continue
            # get useless index details from whole index
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
    display_redundant_indexes(
        redundant_indexes, unused_index_columns, remove_list, detail_info)


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


def get_last_indexes_result(input_path):
    last_indexes_result_file = os.path.join(os.path.realpath(
        os.path.dirname(input_path)), 'index_result.json')
    integrate_indexes = {'historyIndexes': {}}
    if os.path.exists(last_indexes_result_file):
        try:
            with open(last_indexes_result_file, 'r') as file:
                integrate_indexes['historyIndexes'] = json.load(file)
        except json.JSONDecodeError:
            return integrate_indexes
    return integrate_indexes


def index_advisor_workload(history_advise_indexes, db, workload_file_path,
                           multi_iter_mode, show_detail):
    workload_info = workload_compression(workload_file_path)
    index_advisor = IndexAdvisor(db, workload_info, multi_iter_mode)
    if multi_iter_mode:
        opt_indexes = index_advisor.complex_index_advisor()
    else:
        opt_indexes = index_advisor.simple_index_advisor()
    if opt_indexes:
        index_advisor.filter_low_benefit_index(opt_indexes)
        index_advisor.display_advise_indexes_info(opt_indexes, show_detail)

    index_advisor.generate_incremental_index(history_advise_indexes)
    history_invalid_indexes = {}
    index_advisor.generate_redundant_useless_indexes(history_invalid_indexes)
    index_advisor.display_incremental_index(
        history_invalid_indexes, workload_file_path)
    if show_detail:
        print_header_boundary(" Display detail information ")
        sql_info = json.dumps(
            index_advisor.display_detail_info, indent=4, separators=(',', ':'))
        print(sql_info)


def check_parameter(args):
    global MAX_INDEX_NUM, ENABLE_MULTI_NODE, MAX_INDEX_STORAGE, JSON_TYPE, DRIVER
    if args.max_index_num is not None and args.max_index_num <= 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" %
                                         args.max_index_num)
    if args.max_index_storage is not None and args.max_index_storage <= 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" %
                                         args.max_index_storage)
    JSON_TYPE = args.json
    MAX_INDEX_NUM = args.max_index_num
    ENABLE_MULTI_NODE = args.multi_node
    MAX_INDEX_STORAGE = args.max_index_storage
    # check if the password contains illegal characters
    is_legal = re.search(r'^[A-Za-z0-9~!@#%^*\-_=+?,.]+$', args.W)
    if not is_legal:
        raise ValueError("The password contains illegal characters.")


def main(argv):
    arg_parser = argparse.ArgumentParser(
        description='Generate index set for workload.')
    arg_parser.add_argument("p", help="Port of database", type=int)
    arg_parser.add_argument("d", help="Name of database", action=CheckValid)
    arg_parser.add_argument(
        "--h", help="Host for database",  action=CheckValid)
    arg_parser.add_argument(
        "-U", help="Username for database log-in", action=CheckValid)
    arg_parser.add_argument(
        "f", help="File containing workload queries (One query per line)", action=CheckValid)
    arg_parser.add_argument("--schema", help="Schema name for the current business data",
                            required=True, action=CheckValid)
    arg_parser.add_argument(
        "--max_index_num", help="Maximum number of suggested indexes", type=int)
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
    args = arg_parser.parse_args(argv)

    args.W = get_password()
    check_parameter(args)
    # Initialize the connection
    global DRIVER
    if args.driver:
        try:
            import psycopg2
            try:
                from .dao.driver_execute import DriverExecute
            except ImportError:
                from dao.driver_execute import DriverExecute
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
    if args.multi_node and not db.is_multi_node():
        raise argparse.ArgumentTypeError('--multi_node is only support for distributed database')
    index_advisor_workload(get_last_indexes_result(args.f), db, args.f,
                           args.multi_iter_mode, args.show_detail)


if __name__ == '__main__':
    main(sys.argv[1:])
