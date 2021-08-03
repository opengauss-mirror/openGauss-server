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
import select
import logging
import psycopg2
import json

ENABLE_MULTI_NODE = False
SAMPLE_NUM = 5
MAX_INDEX_COLUMN_NUM = 5
MAX_INDEX_NUM = 10
MAX_INDEX_STORAGE = None
FULL_ARRANGEMENT_THRESHOLD = 20
NEGATIVE_RATIO_THRESHOLD = 0.2
BASE_CMD = ''
SHARP = '#'
SCHEMA = None
JSON_TYPE = False
BLANK = ' '
SQL_TYPE = ['select', 'delete', 'insert', 'update']
SQL_PATTERN = [r'\((\s*(\d+(\.\d+)?\s*)[,]?)+\)',  # match integer set in the IN collection
               r'([^\\])\'((\')|(.*?([^\\])\'))',  # match all content in single quotes
               r'(([^<>]\s*=\s*)|([^<>]\s+))(\d+)(\.\d+)?']  # match single integer
SQL_DISPLAY_PATTERN = [r'\((\s*(\d+(\.\d+)?\s*)[,]?)+\)',  # match integer set in the IN collection
                       r'\'((\')|(.*?\'))',  # match all content in single quotes
                       r'([^\_\d])\d+(\.\d+)?']  # match single integer

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


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
    def __init__(self, tbl, cols, positive_pos=None):
        self.table = tbl
        self.columns = cols
        self.atomic_pos = 0
        self.benefit = 0
        self.storage = 0
        self.positive_pos = positive_pos
        self.ineffective_pos = []
        self.negative_pos = []
        self.total_sql_num = 0
        self.insert_sql_num = 0
        self.update_sql_num = 0
        self.delete_sql_num = 0
        self.select_sql_num = 0


class IndexInfo:
    def __init__(self, schema, table, indexname, columns, indexdef):
        self.schema = schema
        self.table = table
        self.indexname = indexname
        self.columns = columns
        self.indexdef = indexdef
        self.primary_key = False
        self.redundant_obj = []


class DatabaseConn:
    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None
        self.cur = None

    def init_conn_handle(self):
        self.conn = psycopg2.connect(dbname=self.dbname,
                                     user=self.user,
                                     password=self.password,
                                     host=self.host,
                                     port=self.port)
        self.cur = self.conn.cursor()

    def execute(self, sql):
        try:
            self.cur.execute(sql)
            self.conn.commit()
            return self.cur.fetchall()
        except Exception:
            self.conn.commit()

    def close_conn(self):
        if self.conn and self.cur:
            self.cur.close()
            self.conn.close()


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


def filter_low_benefit(pos_list, candidate_indexes, multi_iter_mode, workload):
    remove_list = []
    for key, index in enumerate(candidate_indexes):
        sql_optimzed = 0
        for ind, pos in enumerate(index.positive_pos):
            if multi_iter_mode:
                cost_list_pos = index.atomic_pos
            else:
                cost_list_pos = pos_list[key] + 1
            sql_optimzed += 1 - workload[pos].cost_list[cost_list_pos] / workload[pos].cost_list[0]
        negative_ratio = (index.insert_sql_num + index.delete_sql_num + index.update_sql_num) / \
                         index.total_sql_num
        # filter the candidate indexes that do not meet the conditions of optimization
        if sql_optimzed / len(index.positive_pos) < 0.1:
            remove_list.append(key)
        elif sql_optimzed / len(index.positive_pos) < NEGATIVE_RATIO_THRESHOLD < negative_ratio:
            remove_list.append(key)
    for item in sorted(remove_list, reverse=True):
        candidate_indexes.pop(item)


def display_recommend_result(workload, candidate_indexes, index_cost_total,
                             multi_iter_mode, display_info):
    cnt = 0
    index_current_storage = 0
    pos_list = []
    if not multi_iter_mode:
        pos_list = [item[0] for item in candidate_indexes]
        candidate_indexes = [item[1] for item in candidate_indexes]
    # filter candidate indexes with low benefit
    filter_low_benefit(pos_list, candidate_indexes, multi_iter_mode, workload)
    # display determine result
    for key, index in enumerate(candidate_indexes):
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
            cost_list_pos = pos_list[key] + 1

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
        sql_info['schemaName'] = SCHEMA
        sql_info['tbName'] = table_name
        sql_info['columns'] = index.columns
        sql_info['statement'] = statement
        sql_info['dmlCount'] = round(index.total_sql_num)
        sql_info['selectRatio'] = round(index.select_sql_num * 100 / index.total_sql_num, 2)
        sql_info['insertRatio'] = round(index.insert_sql_num * 100 / index.total_sql_num, 2)
        sql_info['deleteRatio'] = round(index.delete_sql_num * 100 / index.total_sql_num, 2)
        sql_info['updateRatio'] = round(100 - sql_info['selectRatio'] - sql_info['insertRatio']
                                        - sql_info['deleteRatio'], 2)
        display_info['recommendIndexes'].append(sql_info)
    return display_info


def record_redundant_indexes(cur_table_indexes, redundant_indexes):
    cur_table_indexes = sorted(cur_table_indexes,
                               key=lambda index_obj: len(index_obj.columns.split(',')))
    # record redundant indexes
    has_restore = []
    for pos, index in enumerate(cur_table_indexes[:-1]):
        is_redundant = False
        for candidate_index in cur_table_indexes[pos + 1:]:
            if 'UNIQUE INDEX' in index.indexdef:
                # ensure that UNIQUE INDEX will not become redundant compared to normal index
                if 'UNIQUE INDEX' not in candidate_index.indexdef:
                    continue
                # ensure redundant index not is pkey
                elif index.primary_key:
                    if re.match(r'%s' % candidate_index.columns, index.columns):
                        candidate_index.redundant_obj.append(index)
                        redundant_indexes.append(candidate_index)
                        has_restore.append(candidate_index)
                    continue
            if re.match(r'%s' % index.columns, candidate_index.columns):
                is_redundant = True
                index.redundant_obj.append(candidate_index)
        if is_redundant and index not in has_restore:
            redundant_indexes.append(index)


def check_useless_index(tables, db):
    whole_indexes = list()
    redundant_indexes = list()
    if not tables:
        return whole_indexes, redundant_indexes
    tables_string = ','.join(["'%s'" % table for table in tables[SCHEMA]])
    sql = "SELECT c.relname AS tablename, i.relname AS indexname, " \
          "pg_get_indexdef(i.oid) AS indexdef, p.contype AS pkey from " \
          "pg_index x JOIN pg_class c ON c.oid = x.indrelid JOIN " \
          "pg_class i ON i.oid = x.indexrelid LEFT JOIN pg_namespace n " \
          "ON n.oid = c.relnamespace LEFT JOIN pg_constraint p ON i.oid = p.conindid " \
          "WHERE (c.relkind = ANY (ARRAY['r'::\"char\", 'm'::\"char\"])) AND " \
          "(i.relkind = ANY (ARRAY['i'::\"char\", 'I'::\"char\"])) AND " \
          "n.nspname = '%s' AND c.relname in (%s) order by c.relname;" % (SCHEMA, tables_string)

    res = db.execute(sql)
    if res:
        cur_table_indexes = list()
        for item in res:
            cur_columns = re.search(r'\(([^\(\)]*)\)', item[2]).group(1)
            cur_index_obj = IndexInfo(SCHEMA, item[0], item[1], cur_columns, item[2])
            if item[3]:
                cur_index_obj.primary_key = True
            whole_indexes.append(cur_index_obj)
            if cur_table_indexes and cur_table_indexes[-1].table != item[0]:
                record_redundant_indexes(cur_table_indexes, redundant_indexes)
                cur_table_indexes = []
            cur_table_indexes.append(cur_index_obj)
        if cur_table_indexes:
            record_redundant_indexes(cur_table_indexes, redundant_indexes)

    return whole_indexes, redundant_indexes


def get_whole_index(tables, db, detail_info):
    db.init_conn_handle()
    whole_index, redundant_indexes = check_useless_index(tables, db)
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
            print("%s;" % index.indexdef)
    return whole_index, redundant_indexes


def check_unused_index_workload(whole_indexes, redundant_indexes, workload_indexes, detail_info):
    indexes_name = set(index.indexname for index in whole_indexes)
    unused_index = list(indexes_name.difference(workload_indexes))
    remove_list = []
    print_header_boundary(" Current workload useless indexes ")
    if not unused_index:
        print("No useless index!")
    detail_info['uselessIndexes'] = []
    # useless index
    unused_index_columns = dict()
    for cur_index in unused_index:
        for index in whole_indexes:
            if cur_index == index.indexname:
                unused_index_columns[cur_index] = index.columns
                if 'UNIQUE INDEX' not in index.indexdef:
                    statement = "DROP INDEX %s;" % index.indexname
                    print(statement)
                    useless_index = {"schemaName": index.schema, "tbName": index.table, "type": 3,
                                     "columns": index.columns, "statement": statement}
                    detail_info['uselessIndexes'].append(useless_index)
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
        statement = "DROP INDEX %s;" % index.indexname
        print(statement)
        existing_index = [item.indexname + ':' + item.columns for item in index.redundant_obj]
        redundant_index = {"schemaName": index.schema, "tbName": index.table, "type": 2,
                           "columns": index.columns, "statement": statement, "existingIndex": existing_index}
        detail_info['uselessIndexes'].append(redundant_index)


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
    total_num = 0
    compressed_workload = []
    if JSON_TYPE:
        with open(input_path, 'r') as file:
            templates = json.load(file)
    else:
        workload = load_workload(input_path)
        templates = get_workload_template(workload)

    for _, elem in templates.items():
        for sql in elem['samples']:
            compressed_workload.append(QueryItem(sql.strip('\n'),
                                                 elem['cnt'] / len(elem['samples'])))
        total_num += elem['cnt']
    return compressed_workload, total_num


# parse the explain plan to get estimated cost by database optimizer
def parse_explain_plan(plan, index_config, ori_indexes_name):
    cost_total = -1
    cost_flag = True
    for item in plan:
        if '(cost=' in item[0] and cost_flag:
            cost_flag = False
            pattern = re.compile(r'\(cost=([^\)]*)\)', re.S)
            matched_res = re.search(pattern, item[0])
            if matched_res:
                cost_list = matched_res.group(1).split()
                if len(cost_list) == 3:
                    cost_total = float(cost_list[0].split('..')[-1])
        if 'Index' in item[0] and 'Scan' in item[0] and not index_config:
            ind1, ind2 = re.search(r'Index.*Scan(.*)on ([^\s]+)',
                                   item[0].strip(), re.IGNORECASE).groups()
            if ind1.strip():
                ori_indexes_name.add(ind1.strip().split(' ')[1])
            else:
                ori_indexes_name.add(ind2)
            break

    return cost_total


def update_index_storage(index_id, index_config, hypo_index_num, db):
    index_size_sql = 'select * from hypopg_estimate_size(%s);' % index_id
    res = db.execute(index_size_sql)
    if res:
        index_config[hypo_index_num].storage = float(res[0][0]) / 1024 / 1024


def estimate_workload_cost_file(workload, db, index_config=None, ori_indexes_name=None):
    total_cost = 0
    hypo_index_num = 0
    is_computed = False
    db.execute('SET current_schema = %s' % SCHEMA)
    if index_config:
        if len(index_config) == 1 and index_config[0].positive_pos:
            is_computed = True
        # create hypo-indexes
        db.execute('SET enable_hypo_index = on')
        for index in index_config:
            res = db.execute("SELECT * from hypopg_create_index('CREATE INDEX ON %s(%s)')" %
                             (index.table, index.columns))
            if MAX_INDEX_STORAGE and res:
                update_index_storage(res[0][0], index_config, hypo_index_num, db)
            hypo_index_num += 1
    if ENABLE_MULTI_NODE:
        db.execute('set enable_fast_query_shipping = off;set enable_stream_operator = on')
    db.execute("set explain_perf_mode = 'normal'")

    remove_list = []
    for ind, query in enumerate(workload):
        # record ineffective sql and negative sql for candidate indexes
        if is_computed:
            record_ineffective_negative_sql(index_config[0], query, ind)
        if 'select ' not in query.statement.lower():
            workload[ind].cost_list.append(0)
        else:
            res = db.execute('EXPLAIN ' + query.statement)
            if res:
                query_cost = parse_explain_plan(res, index_config, ori_indexes_name)
                query_cost *= workload[ind].frequency
                workload[ind].cost_list.append(query_cost)
                total_cost += query_cost
    if index_config:
        db.execute('SELECT hypopg_reset_index()')
    return total_cost


def make_single_advisor_sql(ori_sql):
    sql = 'set current_schema = %s; select gs_index_advise(\'' % SCHEMA
    ori_sql = ori_sql.replace('"', '\'')
    for elem in ori_sql:
        if elem == '\'':
            sql += '\''
        sql += elem
    sql += '\');'

    return sql


def parse_single_advisor_result(res, workload_table_name):
    table_index_dict = {}
    items = res.strip('()').split(',', 1)
    if len(items) == 2:
        table = items[0]
        workload_table_name[SCHEMA] = workload_table_name.get(SCHEMA, set())
        workload_table_name[SCHEMA].add(table)
        indexes = re.split('[()]', items[1].strip('\"'))
        for columns in indexes:
            if columns == '':
                continue
            if table not in table_index_dict.keys():
                table_index_dict[table] = []
            table_index_dict[table].append(columns)

    return table_index_dict


# call the single-index-advisor in the database
def query_index_advisor(query, workload_table_name, db):
    table_index_dict = {}

    if 'select' not in query.lower():
        return table_index_dict

    sql = make_single_advisor_sql(query)
    result = db.execute(sql=sql)
    if not result:
        return table_index_dict
    for res in result:
        table_index_dict.update(parse_single_advisor_result(res[0], workload_table_name))

    return table_index_dict


# judge whether the index is used by the optimizer
def query_index_check(query, query_index_dict, db):
    valid_indexes = {}
    if len(query_index_dict) == 0:
        return valid_indexes

    # create hypo-indexes
    sqls = 'SET enable_hypo_index = on;'
    if ENABLE_MULTI_NODE:
        sqls += 'SET enable_fast_query_shipping = off;SET enable_stream_operator = on;'
    for table in query_index_dict.keys():
        for columns in query_index_dict[table]:
            if columns != '':
                sqls += "SELECT hypopg_create_index('CREATE INDEX ON %s(%s)');" % \
                            (table, columns)
    sqls += 'SELECT * from hypopg_display_index();'
    result = db.execute(sqls)
    if not result:
        return valid_indexes
    hypoid_table_column = {}
    for item in result:
        if len(item) == 4:
            hypoid_table_column[str(item[1])] = item[2] + ':' + item[3].strip('()')
    sqls = "SET explain_perf_mode = 'normal'; explain %s" % query
    result = db.execute(sqls)
    if not result:
        return valid_indexes
    # parse the result of explain plan
    for item in result:
        if 'Index' in item[0] and 'Scan' in item[0] and 'btree' in item[0]:
            tokens = item[0].split(' ')
            for token in tokens:
                if 'btree' in token:
                    hypo_index_id = re.search(r'\d+', token.split('_', 1)[0]).group()
                    table_columns = hypoid_table_column.get(hypo_index_id)
                    if not table_columns:
                        continue
                    table_name, columns = table_columns.split(':')
                    if table_name not in valid_indexes.keys():
                        valid_indexes[table_name] = []
                    if columns not in valid_indexes[table_name]:
                        valid_indexes[table_name].append(columns)
    db.execute('SELECT hypopg_reset_index()')
    return valid_indexes


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


def generate_candidate_indexes(workload, workload_table_name, db):
    candidate_indexes = []
    index_dict = {}
    db.init_conn_handle()
    for k, query in enumerate(workload):
        if 'select ' in query.statement.lower():
            table_index_dict = query_index_advisor(query.statement, workload_table_name, db)
            need_check = False
            query_indexable_columns = get_indexable_columns(table_index_dict)
            valid_index_dict = query_index_check(query.statement, query_indexable_columns, db)

            for i in range(MAX_INDEX_COLUMN_NUM):
                for table in valid_index_dict.keys():
                    for columns in valid_index_dict[table]:
                        if columns.count(',') == i:
                            need_check = True
                            for single_column in query_indexable_columns[table]:
                                if single_column not in columns:
                                    valid_index_dict[table].append(columns + ',' + single_column)
                if need_check:
                    valid_index_dict = query_index_check(query.statement, valid_index_dict, db)
                    need_check = False
                else:
                    break

            # filter duplicate indexes
            for table in valid_index_dict.keys():
                if table not in index_dict.keys():
                    index_dict[table] = {}
                for columns in valid_index_dict[table]:
                    if len(workload[k].valid_index_list) >= FULL_ARRANGEMENT_THRESHOLD:
                        break
                    workload[k].valid_index_list.append(IndexItem(table, columns))
                    if not any(re.match(r'%s' % columns, item) for item in index_dict[table]):
                        column_sql = {columns: [k]}
                        index_dict[table].update(column_sql)
                    elif columns in index_dict[table].keys():
                        index_dict[table][columns].append(k)
    for table, column_sqls in index_dict.items():
        for column, sql in column_sqls.items():
            print("table: ", table, "columns: ", column)
            candidate_indexes.append(IndexItem(table, column, sql))
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

    return atomic_subsets_num, cur_index_atomic_pos


def get_index_num(index, atomic_config_total):
    for i, atomic_config in enumerate(atomic_config_total):
        if len(atomic_config) == 1 and atomic_config[0].table == index.table and \
                atomic_config[0].columns == index.columns:
            return i

    return -1


def record_ineffective_negative_sql(candidate_index, obj, ind):
    cur_table = candidate_index.table + ' '
    if cur_table in obj.statement:
        candidate_index.total_sql_num += obj.frequency
        if 'insert ' in obj.statement.lower():
            candidate_index.insert_sql_num += obj.frequency
            candidate_index.negative_pos.append(ind)
        elif 'delete ' in obj.statement.lower():
            candidate_index.delete_sql_num += obj.frequency
            candidate_index.negative_pos.append(ind)
        elif 'update ' in obj.statement.lower():
            candidate_index.update_sql_num += obj.frequency
            if any(column in obj.statement.lower().split('where ', 1)[0] for column in
                    candidate_index.columns.split(',')):
                candidate_index.negative_pos.append(ind)
            else:
                candidate_index.ineffective_pos.append(ind)
        else:
            candidate_index.select_sql_num += obj.frequency
            if ind not in candidate_index.positive_pos and \
                    any(column in obj.statement.lower() for column in candidate_index.columns):
                candidate_index.ineffective_pos.append(ind)


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
            record_ineffective_negative_sql(config[-1], obj, ind)
    return total_cost, cur_index_atomic_pos


def simple_index_advisor(input_path, max_index_num, db):
    workload, workload_count = workload_compression(input_path)
    print_header_boundary(" Generate candidate indexes ")
    ori_indexes_name = set()
    workload_table_name = dict()
    display_info = {'workloadCount': workload_count, 'recommendIndexes': []}
    candidate_indexes = generate_candidate_indexes(workload, workload_table_name, db)
    db.init_conn_handle()
    if len(candidate_indexes) == 0:
        print("No candidate indexes generated!")
        estimate_workload_cost_file(workload, db, ori_indexes_name=ori_indexes_name)
        return ori_indexes_name, workload_table_name, display_info

    print_header_boundary(" Determine optimal indexes ")
    ori_total_cost = estimate_workload_cost_file(workload, db, ori_indexes_name=ori_indexes_name)
    index_cost_total = [ori_total_cost]
    for _, obj in enumerate(candidate_indexes):
        new_total_cost = estimate_workload_cost_file(workload, db, [obj])
        index_cost_total.append(new_total_cost)
        obj.benefit = ori_total_cost - new_total_cost
    db.close_conn()
    candidate_indexes = sorted(enumerate(candidate_indexes),
                               key=lambda item: item[1].benefit, reverse=True)
    candidate_indexes = [item for item in candidate_indexes if item[1].benefit > 0]
    global MAX_INDEX_NUM
    MAX_INDEX_NUM = max_index_num

    display_recommend_result(workload, candidate_indexes, index_cost_total, False, display_info)
    return ori_indexes_name, workload_table_name, display_info


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
                continue
            if len(opt_config) == MAX_INDEX_NUM:
                break
            min_cost = cur_min_cost
            opt_config.append(cur_index)
            index_num_record.add(cur_index_num)
        else:
            break

    return opt_config


def complex_index_advisor(input_path, db):
    workload, workload_count = workload_compression(input_path)
    print_header_boundary(" Generate candidate indexes ")
    ori_indexes_name = set()
    workload_table_name = dict()
    display_info = {'workloadCount': workload_count, 'recommendIndexes': []}
    candidate_indexes = generate_candidate_indexes(workload, workload_table_name, db)
    db.init_conn_handle()
    if len(candidate_indexes) == 0:
        print("No candidate indexes generated!")
        estimate_workload_cost_file(workload, db, ori_indexes_name=ori_indexes_name)
        return ori_indexes_name, workload_table_name, display_info

    print_header_boundary(" Determine optimal indexes ")
    atomic_config_total = generate_atomic_config(workload)
    if atomic_config_total and len(atomic_config_total[0]) != 0:
        raise ValueError("The empty atomic config isn't generated!")
    index_cost_total = []
    for atomic_config in atomic_config_total:
        index_cost_total.append(estimate_workload_cost_file(workload, db, atomic_config,
                                                            ori_indexes_name))
    db.close_conn()
    opt_config = greedy_determine_opt_config(workload, atomic_config_total,
                                                             candidate_indexes, index_cost_total[0])

    display_recommend_result(workload, opt_config, index_cost_total, True, display_info)
    return ori_indexes_name, workload_table_name, display_info


def main():
    arg_parser = argparse.ArgumentParser(description='Generate index set for workload.')
    arg_parser.add_argument("p", help="Port of database")
    arg_parser.add_argument("d", help="Name of database")
    arg_parser.add_argument("--h", help="Host for database")
    arg_parser.add_argument("-U", help="Username for database log-in")
    arg_parser.add_argument("-W", help="Password for database user", nargs="?", action=PwdAction)
    arg_parser.add_argument("f", help="File containing workload queries (One query per line)")
    arg_parser.add_argument("--schema", help="Schema name for the current business data", required=True)
    arg_parser.add_argument("--max_index_num", help="Maximum number of suggested indexes", type=int)
    arg_parser.add_argument("--max_index_storage",
                            help="Maximum storage of suggested indexes/MB", type=int)
    arg_parser.add_argument("--multi_iter_mode", action='store_true',
                            help="Whether to use multi-iteration algorithm", default=False)
    arg_parser.add_argument("--multi_node", action='store_true',
                            help="Whether to support distributed scenarios", default=False)
    arg_parser.add_argument("--json", action='store_true',
                            help="Whether the workload file format is json", default=False)
    arg_parser.add_argument("--show_detail", action='store_true',
                            help="Whether to show detailed sql information", default=False)
    args = arg_parser.parse_args()

    global MAX_INDEX_NUM, BASE_CMD, ENABLE_MULTI_NODE, MAX_INDEX_STORAGE, SCHEMA, JSON_TYPE
    if args.max_index_num is not None and args.max_index_num <= 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" %
                                         args.max_index_num)
    if args.max_index_storage is not None and args.max_index_storage <= 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" %
                                         args.max_index_storage)
    SCHEMA = args.schema
    JSON_TYPE = args.json
    MAX_INDEX_NUM = args.max_index_num or 10
    ENABLE_MULTI_NODE = args.multi_node
    MAX_INDEX_STORAGE = args.max_index_storage
    BASE_CMD = 'gsql -p ' + args.p + ' -d ' + args.d
    if args.h:
        BASE_CMD += ' -h ' + args.h
    if args.U:
        BASE_CMD += ' -U ' + args.U
        if args.U != getpass.getuser() and not args.W:
            raise ValueError('Enter the \'-W\' parameter for user '
                             + args.U + ' when executing the script.')
    if args.W:
        BASE_CMD += ' -W ' + args.W
    # Initialize the connection
    db = DatabaseConn(args.d, args.U, args.W, args.h, args.p)

    if args.multi_iter_mode:
        workload_indexes, tables, detail_info = complex_index_advisor(args.f, db)
    else:
        workload_indexes, tables, detail_info = simple_index_advisor(args.f, args.max_index_num, db)

    whole_indexes, redundant_indexes = get_whole_index(tables, db, detail_info)
    # check the unused indexes of the current workload based on the whole index
    check_unused_index_workload(whole_indexes, redundant_indexes, workload_indexes, detail_info)
    if args.show_detail:
        print_header_boundary(" Display detail information ")
        sql_info = json.dumps(detail_info, indent=4, separators=(',', ':'))
        print(sql_info)


if __name__ == '__main__':
    main()


