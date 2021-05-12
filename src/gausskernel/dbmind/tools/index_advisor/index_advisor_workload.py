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
import shlex
import subprocess
import time
import select
import logging

ENABLE_MULTI_NODE = False
SAMPLE_NUM = 5
MAX_INDEX_COLUMN_NUM = 5
MAX_INDEX_NUM = 10
MAX_INDEX_STORAGE = None
FULL_ARRANGEMENT_THRESHOLD = 20
BASE_CMD = ''
SHARP = '#'
SCHEMA = None
BLANK = ' '
SQL_TYPE = ['select', 'delete', 'insert', 'update']
SQL_PATTERN = [r'([^\\])\'((\')|(.*?([^\\])\'))',
               r'([^\\])"((")|(.*?([^\\])"))',
               r'(\s*[<>]\s*=*\s*\d+)',
               r'(\'\d+\\.*?\')']

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
    def __init__(self, tbl, cols):
        self.table = tbl
        self.columns = cols
        self.benefit = 0
        self.storage = 0


def run_shell_cmd(target_sql_list):
    cmd = BASE_CMD + ' -c \"'
    if SCHEMA:
        cmd += 'set current_schema = %s; ' % SCHEMA
    for target_sql in target_sql_list:
        cmd += target_sql + ';'
    cmd += '\"'
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (stdout, stderr) = proc.communicate()
    stdout, stderr = stdout.decode(), stderr.decode()
    if 'gsql' in stderr or 'failed to connect' in stderr:
        raise ConnectionError("An error occurred while connecting to the database.\n"
                              + "Details: " + stderr)
    return stdout


def run_shell_sql_cmd(sql_file):
    cmd = BASE_CMD + ' -f ./' + sql_file
    try:
        ret = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print(e.output, file=sys.stderr)

    return ret.decode()


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

    workload = load_workload(input_path)
    templates = get_workload_template(workload)

    for _, elem in templates.items():
        for sql in elem['samples']:
            compressed_workload.append(QueryItem(sql, elem['cnt'] / SAMPLE_NUM))
    return compressed_workload


# parse the explain plan to get estimated cost by database optimizer
def parse_explain_plan(plan):
    cost_total = -1

    plan_list = plan.split('\n')
    for line in plan_list:
        if '(cost=' in line:
            pattern = re.compile(r'\(cost=([^\)]*)\)', re.S)
            matched_res = re.search(pattern, line)
            if matched_res:
                cost_list = matched_res.group(1).split()
                if len(cost_list) == 3:
                    cost_total = float(cost_list[0].split('..')[-1])
            break

    return cost_total


def update_index_storage(res, index_config, hypo_index_num):
    index_id = re.findall(r'<\d+>', res)[0].strip('<>')
    index_size_sql = 'select * from hypopg_estimate_size(%s);' % index_id
    res = run_shell_cmd([index_size_sql]).split('\n')
    for item in res:
        if re.match(r'\d+', item.strip()):
            index_config[hypo_index_num].storage = float(item.strip()) / 1024 / 1024


def estimate_workload_cost_file(workload, index_config=None):
    total_cost = 0
    sql_file = str(time.time()) + '.sql'
    found_plan = False
    hypo_index = False
    with open(sql_file, 'w') as file:
        if SCHEMA:
            file.write('SET current_schema = %s;\n' % SCHEMA)
        if index_config:
            # create hypo-indexes
            file.write('SET enable_hypo_index = on;\n')
            for index in index_config:
                file.write("SELECT hypopg_create_index('CREATE INDEX ON %s(%s)');\n" %
                         (index.table, index.columns))
        if ENABLE_MULTI_NODE:
            file.write('set enable_fast_query_shipping = off;\n')
        file.write("set explain_perf_mode = 'normal'; \n")
        for query in workload:
            file.write('EXPLAIN ' + query.statement + ';\n')

    result = run_shell_sql_cmd(sql_file).split('\n')
    if os.path.exists(sql_file):
        os.remove(sql_file)

    # parse the result of explain plans
    i = 0
    hypo_index_num = 0
    for line in result:
        if 'QUERY PLAN' in line:
            found_plan = True
        if 'ERROR' in line:
            workload.pop(i)
        if 'hypopg_create_index' in line:
            hypo_index = True
        if found_plan and '(cost=' in line:
            if i >= len(workload):
                raise ValueError("The size of workload is not correct!")
            query_cost = parse_explain_plan(line)
            query_cost *= workload[i].frequency
            workload[i].cost_list.append(query_cost)
            total_cost += query_cost
            found_plan = False
            i += 1
        if hypo_index:
            if 'btree' in line and MAX_INDEX_STORAGE:
                hypo_index = False
                update_index_storage(line, index_config, hypo_index_num)
                hypo_index_num += 1
    while i < len(workload):
        workload[i].cost_list.append(0)
        i += 1
    if index_config:
        run_shell_cmd(['SELECT hypopg_reset_index();'])

    return total_cost


def make_single_advisor_sql(ori_sql):
    sql = 'select gs_index_advise(\''
    ori_sql = ori_sql.replace('"', '\'')
    for elem in ori_sql:
        if elem == '\'':
            sql += '\''
        sql += elem
    sql += '\');'

    return sql


def parse_single_advisor_result(res):
    table_index_dict = {}
    if len(res) > 2 and res[0:2] == ' (':
        items = res.split(',', 1)
        table = items[0][2:]
        indexes = re.split('[()]', items[1][:-1].strip('\"'))
        for columns in indexes:
            if columns == '':
                continue
            if table not in table_index_dict.keys():
                table_index_dict[table] = []
            table_index_dict[table].append(columns)

    return table_index_dict


# call the single-index-advisor in the database
def query_index_advisor(query):
    table_index_dict = {}

    if 'select' not in query.lower():
        return table_index_dict

    sql = make_single_advisor_sql(query)
    result = run_shell_cmd([sql]).split('\n')

    for res in result:
        table_index_dict.update(parse_single_advisor_result(res))

    return table_index_dict


# judge whether the index is used by the optimizer
def query_index_check(query, query_index_dict):
    valid_indexes = {}
    if len(query_index_dict) == 0:
        return valid_indexes

    # create hypo-indexes
    sql_list = ['SET enable_hypo_index = on;']
    if ENABLE_MULTI_NODE:
        sql_list.append('SET enable_fast_query_shipping = off;')
    for table in query_index_dict.keys():
        for columns in query_index_dict[table]:
            if columns != '':
                sql_list.append("SELECT hypopg_create_index('CREATE INDEX ON %s(%s)')" %
                                (table, columns))
    sql_list.append("set explain_perf_mode = 'normal'; explain " + query)
    sql_list.append('SELECT hypopg_reset_index()')
    result = run_shell_cmd(sql_list).split('\n')

    # parse the result of explain plan
    for line in result:
        hypo_index = ''
        if 'Index' in line and 'Scan' in line and 'btree' in line:
            tokens = line.split(' ')
            for token in tokens:
                if 'btree' in token:
                    hypo_index = token.split('_', 1)[1]
            if len(hypo_index) > 1:
                for table in query_index_dict.keys():
                    for columns in query_index_dict[table]:
                        index_name_list = columns.split(',')
                        index_name_list.insert(0, table)
                        index_name = "_".join(index_name_list)
                        if index_name != hypo_index:
                            continue
                        if table not in valid_indexes.keys():
                            valid_indexes[table] = []
                        if columns not in valid_indexes[table]:
                            valid_indexes[table].append(columns)

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


def generate_candidate_indexes(workload, iterate=False):
    candidate_indexes = []
    index_dict = {}

    for k, query in enumerate(workload):
        table_index_dict = query_index_advisor(query.statement)
        if iterate:
            need_check = False
            query_indexable_columns = get_indexable_columns(table_index_dict)
            valid_index_dict = query_index_check(query.statement, query_indexable_columns)

            for i in range(MAX_INDEX_COLUMN_NUM):
                for table in valid_index_dict.keys():
                    for columns in valid_index_dict[table]:
                        if columns.count(',') == i:
                            need_check = True
                            for single_column in query_indexable_columns[table]:
                                if single_column not in columns:
                                    valid_index_dict[table].append(columns + ',' + single_column)
                if need_check:
                    valid_index_dict = query_index_check(query.statement, valid_index_dict)
                    need_check = False
                else:
                    break
        else:
            valid_index_dict = query_index_check(query.statement, table_index_dict)

        # filter duplicate indexes
        for table in valid_index_dict.keys():
            if table not in index_dict.keys():
                index_dict[table] = set()
            for columns in valid_index_dict[table]:
                if len(workload[k].valid_index_list) >= FULL_ARRANGEMENT_THRESHOLD:
                    break
                workload[k].valid_index_list.append(IndexItem(table, columns))
                if columns not in index_dict[table]:
                    print("table: ", table, "columns: ", columns)
                    index_dict[table].add(columns)
                    candidate_indexes.append(IndexItem(table, columns))

    return candidate_indexes


def generate_candidate_indexes_file(workload):
    candidate_indexes = []
    index_dict = {}
    sql_file = str(time.time()) + '.sql'

    if len(workload) > 0:
        run_shell_cmd([workload[0].statement])
    with open(sql_file, 'w') as file:
        if SCHEMA:
            file.write('SET current_schema = %s;\n' % SCHEMA)
        for query in workload:
            if 'select' in query.statement.lower():
                file.write(make_single_advisor_sql(query.statement) + '\n')

    result = run_shell_sql_cmd(sql_file).split('\n')
    if os.path.exists(sql_file):
        os.remove(sql_file)

    for line in result:
        table_index_dict = parse_single_advisor_result(line.strip('\n'))
        # filter duplicate indexes
        for table, columns in table_index_dict.items():
            if table not in index_dict.keys():
                index_dict[table] = set()
            for column in columns:
                if column == "":
                    continue
                if column not in index_dict[table] and not re.match(r'\s*,\s*$', column):
                    print("table: ", table, "columns: ", column)
                    index_dict[table].add(column)
                    candidate_indexes.append(IndexItem(table, column))

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

    return atomic_subsets_num


def get_index_num(index, atomic_config_total):
    for i, atomic_config in enumerate(atomic_config_total):
        if len(atomic_config) == 1 and atomic_config[0].table == index.table and \
                atomic_config[0].columns == index.columns:
            return i

    return -1


# infer the total cost of workload for a config according to the cost of atomic configs
def infer_workload_cost(workload, config, atomic_config_total):
    total_cost = 0

    atomic_subsets_num = find_subsets_num(config, atomic_config_total)
    if len(atomic_subsets_num) == 0:
        raise ValueError("No atomic configs found for current config!")

    for ind, obj in enumerate(workload):
        if max(atomic_subsets_num) >= len(obj.cost_list):
            raise ValueError("Wrong atomic config for current query!")
        # compute the cost for selection
        min_cost = sys.maxsize
        for num in atomic_subsets_num:
            if num < len(obj.cost_list) and obj.cost_list[num] < min_cost:
                min_cost = obj.cost_list[num]
        total_cost += min_cost

        # compute the cost for updating indexes
        if 'insert' in obj.statement.lower() or 'delete' in obj.statement.lower():
            for index in config:
                index_num = get_index_num(index, atomic_config_total)
                if index_num == -1:
                    raise ValueError("The index isn't found for current query!")
                if 0 <= index_num < len(workload[ind].cost_list):
                    total_cost += obj.cost_list[index_num] - obj.cost_list[0]

    return total_cost


def simple_index_advisor(input_path, max_index_num):
    workload = workload_compression(input_path)
    print_header_boundary(" Generate candidate indexes ")
    candidate_indexes = generate_candidate_indexes_file(workload)
    if len(candidate_indexes) == 0:
        print("No candidate indexes generated!")
        return

    print_header_boundary(" Determine optimal indexes ")
    ori_total_cost = estimate_workload_cost_file(workload)
    for _, obj in enumerate(candidate_indexes):
        new_total_cost = estimate_workload_cost_file(workload, [obj])
        obj.benefit = ori_total_cost - new_total_cost
    candidate_indexes = sorted(candidate_indexes, key=lambda item: item.benefit, reverse=True)

    # filter out duplicate index
    final_index_set = {}
    final_index_list = []
    for index in candidate_indexes:
        picked = True
        cols = set(index.columns.split(','))
        if index.table not in final_index_set.keys():
            final_index_set[index.table] = []
        for i in range(len(final_index_set[index.table]) - 1, -1, -1):
            pre_index = final_index_set[index.table][i]
            pre_cols = set(pre_index.columns.split(','))
            if len(pre_cols.difference(cols)) == 0 and len(pre_cols) < len(cols):
                final_index_set[index.table].pop(i)
            if len(cols.difference(pre_cols)) == 0:
                picked = False
                break
        if picked and index.benefit > 0:
            final_index_set[index.table].append(index)
    [final_index_list.extend(item) for item in list(final_index_set.values())]
    final_index_list = sorted(final_index_list, key=lambda item: item.benefit, reverse=True)
    cnt = 0
    index_current_storage = 0
    for index in final_index_list:
        if max_index_num and cnt == max_index_num:
            break
        if MAX_INDEX_STORAGE and (index_current_storage + index.storage) > MAX_INDEX_STORAGE:
            continue
        index_current_storage += index.storage
        print("create index ind" + str(cnt) + " on " + index.table + "(" + index.columns + ");")
        cnt += 1


def greedy_determine_opt_config(workload, atomic_config_total, candidate_indexes):
    opt_config = []
    index_num_record = set()
    min_cost = sys.maxsize
    for i in range(len(candidate_indexes)):
        if i == 1 and min_cost == sys.maxsize:
            break
        cur_min_cost = sys.maxsize
        cur_index = None
        cur_index_num = -1
        for k, index in enumerate(candidate_indexes):
            if k in index_num_record:
                continue
            cur_config = copy.copy(opt_config)
            cur_config.append(index)
            cur_estimated_cost = infer_workload_cost(workload, cur_config, atomic_config_total)
            if cur_estimated_cost < cur_min_cost:
                cur_min_cost = cur_estimated_cost
                cur_index = index
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


def complex_index_advisor(input_path):
    workload = workload_compression(input_path)
    print_header_boundary(" Generate candidate indexes ")
    candidate_indexes = generate_candidate_indexes(workload, True)
    if len(candidate_indexes) == 0:
        print("No candidate indexes generated!")
        return

    print_header_boundary(" Determine optimal indexes ")
    atomic_config_total = generate_atomic_config(workload)
    if len(atomic_config_total[0]) != 0:
        raise ValueError("The empty atomic config isn't generated!")

    for atomic_config in atomic_config_total:
        estimate_workload_cost_file(workload, atomic_config)

    opt_config = greedy_determine_opt_config(workload, atomic_config_total, candidate_indexes)

    cnt = 0
    index_current_storage = 0
    for index in opt_config:
        if MAX_INDEX_STORAGE and (index_current_storage + index.storage) > MAX_INDEX_STORAGE:
            continue
        if cnt == MAX_INDEX_NUM:
            break
        index_current_storage += index.storage
        print("create index ind" + str(cnt) + " on " + index.table + "(" + index.columns + ");")
        cnt += 1


def main():
    arg_parser = argparse.ArgumentParser(description='Generate index set for workload.')
    arg_parser.add_argument("p", help="Port of database")
    arg_parser.add_argument("d", help="Name of database")
    arg_parser.add_argument("--h", help="Host for database")
    arg_parser.add_argument("-U", help="Username for database log-in")
    arg_parser.add_argument("-W", help="Password for database user", nargs="?", action=PwdAction)
    arg_parser.add_argument("f", help="File containing workload queries (One query per line)")
    arg_parser.add_argument("--schema", help="Schema name for the current business data")
    arg_parser.add_argument("--max_index_num", help="Maximum number of suggested indexes", type=int)
    arg_parser.add_argument("--max_index_storage",
                            help="Maximum storage of suggested indexes/MB", type=int)
    arg_parser.add_argument("--multi_iter_mode", action='store_true',
                            help="Whether to use multi-iteration algorithm", default=False)
    arg_parser.add_argument("--multi_node", action='store_true',
                            help="Whether to support distributed scenarios", default=False)
    args = arg_parser.parse_args()

    global MAX_INDEX_NUM, BASE_CMD, ENABLE_MULTI_NODE, MAX_INDEX_STORAGE, SCHEMA
    if args.max_index_num is not None and args.max_index_num <= 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" %
                                         args.max_index_num)
    if args.max_index_storage is not None and args.max_index_storage <= 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" %
                                         args.max_index_storage)
    if args.schema:
        SCHEMA = args.schema
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

    if args.multi_iter_mode:
        complex_index_advisor(args.f)
    else:
        simple_index_advisor(args.f, args.max_index_num)


if __name__ == '__main__':
    main()
