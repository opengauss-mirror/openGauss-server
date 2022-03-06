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

import re
import os
import sys
import argparse
import json
import random
import time
from collections import deque
from subprocess import Popen, PIPE

SQL_TYPE = ['select ', 'delete ', 'insert ', 'update ']
SQL_AMOUNT = 0
PLACEHOLDER = r'@@@'
SAMPLE_NUM = 5
IS_ALL_LATEST_SQL = False
FILEHANDLES = 500
SQL_PATTERN = [r'\((\s*(\d+(\.\d+)?\s*)[,]?)+\)',  # match integer set in the IN collection
               r'([^\\])\'((\')|(.*?([^\\])\'))',  # match all content in single quotes
               r'(([^<>]\s*=\s*)|([^<>]\s+))(\d+)(\.\d+)?']  # match single integer


def truncate_template(templates, update_time, avg_update):
    global IS_ALL_LATEST_SQL
    prune_list = []
    # get the currently unupdated template list
    if not IS_ALL_LATEST_SQL:
        for sql_template, sql_detail in templates.items():
            if sql_detail['update'][-1] != update_time and len(sql_detail['update']) < avg_update:
                prune_list.append((sql_template, len(sql_detail['update'])))
    # filter by update frequency
    if len(prune_list) > len(templates) / SAMPLE_NUM:
        sorted(prune_list, key=lambda elem: elem[1])
        prune_list = prune_list[:len(templates) // SAMPLE_NUM]
    if len(prune_list):
        for item in prune_list:
            del templates[item[0]]
        return True
    IS_ALL_LATEST_SQL = True
    # if all templates have been updated, then randomly selected one to be deleted
    if random.random() < 0.5:
        del templates[random.sample(templates.keys(), 1)[0]]
        return True
    return False


def get_workload_template(templates, sqls, args):
    update_time = time.time()
    invalid_template = []
    total_update = 0
    is_record = True
    # delete templates that have not been updated within UPDATE_THRESHOLD threshold
    for sql_template, sql_detail in templates.items():
        if (update_time - sql_detail['update'][-1]) / 60 / 60 / 24 >= args.max_reserved_period:
            invalid_template.append(sql_template)
            continue
        total_update += len(sql_detail['update'])
    avg_update = (total_update / len(templates)) if len(templates) else 0
    for item in invalid_template:
        del templates[item]
    for sql in sqls:
        sql_template = sql
        for pattern in SQL_PATTERN:
            sql_template = re.sub(pattern, PLACEHOLDER, sql_template)
        if sql_template not in templates:
            # prune the templates if the total size is greater than the given threshold
            if len(templates) >= args.max_template_num:
                is_record = truncate_template(templates, update_time, avg_update)
            if not is_record:
                continue
            templates[sql_template] = {}
            templates[sql_template]['cnt'] = 0
            templates[sql_template]['samples'] = []
            templates[sql_template]['update'] = []
        templates[sql_template]['cnt'] += 1
        # clear the update threshold outside
        for ind, item in enumerate(templates[sql_template]['update']):
            if (update_time - item) / 60 / 60 / 24 < args.max_reserved_period:
                templates[sql_template]['update'] = templates[sql_template]['update'][ind:]
                break
        # update the last update time of the sql template
        if update_time not in templates[sql_template]['update']:
            templates[sql_template]['update'].append(update_time)
        # reservoir sampling
        if len(templates[sql_template]['samples']) < SAMPLE_NUM:
            if sql not in templates[sql_template]['samples']:
                templates[sql_template]['samples'].append(sql)
        else:
            if random.randint(0, templates[sql_template]['cnt']) < SAMPLE_NUM:
                templates[sql_template]['samples'][random.randint(0, SAMPLE_NUM - 1)] = sql


def output_valid_sql(sql):
    is_quotation_valid = sql.count("'") % 2
    if re.search(r'=([\s]+)?\$', sql):
        return ''
    if 'from pg_' in sql.lower() or 'gs_index_advise' in sql.lower() or is_quotation_valid:
        return ''
    if any(tp in sql.lower() for tp in SQL_TYPE[1:]) or \
            (SQL_TYPE[0] in sql.lower() and 'from ' in sql.lower()):
        sql = re.sub(r'for\s+update[\s;]*$', '', sql, flags=re.I)
        return sql.strip('; ') + ';'
    return ''


class SqlRecord:
    def __init__(self):
        self.sqllist = []
        self.in_transaction = False


def read_record_rest(file):
    # get the rest string for a record, and start line of the next record
    line = file.readline()
    rest_content = ''
    while re.match(r'^\t', line):
        rest_content += (line.strip('\n') + ' ')
        line = file.readline()
    return rest_content, line


def get_parsed_sql(file, filter_config, log_info_position):
    global SQL_AMOUNT
    user = filter_config['user']
    database = filter_config['database']
    sql_amount = filter_config['sql_amount']
    statement = filter_config['statement']
    user_position = log_info_position.get('u')
    database_position = log_info_position.get('d')
    threadid_position = log_info_position.get('p')
    line = file.readline()
    sql_record = SqlRecord()
    search_p = r'execute .*: (.*)'
    if statement:
        search_p = r'statement: (.*)|' + search_p
    while line:
        if sql_amount and SQL_AMOUNT == sql_amount:
            break
        try:
            if (statement and re.search('statement: ', line.lower(), re.IGNORECASE)) \
                    or re.search(r'execute .*:', line, re.IGNORECASE):
                rest_content, nextline = read_record_rest(file)
                recordstring = line.strip() + ' ' + rest_content.strip()
                line = nextline
                log_info = recordstring.split(' ')
                if (user and user != log_info[user_position]) \
                        or (database and database != log_info[database_position]):
                    continue
                search_results = re.search(search_p, recordstring, re.IGNORECASE).groups()
                sql = search_results[0] if search_results[0] else search_results[1]
                if re.match(r'(start transaction)|(begin)|(begin transaction)', sql.lower() \
                        .split(';')[0].strip()) \
                        and threadid_position:
                    sql_record.in_transaction = True
                    if sql_record.sqllist:
                        yield ''.join(sql_record.sqllist)
                        SQL_AMOUNT += 1
                        sql_record.sqllist = []
                    sql = '' if len(sql.lower().strip(';').split(';', 1)) == 1 else \
                    sql.lower().strip(';').split(';', 1)[1]
                if sql.lower().strip().strip(';').strip().endswith(('commit', 'rollback')) \
                        and threadid_position:
                    output_sql = output_valid_sql(sql.lower().strip().strip(';') \
                                                  .replace('commit', '').replace('rollback', ''))
                    if output_sql:
                        sql_record.sqllist.append(output_sql)
                    sql_record.in_transaction = False
                    if sql_record.sqllist:
                        yield ''.join(sql_record.sqllist)
                        SQL_AMOUNT += 1
                    sql_record.sqllist = []
                else:
                    output_sql = output_valid_sql(sql)
                    if output_sql:
                        if sql_record.in_transaction == False:
                            yield output_sql
                            SQL_AMOUNT += 1
                        else:
                            sql_record.sqllist.append(output_sql)
                continue
            elif re.search(r'parameters: ', line, re.IGNORECASE):
                param_list = re.search(r'parameters: (.*)', line.strip(),
                                       re.IGNORECASE).group(1).split(', $')
                param_list = list(param.split('=', 1) for param in param_list)
                param_list.sort(key=lambda x: int(x[0].strip(' $')),
                                reverse=True)
                for item in param_list:
                    sql = sql.replace(item[0].strip() if re.match(r'\$', item[0]) else
                                      ('$' + item[0].strip()), item[1].strip())
                output_sql = output_valid_sql(sql)
                if output_sql:
                    if not sql_record.in_transaction:
                        yield output_sql
                        SQL_AMOUNT += 1
                    else:
                        sql_record.sqllist.append(output_sql)
                line = file.readline()
            else:
                line = file.readline()
        except:
            line = file.readline()


def get_start_position(start_time, file_path):
    while start_time:
        cmd = 'head -n $(cat %s | grep -m 1 -n "^%s" | awk -F : \'{print $1}\') %s | wc -c' % \
              (file_path, start_time, file_path)
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
        std, err_msg = proc.communicate()
        if proc.returncode == 0 and not err_msg:
            return int(std)
        elif len(start_time) > 13:
            start_time = start_time[0: -3]
        else:
            break
    return -1


def make_not_existsfile(basename):
    index = 0
    tempname = basename
    while os.path.exists(tempname):
        tempname = basename + '.' + str(index)
        index += 1
    return tempname


def get_tempfile_name(threadid):
    return make_not_existsfile('/tmp/' + 'threadid_' + threadid + '.log')


class threadid_info:

    def __init__(self, filename):
        self.filename = filename
        self.fileh = open(self.filename, 'w')

    def close(self):
        self.fileh.close()

    def open(self):
        self.fileh = open(self.filename, 'a+')

    def write(self, content):
        self.fileh.write(content)


# split the log to different files groupby the threadid with file handles below FILEHANDLES 
def group_log_by_threadid(f, threadid_position):
    threadid = '000000'
    threadid_log = dict()
    closed_files = deque()
    opened_files = deque()
    threadid_log_files = []

    try:
        for line in f:
            if not line.startswith('\t') and threadid_position:
                try:
                    threadid = line.strip().split()[threadid_position]
                except IndexError:
                    raise ValueError(f'wrong format for log line:{line.strip()}')
                if not threadid.isdigit():
                    raise ValueError(f'invalid int value {threadid} for %p')
            if not threadid in threadid_log:
                threadid_log_file = get_tempfile_name(threadid)
                threadid_log_files.append(threadid_log_file)
                threadid_log[threadid] = threadid_info(threadid_log_file)
                opened_files.append(threadid)
            elif threadid in closed_files:
                closed_files.remove(threadid)
                threadid_log[threadid].open()
                opened_files.append(threadid)
            threadid_log[threadid].write(line)
            if len(opened_files) > FILEHANDLES:
                threadid = opened_files.popleft()
                threadid_log[threadid].close()
                closed_files.append(threadid)
        for threadid in opened_files:
            threadid_log[threadid].close()
    except Exception as ex:
        for threadid in opened_files:
            threadid_log[threadid].close()
        for threadid_log_file in threadid_log_files:
            os.remove(threadid_log_file)
        raise ex

    return threadid_log_files


def merge_log(threadid_log_files, start_time):
    merged_log_file = '/tmp/threadid_groupby_id' + start_time + '.log'
    merged_log_file = make_not_existsfile(merged_log_file)
    with open(merged_log_file, 'w') as fileh:
        for threadid_log_file in threadid_log_files:
            for line in open(threadid_log_file):
                fileh.write(line)
            os.remove(threadid_log_file)
    return merged_log_file


def split_transaction(transactions):
    for transaction in transactions:
        for sql in transaction.strip().strip(';').split(';'):
            yield sql


def generate_info_position(log_line_prefix):
    log_info_position = {}
    index = 0
    for _format in log_line_prefix.replace(' ', '').replace('%', ''):
        log_info_position[_format] = index
        if _format == 'm':
            index += 1
        index += 1
    return log_info_position


def record_sql(valid_files, args, log_info_position, output_obj):
    for ind, file in enumerate(valid_files):
        if args.sql_amount and SQL_AMOUNT >= args.sql_amount:
            break
        file_path = os.path.join(args.l, file)
        if os.path.isfile(file_path) and re.search(r'.log$', file):
            start_position = 0
            if ind == 0 and args.start_time:
                start_position = get_start_position(args.start_time, file_path)
                if start_position == -1:
                    continue
            with open(file_path) as f:
                f.seek(start_position, 0)
                threadid_log_files = group_log_by_threadid(f, log_info_position.get('p'))
            try:
                merged_log_file = merge_log(threadid_log_files, args.start_time if args.start_time else '')
            except Exception as ex:
                raise ex
            finally:
                for threadid_log_file in threadid_log_files:
                    if os.path.isfile(threadid_log_file):
                        os.remove(threadid_log_file)
            try:
                with open(merged_log_file, mode='r') as f:
                    if isinstance(output_obj, dict):
                        get_workload_template(output_obj, split_transaction(
                            get_parsed_sql(f, args.U, args.d,
                                           args.sql_amount,
                                           args.statement),
                        ), args)
                    else:
                        filter_config = {'user': args.U, 'database': args.d,
                                         'sql_amount': args.sql_amount, 'statement': args.statement}
                        for sql in get_parsed_sql(f, filter_config, log_info_position):
                            output_obj.write(sql + '\n')
            except Exception as ex:
                raise ex
            finally:
                os.remove(merged_log_file)


def extract_sql_from_log(args):
    files = [file for file in os.listdir(args.l) if file.endswith('.log')]
    log_info_position = generate_info_position(args.p)
    files = sorted(files, key=lambda x: os.path.getctime(os.path.join(args.l, x)), reverse=True)
    valid_files = files
    if args.start_time:
        time_stamp = int(time.mktime(time.strptime(args.start_time, '%Y-%m-%d %H:%M:%S')))
        valid_files = []
        for file in files:
            if os.path.getmtime(os.path.join(args.l, file)) < time_stamp:
                break
            valid_files.insert(0, file)
    if args.json:
        try:
            with open(args.f, 'r') as output_file:
                templates = json.load(output_file)
        except (json.JSONDecodeError, FileNotFoundError) as e:
            templates = {}
        record_sql(valid_files, args, log_info_position, templates)
        with open(args.f, 'w') as output_file:
            json.dump(templates, output_file)
    else:
        with open(args.f, 'w') as output_file:
            record_sql(valid_files, args, log_info_position, output_file)


def main(argv):
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("l", help="The path of the log file that needs to be parsed.")
    arg_parser.add_argument("f", help="The output path of the extracted file.")
    arg_parser.add_argument("p",
                            type=str,
                            help="Log line prefix")
    arg_parser.add_argument("-d", help="Name of database")
    arg_parser.add_argument("-U", help="Username for database log-in")
    arg_parser.add_argument("--start_time", help="Start time of extracted log")
    arg_parser.add_argument("--sql_amount", help="The number of sql collected", type=int)
    arg_parser.add_argument("--statement", action='store_true', help="Extract statement log type",
                            default=False)
    arg_parser.add_argument("--max_reserved_period", type=int, help='Specify days to reserve template')
    arg_parser.add_argument("--max_template_num", type=int, help='Set the max number of template and '
                                                                 'the number below 5000 is advised '
                                                                 'for time cost')
    arg_parser.add_argument("--json", action='store_true',
                            help="Whether the workload file format is json", default=False)

    args = arg_parser.parse_args(argv)
    if args.U:
        if not 'u' in args.p:
            raise argparse.ArgumentTypeError(f"input parameter p '{args.p}' does not contain"
                                             " '%u' and U is not allowed.")
    if args.d:
        if not 'd' in args.p:
            raise argparse.ArgumentTypeError(f"input parameter p '{args.p}' does not contain"
                                             " '%d' and d is not allowed.")
    if args.start_time:
        # compatible with '2022-1-4 1:2:3'
        args.start_time = time.strftime('%Y-%m-%d %H:%M:%S',
                                        time.strptime(args.start_time,
                                                      '%Y-%m-%d %H:%M:%S')
                                        )
        if not 'm' in args.p:
            raise argparse.ArgumentTypeError(f"input parameter p '{args.p}' does not contain"
                                             " '%m' and start_time is not allowed.")
    if args.sql_amount is not None and args.sql_amount <= 0:
        raise argparse.ArgumentTypeError("sql_amount %s is an invalid positive int value" %
                                         args.sql_amount)
    if args.max_reserved_period and args.max_reserved_period <= 0:
        raise argparse.ArgumentTypeError("max_reserved_period %s is an invalid positive int value" %
                                         args.max_reserved_period)
    if args.max_template_num and args.max_template_num <= 0:
        raise argparse.ArgumentTypeError("max_template_num %s is an invalid positive int value" %
                                         args.max_template_num)
    elif args.max_template_num and args.max_template_num > 5000:
        print('max_template_num %d above 5000 is not advised for time cost' % args.max_template_num)
    if not args.max_reserved_period:
        args.max_reserved_period = float('inf')
    if not args.max_template_num:
        args.max_template_num = float('inf')
    extract_sql_from_log(args)


if __name__ == '__main__':
    main(sys.argv[1:])
