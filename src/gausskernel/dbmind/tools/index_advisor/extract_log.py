import re
import os
import argparse
import json
import random
import time
from subprocess import Popen, PIPE

SQL_TYPE = ['select ', 'delete ', 'insert ', 'update ']
SQL_AMOUNT = 0
PLACEHOLDER = r'@@@'
SAMPLE_NUM = 5
SQL_PATTERN = [r'\((\s*(\d+(\.\d+)?\s*)[,]?)+\)',  # match integer set in the IN collection
               r'([^\\])\'((\')|(.*?([^\\])\'))',  # match all content in single quotes
               r'(([^<>]\s*=\s*)|([^<>]\s+))(\d+)(\.\d+)?']  # match single integer


def get_workload_template(templates, sqls):
    for sql in sqls:
        sql_template = sql
        for pattern in SQL_PATTERN:
            sql_template = re.sub(pattern, PLACEHOLDER, sql_template)
        if sql_template not in templates:
            templates[sql_template] = {}
            templates[sql_template]['cnt'] = 0
            templates[sql_template]['samples'] = []
        templates[sql_template]['cnt'] += 1
        # reservoir sampling
        if len(templates[sql_template]['samples']) < SAMPLE_NUM:
            if sql not in templates[sql_template]['samples']:
                templates[sql_template]['samples'].append(sql)
        else:
            if random.randint(0, templates[sql_template]['cnt']) < SAMPLE_NUM:
                templates[sql_template]['samples'][random.randint(0, SAMPLE_NUM - 1)] = sql


def output_valid_sql(sql):
    is_quotation_valid = sql.count("'") % 2
    if 'from pg_' in sql.lower() or is_quotation_valid:
        return ''
    if any(tp in sql.lower() for tp in SQL_TYPE[1:]) or \
            (SQL_TYPE[0] in sql.lower() and 'from ' in sql.lower()):
        sql = re.sub(r'for\s+update[\s;]*$', '', sql, flags=re.I)
        return sql.strip() if sql.endswith('; ') else sql + ';'
    return ''


def get_parsed_sql(file, user, database, sql_amount, statement):
    global SQL_AMOUNT
    line = file.readline()
    sql = ''
    statement_flag = False
    execute_flag = False

    while line:
        if sql_amount and SQL_AMOUNT == sql_amount:
            break
        try:
            # Identify statement scene
            if re.search('statement: ', line.lower(), re.IGNORECASE) and statement:
                if output_valid_sql(sql):
                    SQL_AMOUNT += 1
                    yield output_valid_sql(sql)
                log_info = line.split(' ')
                if (user and user not in log_info) or (
                        database and database not in log_info):
                    line = file.readline()
                    continue
                statement_flag = True
                sql = re.search(r'statement: (.*)', line.strip(), re.IGNORECASE).group(1) + ' '
                line = file.readline()

            # Identify execute statement scene
            elif re.search(r'execute .*:', line, re.IGNORECASE):
                if output_valid_sql(sql):
                    SQL_AMOUNT += 1
                    yield output_valid_sql(sql)
                log_info = line.split(' ')
                if (user and user not in log_info) or (
                        database and database not in log_info):
                    line = file.readline()
                    continue
                execute_flag = True
                sql = re.search(r'execute .*: (.*)', line.strip(), re.IGNORECASE).group(1)
                line = file.readline()
            else:
                if statement_flag:
                    if re.match(r'^\t', line):
                        sql += line.strip('\t\n')
                    else:
                        statement_flag = False
                        if output_valid_sql(sql):
                            SQL_AMOUNT += 1
                            yield output_valid_sql(sql)
                            sql = ''
                if execute_flag:
                    execute_flag = False
                    if re.search(r'parameters: ', line, re.IGNORECASE):
                        param_list = re.search(r'parameters: (.*)', line.strip(),
                                               re.IGNORECASE).group(1).split(', $')
                        param_list = list(param.split('=', 1) for param in param_list)
                        param_list.sort(key=lambda x: int(x[0].strip(' $')),
                                        reverse=True)
                        for item in param_list:
                            sql = sql.replace(item[0].strip() if re.match(r'\$', item[0]) else
                                               ('$' + item[0].strip()), item[1].strip())
                        if output_valid_sql(sql):
                            SQL_AMOUNT += 1
                            yield output_valid_sql(sql)
                            sql = ''
                line = file.readline()
        except:
            execute_flag = False
            statement_flag = False
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


def record_sql(valid_files, args, output_obj):
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
            with open(file_path, mode='r') as f:
                f.seek(start_position, 0)
                if isinstance(output_obj, dict):
                    get_workload_template(output_obj, get_parsed_sql(f, args.U, args.d,
                                                                     args.sql_amount,
                                                                     args.statement))
                else:
                    for sql in get_parsed_sql(f, args.U, args.d, args.sql_amount, args.statement):
                        output_obj.write(sql + '\n')


def extract_sql_from_log(args):
    files = os.listdir(args.l)
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
        templates = {}
        record_sql(valid_files, args, templates)
        with open(args.f, 'w') as output_file:
            json.dump(templates, output_file)
    else:
        with open(args.f, 'w') as output_file:
            record_sql(valid_files, args, output_file)


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("l", help="The path of the log file that needs to be parsed.")
    arg_parser.add_argument("f", help="The output path of the extracted file.")
    arg_parser.add_argument("-d", help="Name of database")
    arg_parser.add_argument("-U", help="Username for database log-in")
    arg_parser.add_argument("--start_time", help="Start time of extracted log")
    arg_parser.add_argument("--sql_amount", help="The number of sql collected", type=int)
    arg_parser.add_argument("--statement", action='store_true', help="Extract statement log type",
                            default=False)
    arg_parser.add_argument("--json", action='store_true',
                            help="Whether the workload file format is json", default=False)

    args = arg_parser.parse_args()
    if args.start_time:
        time.strptime(args.start_time, '%Y-%m-%d %H:%M:%S')
    if args.sql_amount is not None and args.sql_amount <= 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" % args.sql_amount)
    extract_sql_from_log(args)


if __name__ == '__main__':
    main()

