import re
import os
import argparse


SQL_TYPE = ['select', 'delete', 'insert', 'update']


def output_valid_sql(sql):
    if 'from pg_' in sql.lower():
        return ''
    if any(tp in sql.lower() for tp in SQL_TYPE[1:]):
        return sql if sql.endswith('; ') else sql + ';'
    elif SQL_TYPE[0] in sql.lower() and 'from ' in sql.lower():
        return sql if sql.endswith('; ') else sql + ';'
    return ''


def extract_sql_from_log(log_path):
    files = os.listdir(log_path)
    for file in files:
        file_path = log_path + "/" + file
        if os.path.isfile(file_path) and re.search(r'.log$', file):
            with open(file_path, mode='r') as f:
                line = f.readline()
                sql = ''
                statement_flag = False
                execute_flag = False

                while line:
                    try:
                        # Identify statement scene
                        if re.search('statement: ', line, re.IGNORECASE):
                            statement_flag = True
                            if output_valid_sql(sql):
                                yield output_valid_sql(sql)
                            sql = re.search(r'statement: (.*)', line.strip(),
                                            re.IGNORECASE).group(1) + ' '
                            line = f.readline()

                        # Identify execute statement scene
                        elif re.search(r'execute .*:', line, re.IGNORECASE):
                            if output_valid_sql(sql):
                                yield output_valid_sql(sql)
                            execute_flag = True
                            sql = re.search(r'execute .*: (.*)', line.strip(), re.IGNORECASE).group(1)
                            line = f.readline()
                        else:
                            if statement_flag:
                                if re.match(r'^\t', line):
                                    sql += line.strip('\t\n')
                                else:
                                    statement_flag = False
                                    if output_valid_sql(sql):
                                        yield output_valid_sql(sql)
                                        sql = ''
                            if execute_flag and re.search(r'parameters: ', line, re.IGNORECASE):
                                execute_flag = False
                                param_list = re.search(r'parameters: (.*)', line.strip(),
                                                       re.IGNORECASE).group(1).split(', ')
                                param_list = list(param.split('=', 1) for param in param_list)
                                param_list.sort(key=lambda x: int(x[0].strip(' $')),
                                                reverse=True)
                                for item in param_list:
                                    if len(item[1].strip()) >= 256:
                                        sql = sql.replace(item[0].strip(), "''")
                                    else:
                                        sql = sql.replace(item[0].strip(), item[1].strip())
                                yield output_valid_sql(sql)
                                sql = ''
                            line = f.readline()
                    except:
                        execute_flag = False
                        statement_flag = False
                        line = f.readline()


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("l", help="The path of the log file that needs to be parsed.")
    arg_parser.add_argument("f", help="The output path of the extracted file.")
    args = arg_parser.parse_args()
    sqls = extract_sql_from_log(args.l)
    with open(args.f, 'w') as file:
        for sql in sqls:
            file.write(sql + '\n')


if __name__ == '__main__':
    main()

