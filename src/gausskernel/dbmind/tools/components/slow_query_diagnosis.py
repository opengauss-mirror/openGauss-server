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
import argparse
import sys
import os
import traceback
import time

from prettytable import PrettyTable

from dbmind import global_vars
from dbmind import constants
from dbmind.common.utils import write_to_terminal
from dbmind.cmd.config_utils import load_sys_configs
from dbmind.common.utils import keep_inputting_until_correct
from dbmind.common.utils import check_positive_integer, check_positive_float
from dbmind.metadatabase.dao import slow_queries


def show(query, start_time, end_time):
    field_names = (
        'slow_query_id', 'schema_name', 'db_name',
        'query', 'start_at', 'duration_time',
        'root_cause', 'suggestion'
    )
    output_table = PrettyTable()
    output_table.field_names = field_names

    result = slow_queries.select_slow_queries(field_names, query, start_time, end_time)
    nb_rows = 0
    for slow_query in result:
        row = [getattr(slow_query, field) for field in field_names]
        output_table.add_row(row)
        nb_rows += 1

    if nb_rows > 50:
        write_to_terminal('The number of rows is greater than 50. '
                          'It seems too long to see.')
        char = keep_inputting_until_correct('Do you want to dump to a file? [Y]es, [N]o.', ('Y', 'N'))
        if char == 'Y':
            dump_file_name = 'slow_queries_%s.txt' % int(time.time())
            with open(dump_file_name, 'w+') as fp:
                fp.write(str(output_table))
            write_to_terminal('Dumped file is %s.' % os.path.realpath(dump_file_name))
        elif char == 'N':
            print(output_table)
            print('(%d rows)' % nb_rows)
    else:
        print(output_table)
        print('(%d rows)' % nb_rows)


def clean(retention_days):
    if float(retention_days) == 0:
        slow_queries.truncate_slow_queries()
    else:
        start_time = int((time.time() - retention_days * 24 * 60 * 60) * 1000)
        slow_queries.delete_slow_queries(start_time)
    write_to_terminal('Success to delete redundant results.')


def main(argv):
    parser = argparse.ArgumentParser(description='Slow Query Diagnosis: Analyse the root cause of slow query')
    parser.add_argument('action', choices=('show', 'clean'), help='choose a functionality to perform')
    parser.add_argument('-c', '--conf', metavar='DIRECTORY', required=True,
                        help='set the directory of configuration files')

    parser.add_argument('--query', metavar='SLOW_QUERY',
                        help='set a slow query you want to retrieve')
    parser.add_argument('--start-time', type=check_positive_integer, metavar='TIMESTAMP_IN_MICROSECONDS',
                        help='set the start time of a slow SQL diagnosis result to be retrieved')
    parser.add_argument('--end-time', type=check_positive_integer, metavar='TIMESTAMP_IN_MICROSECONDS',
                        help='set the end time of a slow SQL diagnosis result to be retrieved')

    parser.add_argument('--retention-days', type=check_positive_float, metavar='DAYS', default=0,
                        help='clear historical diagnosis results and set '
                             'the maximum number of days to retain data')

    args = parser.parse_args(argv)

    if not os.path.exists(args.conf):
        parser.exit(1, 'Not found the directory %s.' % args.conf)

    if args.action == 'show':
        if None in (args.query, args.start_time, args.end_time):
            write_to_terminal('There may be a lot of results because you did not use all filter conditions.',
                              color='red')
            inputted_char = keep_inputting_until_correct('Press [A] to agree, press [Q] to quit:', ('A', 'Q'))
            if inputted_char == 'Q':
                parser.exit(0, "Quitting due to user's instruction.")
    elif args.action == 'clean':
        if args.retention_days is None:
            write_to_terminal('You did not specify retention days, so we will delete all historical results.',
                              color='red')
            inputted_char = keep_inputting_until_correct('Press [A] to agree, press [Q] to quit:', ('A', 'Q'))
            if inputted_char == 'Q':
                parser.exit(0, "Quitting due to user's instruction.")

    # Set the global_vars so that DAO can login the meta-database.
    os.chdir(args.conf)
    global_vars.configs = load_sys_configs(constants.CONFILE_NAME)

    try:
        if args.action == 'show':
            show(args.query, args.start_time, args.end_time)
        elif args.action == 'clean':
            clean(args.retention_days)
    except Exception as e:
        write_to_terminal('An error occurred probably due to database operations, '
                          'please check database configurations. For details:\n'
                          + str(e), color='red', level='error')
        traceback.print_tb(e.__traceback__)
        return 2
    return args


if __name__ == '__main__':
    main(sys.argv[1:])
