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
import os
import sys
import time
import traceback
import logging

from prettytable import PrettyTable

from dbmind import constants
from dbmind import global_vars
from dbmind.cmd.config_utils import DynamicConfig
from dbmind.cmd.config_utils import load_sys_configs
from dbmind.common.utils.checking import path_type, date_type, positive_int_type
from dbmind.common.utils.cli import keep_inputting_until_correct, write_to_terminal
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
    if retention_days is None:
        slow_queries.truncate_slow_queries()
        slow_queries.truncate_killed_slow_queries()
    else:
        start_time = int((time.time() - float(retention_days) * 24 * 60 * 60) * 1000)
        slow_queries.delete_slow_queries(start_time)
        slow_queries.delete_killed_slow_queries(start_time)
    write_to_terminal('Success to delete redundant results.')


def diagnosis(sql, database, schema=None, start_time=None, end_time=None):
    def is_rpc_available():
        try:
            from dbmind.common.rpc import RPCClient
            from dbmind.common.utils import read_simple_config_file
            from dbmind.constants import METRIC_MAP_CONFIG
            global_vars.metric_map = read_simple_config_file(
                METRIC_MAP_CONFIG
            )
            # Initialize RPC components.
            master_url = global_vars.configs.get('AGENT', 'master_url')
            ssl_certfile = global_vars.configs.get('AGENT', 'ssl_certfile')
            ssl_keyfile = global_vars.configs.get('AGENT', 'ssl_keyfile')
            ssl_keyfile_password = global_vars.configs.get('AGENT', 'ssl_keyfile_password')
            ssl_ca_file = global_vars.configs.get('AGENT', 'ssl_ca_file')
            agent_username = global_vars.configs.get('AGENT', 'username')
            agent_pwd = global_vars.configs.get('AGENT', 'password')
            global_vars.agent_rpc_client = RPCClient(
                master_url,
                username=agent_username,
                pwd=agent_pwd,
                ssl_cert=ssl_certfile,
                ssl_key=ssl_keyfile,
                ssl_key_password=ssl_keyfile_password,
                ca_file=ssl_ca_file
            )
            global_vars.agent_rpc_client.call('query_in_database',
                                              'select 1',
                                              'postgres',
                                              return_tuples=True)
            return True
        except Exception as e:
            logging.warning(e)
            global_vars.agent_rpc_client = None
            return False

    def initialize_tsdb_param():
        from dbmind.common.tsdb import TsdbClientFactory
        try:
            TsdbClientFactory.set_client_info(
                global_vars.configs.get('TSDB', 'name'),
                global_vars.configs.get('TSDB', 'host'),
                global_vars.configs.get('TSDB', 'port'),
                global_vars.configs.get('TSDB', 'username'),
                global_vars.configs.get('TSDB', 'password'),
                global_vars.configs.get('TSDB', 'ssl_certfile'),
                global_vars.configs.get('TSDB', 'ssl_keyfile'),
                global_vars.configs.get('TSDB', 'ssl_keyfile_password'),
                global_vars.configs.get('TSDB', 'ssl_ca_file')
            )
            return True
        except Exception as e:
            logging.warning(e)
            return False

    def is_database_exist():
        stmt = "select datname from pg_database where datname='%s'" % database
        rows = global_vars.agent_rpc_client.call('query_in_database',
                                                 stmt,
                                                 'postgres',
                                                 return_tuples=True)
        if rows:
            return True
        return False

    field_names = ('root_cause', 'suggestion')
    output_table = PrettyTable()
    output_table.field_names = field_names
    output_table.align = "l"

    if is_rpc_available() and initialize_tsdb_param():
        from dbmind.service.web import toolkit_slow_sql_rca

        write_to_terminal('RPC service exists, current diagnosis is based on RPC service...', color='green')
        if database is None:
            write_to_terminal("Lack the information of 'database', stop to diagnosis...", color='red')
            return
        if not is_database_exist():
            write_to_terminal("Database does not exist, stop to diagnosis.", color='red')
            return
        if schema is None:
            write_to_terminal("Lack the information of 'schema', use default value: 'public'.", color='yellow')
            schema = 'public'
        root_causes, suggestions = toolkit_slow_sql_rca(sql=sql,
                                                        database=database,
                                                        schema=schema,
                                                        start_time=start_time,
                                                        end_time=end_time,
                                                        skip_search=True)
        for root_cause, suggestion in zip(root_causes[0], suggestions[0]):
            output_table.add_row([root_cause, suggestion])
        print(output_table)
    else:
        write_to_terminal('RPC service not exists, existing...', color='green')


def main(argv):
    parser = argparse.ArgumentParser(description='Slow Query Diagnosis: Analyse the root cause of slow query')
    parser.add_argument('action', choices=('show', 'clean', 'diagnosis'), help='choose a functionality to perform')
    parser.add_argument('-c', '--conf', metavar='DIRECTORY', required=True, type=path_type,
                        help='set the directory of configuration files')

    parser.add_argument('--database', metavar='DATABASE',
                        help='name of database')
    parser.add_argument('--schema', metavar='SCHEMA',
                        help='schema of database')
    parser.add_argument('--query', metavar='SLOW_QUERY',
                        help='set a slow query you want to retrieve')
    parser.add_argument('--start-time', metavar='TIMESTAMP_IN_MICROSECONDS', type=date_type,
                        help='set the start time of a slow SQL diagnosis result to be retrieved')
    parser.add_argument('--end-time', metavar='TIMESTAMP_IN_MICROSECONDS', type=date_type,
                        help='set the end time of a slow SQL diagnosis result to be retrieved')
    parser.add_argument('--retention-days', metavar='DAYS', type=float,
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
    elif args.action == 'diagnosis':
        if args.query is None:
            write_to_terminal('You did noy specify query, so we cant not diagnosis root cause.')
            parser.exit(1, "Quiting due to lack of query.")

    # Set the global_vars so that DAO can login the meta-database.
    os.chdir(args.conf)
    global_vars.configs = load_sys_configs(constants.CONFILE_NAME)

    try:
        if args.action == 'show':
            show(args.query, args.start_time, args.end_time)
        elif args.action == 'clean':
            clean(args.retention_days)
        elif args.action == 'diagnosis':
            global_vars.dynamic_configs = DynamicConfig
            diagnosis(args.query, args.database, args.schema)
    except Exception as e:
        write_to_terminal('An error occurred probably due to database operations, '
                          'please check database configurations. For details:\n'
                          + str(e), color='red', level='error')
        traceback.print_tb(e.__traceback__)
        return 2
    return args


if __name__ == '__main__':
    main(sys.argv[1:])
