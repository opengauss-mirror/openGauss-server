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
import getpass
import logging
import sys
from copy import deepcopy

import sqlparse
from prettytable import PrettyTable
from mo_sql_parsing import parse, format

from dbmind.common.utils.checking import CheckWordValid, path_type, positive_int_type
from dbmind.common.utils.cli import read_input_from_pipe
from dbmind.constants import __version__

try:
    from utils import get_table_names
    from executor import Executor
    from rules import AlwaysTrue, DistinctStar, OrderbyConst, Star2Columns, UnionAll, Delete2Truncate, \
        Or2In, \
        OrderbyConstColumns, \
        ImplicitConversion, \
        SelfJoin, Having2Where
    from rules import Rule
except ImportError:
    from .utils import get_table_names
    from .executor import Executor
    from .rules import AlwaysTrue, DistinctStar, OrderbyConst, Star2Columns, UnionAll, Delete2Truncate, \
        Or2In, \
        OrderbyConstColumns, \
        ImplicitConversion, \
        SelfJoin, Having2Where
    from .rules import Rule


def get_all_involved_tables(sql, table_names=None):
    if table_names is None:
        table_names = []
        parsed_sql = parse(sql)
    else:
        parsed_sql = sql
    if isinstance(parsed_sql, list):
        for sub_parsed_sql in parsed_sql:
            get_all_involved_tables(sub_parsed_sql, table_names)
    elif isinstance(parsed_sql, dict):
        if 'union' in parsed_sql or 'union_all' in parsed_sql:
            return get_all_involved_tables(list(parsed_sql.values())[0], table_names)
        if parsed_sql.get('from'):
            if not (isinstance(parsed_sql['from'], dict) and parsed_sql.get('from')):
                cur_table_names = get_table_names(parsed_sql['from'])
                for table_name in cur_table_names:
                    if isinstance(table_name, str):
                        table_names.append(table_name)
            else:
                # for select nested statements
                table_names.extend(get_all_involved_tables(parsed_sql['from'], []))
    return table_names


def get_table_columns(sql, executor: Executor):
    table2columns_mapper = dict()
    involved_tables = get_all_involved_tables(sql)
    for table_name in involved_tables:
        table2columns_mapper[table_name] = executor.get_table_columns(table_name)
    return table2columns_mapper


def exists_primary_key(tables, executor: Executor):
    table_exists_primary = dict()
    for table_name in tables:
        table_exists_primary[table_name] = executor.exists_primary_key(table_name)

    return table_exists_primary


def singleton(cls):
    _instance = {}

    def inner():
        if cls not in _instance:
            _instance[cls] = cls()
        return _instance[cls]

    return inner


def get_offline_rewriter():
    rewriter = deepcopy(SQLRewriter())
    rewriter.clear_rules()
    rewriter.add_rule(ImplicitConversion)
    rewriter.add_rule(OrderbyConstColumns)
    rewriter.add_rule(AlwaysTrue)
    rewriter.add_rule(UnionAll)
    rewriter.add_rule(Delete2Truncate)
    rewriter.add_rule(Or2In)
    rewriter.add_rule(SelfJoin)
    return rewriter


@singleton
class SQLRewriter:
    def __init__(self):
        self.rules = []
        self.add_rule(DistinctStar)
        self.add_rule(Star2Columns)
        self.add_rule(ImplicitConversion)
        self.add_rule(OrderbyConst)
        self.add_rule(Having2Where)
        self.add_rule(OrderbyConstColumns)
        self.add_rule(AlwaysTrue)
        self.add_rule(UnionAll)
        self.add_rule(Delete2Truncate)
        self.add_rule(Or2In)
        self.add_rule(SelfJoin)

    def rewrite(self, sql, table2columns=None, table_exists_primary=None, if_format=True):
        if table2columns is None:
            table2columns = dict()
        if table_exists_primary is None:
            table_exists_primary = dict()
        parsed_sql = parse(sql)
        try:
            checked_rules = self._apply_rules(parsed_sql, table2columns, table_exists_primary)
        except:
            return False, sql if sql.endswith(';') else sql + ';'
        sql_string = format(parsed_sql) + ';'
        if Delete2Truncate().__class__.__name__ in checked_rules:
            return True, 'TRUNCATE TABLE ' + sql_string.split('(')[1].split(')')[0] + ';'
        if if_format:
            sql_string = sqlparse.format(sql_string, reindent=True, keyword_case='upper')

        return True if checked_rules else False, sqlparse.format(sql_string, reindent=True, keyword_case='upper')

    def add_rule(self, rule):
        if not issubclass(rule, Rule):
            raise NotImplementedError()
        self.rules.append(rule)

    def clear_rules(self):
        self.rules = []

    def _apply_rules(self, parsed_sql, table2columns, table_exists_primary):
        checked_rules = []
        for rule in self.rules:
            res = rule().check_and_format(parsed_sql, table2columns, table_exists_primary)
            if res:
                checked_rules.append(res)
        return checked_rules


def get_password():
    password = read_input_from_pipe()
    if password:
        logging.warning("Read password from pipe.")
    else:
        password = getpass.getpass("Password for database user:")
    if not password:
        raise ValueError('Please input the password')
    return password


def canbe_parsed(sql):
    try:
        parse(sql)
    except Exception as e:
        return False
    return True


def main(argv):
    arg_parser = argparse.ArgumentParser(
        description='SQL Rewriter')
    arg_parser.add_argument("db_port", help="Port for database", type=positive_int_type)
    arg_parser.add_argument("database", help="Name for database", action=CheckWordValid)
    arg_parser.add_argument("file", type=path_type, help="File containing SQL statements which need to rewrite")
    arg_parser.add_argument(
        "--db-host", help="Host for database", action=CheckWordValid)
    arg_parser.add_argument(
        "--db-user", help="Username for database log-in", action=CheckWordValid)
    arg_parser.add_argument(
        "--schema", help="Schema name for the current business data", default='public', action=CheckWordValid)
    arg_parser.add_argument('-v', '--version', action='version', version=__version__)

    args = arg_parser.parse_args(argv)
    args.W = get_password()
    executor = Executor(args.database, args.db_user, args.W, args.db_host, args.db_port, args.schema)
    field_names = ('Raw SQL', 'Rewritten SQL')
    output_table = PrettyTable()
    output_table.field_names = field_names
    output_table.align = "l"
    with open(args.file) as file_h:
        content = file_h.read()
    for _sql in sqlparse.split(content):
        if not _sql.strip():
            continue
        sql = _sql.strip() if _sql.strip().endswith(';') else _sql.strip() + ';'
        if not executor.syntax_check(sql) or not canbe_parsed(sql):
            output_table.add_row([sql, ''])
            continue
        table2columns_mapper = get_table_columns(sql, executor)
        table_exists_primary = exists_primary_key(table2columns_mapper.keys(), executor)
        res, rewritten_sql = SQLRewriter().rewrite(sql, table2columns_mapper, table_exists_primary)
        if not executor.syntax_check(rewritten_sql) or not res:
            output_table.add_row([sql, '']) 
        else:
            output_table.add_row([sql, rewritten_sql])
    print(output_table)


if __name__ == '__main__':
    main(sys.argv[1:])
