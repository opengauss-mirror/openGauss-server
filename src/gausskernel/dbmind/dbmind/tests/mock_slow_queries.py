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
from dbmind.common.types import Sequence
from dbmind.service.dai import LazyFetcher

simple_slow_sql_dict = {'from_instance': '127.0.0.1:5432', 'datname': 'database1', 'schema': 'public',
                        'query': 'select count(*) from schema1.table1', 'start_time': '1640139690000',
                        'finish_time': '1640139700000', 'hit_rate': '0.988', 'fetch_rate': '0.99', 'cpu_time': '14200',
                        'data_io_time': '1231243', 'unique_query_id': '12432453234', 'sort_count': '13',
                        'sort_mem_used': '12.43', 'sort_spill_count': '3', 'hash_count': '0', 'hash_mem_used': '0',
                        'hash_spill_count': '0', 'lock_wait_count': '0', 'lwlock_wait_count': '0',
                        'n_returned_rows': '1', 'n_tuples_returned': '100000', 'n_tuples_fetched': '0',
                        'n_tuples_inserted': '0', 'n_tuples_updated': '0', 'n_tuples_deleted': 0}
simple_slow_sql_seq = Sequence(timestamps=[1640139695000],
                               values=[101],
                               name='pg_sql_statement_history_exec_time',
                               step=5,
                               labels=simple_slow_sql_dict)

complex_slow_sql_dict = {'from_instance': '127.0.0.1:5432', 'datname': 'database1', 'schema': 'public',
                         'query': 'update schema1.table1 set age=30 where id=3', 'start_time': '1640139690000',
                         'finish_time': '1640139700000', 'hit_rate': '0.899', 'fetch_rate': '0.99', 'cpu_time': '14200',
                         'data_io_time': '1231243', 'unique_query_id': '12432453234', 'sort_count': '0',
                         'sort_mem_used': '0', 'sort_spill_count': '0', 'hash_count': '0', 'hash_mem_used': '0',
                         'hash_spill_count': '0', 'lock_wait_count': '2', 'lwlock_wait_count': '3',
                         'n_returned_rows': '100000', 'n_tuples_returned': '100000', 'n_tuples_fetched': '100000',
                         'n_tuples_inserted': '0', 'n_tuples_updated': '100000', 'n_tuples_deleted': 0}
complex_slow_sql_seq = Sequence(timestamps=[1640139695000],
                                values=[101],
                                name='pg_sql_statement_history_exec_time',
                                step=5,
                                labels=complex_slow_sql_dict)

commit_slow_sql_dict = {'from_instance': '127.0.0.1:5432', 'datname': 'database1', 'schema': 'public',
                        'query': 'COMMIT', 'start_time': '1640139790000',
                        'finish_time': '1640139700000', 'hit_rate': '0.988', 'fetch_rate': '0.99', 'cpu_time': '14200',
                        'data_io_time': '1231243', 'unique_query_id': '12432453234', 'sort_count': '13',
                        'sort_mem_used': '12.43', 'sort_spill_count': '3', 'hash_count': '0', 'hash_mem_used': '0',
                        'hash_spill_count': '0', 'lock_wait_count': '0', 'lwlock_wait_count': '0',
                        'n_returned_rows': '1', 'n_tuples_returned': '100000', 'n_tuples_fetched': '0',
                        'n_tuples_inserted': '0', 'n_tuples_updated': '0', 'n_tuples_deleted': 0}
commit_slow_sql_seq = Sequence(timestamps=[1640139695000],
                               values=[101],
                               name='pg_sql_statement_history_exec_time',
                               step=5,
                               labels=commit_slow_sql_dict)

system_slow_sql_dict = {'from_instance': '127.0.0.1:5432', 'datname': 'database1', 'schema': 'public',
                        'query': 'select * from PG_SETTINGS', 'start_time': '1640139890000',
                        'finish_time': '1640139700000', 'hit_rate': '0.988', 'fetch_rate': '0.99', 'cpu_time': '14200',
                        'data_io_time': '1231243', 'unique_query_id': '12432453234', 'sort_count': '13',
                        'sort_mem_used': '12.43', 'sort_spill_count': '3', 'hash_count': '0', 'hash_mem_used': '0',
                        'hash_spill_count': '0', 'lock_wait_count': '0', 'lwlock_wait_count': '0',
                        'n_returned_rows': '1', 'n_tuples_returned': '100000', 'n_tuples_fetched': '0',
                        'n_tuples_inserted': '0', 'n_tuples_updated': '0', 'n_tuples_deleted': 0}
system_slow_sql_seq = Sequence(timestamps=[1640139695000],
                               values=[101],
                               name='pg_sql_statement_history_exec_time',
                               step=5,
                               labels=system_slow_sql_dict)

shield_slow_sql_dict = {'from_instance': '127.0.0.1:5432', 'datname': 'database1', 'schema': 'public',
                        'query': 'CREATE TABLE table1(id int, name varchar(10))', 'start_time': '1640149690000',
                        'finish_time': '1640139700000', 'hit_rate': '0.988', 'fetch_rate': '0.99', 'cpu_time': '14200',
                        'data_io_time': '1231243', 'unique_query_id': '12432453234', 'sort_count': '13',
                        'sort_mem_used': '12.43', 'sort_spill_count': '3', 'hash_count': '0', 'hash_mem_used': '0',
                        'hash_spill_count': '0', 'lock_wait_count': '0', 'lwlock_wait_count': '0',
                        'n_returned_rows': '1', 'n_tuples_returned': '100000', 'n_tuples_fetched': '0',
                        'n_tuples_inserted': '0', 'n_tuples_updated': '0', 'n_tuples_deleted': 0}
shield_slow_sql_seq = Sequence(timestamps=[1640139695000],
                               values=[101],
                               name='pg_sql_statement_history_exec_time',
                               step=5,
                               labels=shield_slow_sql_dict)

locked_slow_sql_dict = {'from_instance': '127.0.0.1:5432', 'datname': 'database1', 'schema': 'public',
                        'query': 'CREATE TABLE table1(id int, name varchar(10))', 'start_time': '1640149690000',
                        'finish_time': '1640139700000', 'hit_rate': '0.988', 'fetch_rate': '0.99', 'cpu_time': '14200',
                        'data_io_time': '1231243', 'unique_query_id': '12432453234', 'sort_count': '13',
                        'sort_mem_used': '12.43', 'sort_spill_count': '3', 'hash_count': '0', 'hash_mem_used': '0',
                        'hash_spill_count': '0', 'lock_wait_count': '1', 'lwlock_wait_count': '1',
                        'n_returned_rows': '1', 'n_tuples_returned': '100000', 'n_tuples_fetched': '0',
                        'n_tuples_inserted': '0', 'n_tuples_updated': '0', 'n_tuples_deleted': 0}
locked_slow_sql_seq = Sequence(timestamps=[1640139695000],
                               values=[101],
                               name='pg_sql_statement_history_exec_time',
                               step=5,
                               labels=locked_slow_sql_dict)


class SimpleQueryMockedFetcher(LazyFetcher):
    def _fetch_sequence(self, start_time=None, end_time=None, step=None):
        return [simple_slow_sql_seq]


class ComplexQueryMockedFetcher(LazyFetcher):
    def _fetch_sequence(self, start_time=None, end_time=None, step=None):
        return [complex_slow_sql_seq]


class CommitQueryMockedFetcher(LazyFetcher):
    def _fetch_sequence(self, start_time=None, end_time=None, step=None):
        return [commit_slow_sql_seq]


class SystemQueryMockedFetcher(LazyFetcher):
    def _fetch_sequence(self, start_time=None, end_time=None, step=None):
        return [system_slow_sql_seq]


class ShieldQueryMockedFetcher(LazyFetcher):
    def _fetch_sequence(self, start_time=None, end_time=None, step=None):
        return [shield_slow_sql_seq]


class LockedQueryMockedFetcher(LazyFetcher):
    def _fetch_sequence(self, start_time=None, end_time=None, step=None):
        return [locked_slow_sql_seq]
