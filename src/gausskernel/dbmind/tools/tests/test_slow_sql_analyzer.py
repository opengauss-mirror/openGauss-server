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
import configparser
from typing import List
from unittest import mock

import numpy as np

from dbmind.app.diagnosis.query.slow_sql import analyzer
from dbmind.app.diagnosis.query.slow_sql import query_feature
from dbmind.app.diagnosis.query.slow_sql import query_info_source
from dbmind.app.diagnosis.query.slow_sql.analyzer import SlowSQLAnalyzer
from dbmind.app.diagnosis.query.slow_sql.featurelib import load_feature_lib, get_feature_mapper
from dbmind.app.diagnosis.query.slow_sql.query_info_source import TableStructure, DatabaseInfo, LockInfo, SystemInfo
from dbmind.common.types import Sequence
from dbmind.service import dai
from dbmind.app.diagnosis.query.slow_sql.significance_detection import average_base, ks_base, sum_base
from dbmind.service.dai import _AbstractLazyFetcher


big_current_data = [10, 10, 10, 10, 10]
big_history_data = [1, 1, 1, 1, 1]

small_current_data = [2, 1, 3, 1, 0]
small_history_data = [1, 1, 1, 1, 1]

ilegal_current_data = []
ilegal_history_data = [1, 1, 1, 1, 1]


configs = configparser.ConfigParser()
configs.add_section('slow_sql_threshold')
configs.set('slow_sql_threshold', 'tuple_number_limit', '1000')
configs.set('slow_sql_threshold', 'dead_rate_limit', '0.2')
configs.set('slow_sql_threshold', 'fetch_tuples_limit', '1000')
configs.set('slow_sql_threshold', 'fetch_rate_limit', '0.6')
configs.set('slow_sql_threshold', 'returned_rows_limit', '1000')
configs.set('slow_sql_threshold', 'returned_rate_limit', '0.6')
configs.set('slow_sql_threshold', 'hit_rate_limit', '0.9')
configs.set('slow_sql_threshold', 'updated_tuples_limit', '1000')
configs.set('slow_sql_threshold', 'updated_rate_limit', '0.6')
configs.set('slow_sql_threshold', 'inserted_tuples_limit', '1000')
configs.set('slow_sql_threshold', 'inserted_rate_limit', '0.6')
configs.set('slow_sql_threshold', 'index_number_limit', '3')
configs.set('slow_sql_threshold', 'deleted_tuples_limit', '1000')
configs.set('slow_sql_threshold', 'deleted_rate_limit', '0.6')
configs.set('slow_sql_threshold', 'tps_limit', '100')
configs.set('slow_sql_threshold', 'iops_limit', '100')
configs.set('slow_sql_threshold', 'iowait_limit', '0.2')
configs.set('slow_sql_threshold', 'ioutils_limit', '0.8')
configs.set('slow_sql_threshold', 'iocapacity_limit', '100')
configs.set('slow_sql_threshold', 'cpu_usage_limit', '0.5')
configs.set('slow_sql_threshold', 'load_average_rate_limit', '0.5')

query_feature._get_threshold = mock.Mock(side_effect=lambda x: configs.getfloat('slow_sql_threshold', x))


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


class SimpleQueryMockedFetcher(_AbstractLazyFetcher):
    def _real_fetching_action(self) -> List[Sequence]:
        return [simple_slow_sql_seq]


class ComplexQueryMockedFetcher(_AbstractLazyFetcher):
    def _real_fetching_action(self) -> List[Sequence]:
        return [complex_slow_sql_seq]


class CommitQueryMockedFetcher(_AbstractLazyFetcher):
    def _real_fetching_action(self) -> List[Sequence]:
        return [commit_slow_sql_seq]


class SystemQueryMockedFetcher(_AbstractLazyFetcher):
    def _real_fetching_action(self) -> List[Sequence]:
        return [system_slow_sql_seq]


class ShieldQueryMockedFetcher(_AbstractLazyFetcher):
    def _real_fetching_action(self) -> List[Sequence]:
        return [shield_slow_sql_seq]


class LockedQueryMockedFetcher(_AbstractLazyFetcher):
    def _real_fetching_action(self) -> List[Sequence]:
        return [locked_slow_sql_seq]


class MockedSimpleQueryContext:
    @staticmethod
    def acquire_pg_class():
        return {'database1': {'schema1': ['table1', 'table2'], 'schema2': ['table3'], 'public': []}}

    @staticmethod
    def acquire_fetch_interval():
        return 5

    @staticmethod
    def acquire_lock_info():
        return LockInfo()

    @staticmethod
    def acquire_tables_structure_info():
        return [TableStructure()]

    @staticmethod
    def acquire_database_info():
        return DatabaseInfo()

    @staticmethod
    def acquire_system_info():
        return SystemInfo()


class MockedComplexQueryContext:
    @staticmethod
    def acquire_pg_class():
        return {'database1': {'schema1': ['table1', 'table2'], 'schema2': ['table3'], 'public': []}}

    @staticmethod
    def acquire_fetch_interval():
        return 5

    @staticmethod
    def acquire_lock_info():
        lock_info = LockInfo()
        lock_info.db_host = '127.0.0.1'
        lock_info.locked_query = ['update schema1.table1 set age=30 where id=3']
        lock_info.locked_query_start = [1640139690000]
        lock_info.locker_query = ['vacuum full']
        lock_info.locker_query_start = [1640139900000]
        return lock_info

    @staticmethod
    def acquire_tables_structure_info():
        table_info = TableStructure()
        table_info.db_host = '127.0.0.1'
        table_info.db_port = '5432'
        table_info.db_name = 'database1'
        table_info.schema_name = 'schema1'
        table_info.table_name = 'table1'
        table_info.dead_tuples = 8000
        table_info.live_tuples = 10000
        table_info.dead_rate = 0.7
        table_info.last_autovacuum = 1640139690000
        table_info.last_autoanalyze = 1640139690000
        table_info.analyze = 1640139690
        table_info.vacuum = 1640139690
        table_info.table_size = 10000
        table_info.index = ['index1', 'index2', 'index3', 'index4']
        table_info.redundant_index = ['redundant_index1', 'redundant_index2', 'redundant_index3', 'redundant_index4']
        return [table_info]

    @staticmethod
    def acquire_database_info():
        db_info = DatabaseInfo()
        db_info.db_host = '127.0.0.1'
        db_info.db_port = '8080'
        db_info.history_tps = [100]
        db_info.current_tps = [100000]
        db_info.max_conn = 100
        db_info.used_conn = 99
        return db_info

    @staticmethod
    def acquire_system_info():
        system_info = SystemInfo()
        system_info.db_host = '127.0.0.1'
        system_info.db_port = '8080'
        system_info.iops = 10000
        system_info.ioutils = {'sdm-0': 0.9}
        system_info.iocapacity = 10000
        system_info.iowait = 0.7
        system_info.cpu_usage = 0.9
        system_info.mem_usage = 0.9
        system_info.load_average = 0.9
        return system_info


dai.get_latest_metric_sequence = mock.Mock(side_effect=[SimpleQueryMockedFetcher(),
                                                        ComplexQueryMockedFetcher(),
                                                        LockedQueryMockedFetcher(),
                                                        SimpleQueryMockedFetcher(),
                                                        CommitQueryMockedFetcher(),
                                                        SystemQueryMockedFetcher(),
                                                        SimpleQueryMockedFetcher(),
                                                        ShieldQueryMockedFetcher()])
query_info_source.QueryContext = mock.Mock(side_effect=[MockedSimpleQueryContext,
                                                        MockedComplexQueryContext,
                                                        MockedSimpleQueryContext,
                                                        MockedSimpleQueryContext,
                                                        MockedSimpleQueryContext,
                                                        MockedSimpleQueryContext,
                                                        MockedSimpleQueryContext,
                                                        MockedSimpleQueryContext])


def test_average_base():
    check_res_1 = average_base.detect(big_current_data, big_history_data, method='bool')
    check_res_2 = average_base.detect(small_current_data, small_history_data, method='bool')
    check_res_3 = average_base.detect(ilegal_current_data, ilegal_history_data, method='bool')
    check_res_4 = average_base.detect(big_current_data, big_history_data, method='other')
    check_res_5 = average_base.detect(big_history_data, big_current_data, method='other')
    try:
        _ = average_base.detect(100, 200)
    except TypeError as execinfo:
        assert 'The format of the input data is wrong' in str(execinfo)
    try:
        _ = average_base.detect(big_current_data, big_history_data, method='inner')
    except ValueError as execinfo:
        assert 'Not supported method' in str(execinfo)
    assert check_res_1
    assert not check_res_2
    assert not check_res_3
    assert round(check_res_4, 4) == 0.9000
    assert check_res_5 == 0


def test_ks_base():
    check_res_1 = ks_base.detect(big_current_data, big_history_data)
    check_res_2 = ks_base.detect(small_current_data, small_history_data)
    check_res_3 = ks_base.detect(ilegal_current_data, ilegal_history_data)
    assert check_res_1
    assert not check_res_2
    assert not check_res_3


def test_sum_base():
    check_res_1 = sum_base.detect(big_current_data, big_history_data, method='bool')
    check_res_2 = sum_base.detect(small_current_data, small_history_data, method='bool')
    check_res_3 = sum_base.detect(ilegal_current_data, ilegal_history_data, method='bool')
    check_res_4 = sum_base.detect(big_current_data, big_history_data, method='other')
    check_res_5 = sum_base.detect(big_history_data, big_current_data, method='other')
    try:
        _ = sum_base.detect(100, 200)
    except TypeError as execinfo:
        assert 'The format of the input data is wrong' in str(execinfo)
    try:
        _ = sum_base.detect(big_current_data, big_history_data, method='inner')
    except ValueError as execinfo:
        assert 'Not supported method' in str(execinfo)
    assert check_res_1
    assert not check_res_2
    assert not check_res_3
    assert round(check_res_4, 4) == 0.9000
    assert check_res_5 == 0


def test_load_feature_lib():
    feature_lib = load_feature_lib()
    assert len(feature_lib) == 3
    assert len(feature_lib['features']) > 0
    assert len(feature_lib['labels']) > 0
    assert len(feature_lib['weight_matrix']) > 0


def test_get_feature_mapper():
    feature_mapping = get_feature_mapper()
    assert len(feature_mapping) == 20


def test_vector_distance():
    feature_lib = load_feature_lib()
    features, causes, weight_matrix = feature_lib['features'], feature_lib['labels'], feature_lib['weight_matrix']
    feature_instance1 = np.array([1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    feature_instance2 = np.array([1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    distance = analyzer._vector_distance(feature_instance1, features[0], 1, weight_matrix)
    assert round(distance, 4) == 0.9985
    try:
        _ = analyzer._vector_distance(feature_instance2, features[0], 1, weight_matrix)
    except ValueError as execinfo:
        assert 'not equal' in str(execinfo)


def test_euclid_distance():
    feature1 = np.array([1, 1, 0, 0, 0])
    feature2 = np.array([0, 1, 0, 0, 0])
    distance = analyzer._euclid_distance(feature1, feature2)
    assert distance == 1.0


def test_calculate_nearest_feature():
    feature = np.array([1, 1, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
    nearest_feature = analyzer._calculate_nearest_feature(feature, topk=3)
    assert len(nearest_feature) == 3
    assert nearest_feature[0][1] == 11
    assert nearest_feature[1][1] == 6
    assert nearest_feature[2][1] == 17


def test_simple_query_feature():
    slow_sql_instance = dai.get_all_slow_queries(minutes=5)[0]
    data_factory = query_info_source.QueryContext(slow_sql_instance)
    query_f = query_feature.QueryFeature(slow_sql_instance, data_factory)
    query_f.initialize_metrics()
    features, detail = query_f()
    assert query_f.select_type
    assert not query_f.update_type
    assert not query_f.delete_type
    assert not query_f.insert_type
    assert not query_f.other_type
    assert not query_f.query_block
    assert not query_f.large_table
    assert not query_f.large_dead_tuples
    assert query_f.large_fetch_tuples
    assert not query_f.large_returned_rows
    assert not query_f.lower_hit_ratio
    assert not query_f.update_redundant_index
    assert not query_f.insert_redundant_index
    assert not query_f.delete_redundant_index
    assert not query_f.large_updated_tuples
    assert not query_f.large_inserted_tuples
    assert not query_f.index_number_insert
    assert not query_f.large_deleted_tuples
    assert query_f.external_sort
    assert not query_f.vacuum_operation
    assert not query_f.analyze_operation
    assert not query_f.tps_significant_change
    assert not query_f.large_iowait
    assert not query_f.large_iops
    assert not query_f.large_load_average
    assert not query_f.large_cpu_usage
    assert not query_f.large_ioutils
    assert not query_f.large_iocapacity
    assert features == [0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0]


def test_complex_query_feature():
    slow_sql_instance = dai.get_all_slow_queries(minutes=5)[0]
    data_factory = query_info_source.QueryContext(slow_sql_instance)
    query_f = query_feature.QueryFeature(slow_sql_instance, data_factory)
    query_f.initialize_metrics()
    _, _ = query_f()
    assert not query_f.select_type
    assert query_f.update_type
    assert query_f.query_block
    assert query_f.large_table
    assert not query_f.external_sort
    assert query_f.large_dead_tuples
    assert query_f.large_updated_tuples
    assert query_f.update_redundant_index
    assert query_f.lower_hit_ratio
    assert query_f.redundant_index
    assert query_f.large_index_number
    assert query_f.vacuum_operation
    assert query_f.analyze_operation
    assert query_f.tps_significant_change
    assert query_f.large_iops
    assert query_f.large_iowait
    assert query_f.large_ioutils
    assert query_f.large_iocapacity
    assert query_f.large_cpu_usage
    assert query_f.large_load_average


def test_locked_query_feature():
    slow_sql_instance = dai.get_all_slow_queries(minutes=5)[0]
    data_factory = query_info_source.QueryContext(slow_sql_instance)
    query_f = query_feature.QueryFeature(slow_sql_instance, data_factory)
    query_f.initialize_metrics()
    _, _ = query_f()
    assert query_f.query_block
    assert query_f.other_type


def test_slow_analyzer():
    _analyzer = SlowSQLAnalyzer(topk=3)
    simple_slow_sql_instances = dai.get_all_slow_queries(minutes=5)
    commit_slow_sql_instances = dai.get_all_slow_queries(minutes=5)
    system_slow_sql_instances = dai.get_all_slow_queries(minutes=5)
    repeat_slow_sql_instances = dai.get_all_slow_queries(minutes=5)
    shield_slow_sql_instances = dai.get_all_slow_queries(minutes=5)
    _analyzer.run(simple_slow_sql_instances[-1])
    assert sum(item in simple_slow_sql_instances[-1].root_causes for item in ('EXTERNAL_SORT', 'LARGE_FETCHED_TUPLES'))
    _analyzer.run(commit_slow_sql_instances[-1])
    assert sum(item in commit_slow_sql_instances[-1].root_causes for item in ('UNKNOWN'))
    _analyzer.run(system_slow_sql_instances[-1])
    assert sum(item in system_slow_sql_instances[-1].root_causes for item in ('DATABASE_VIEW'))
    _analyzer.run(repeat_slow_sql_instances[-1])
    assert not repeat_slow_sql_instances[-1].root_causes
    _analyzer.run(shield_slow_sql_instances[-1])
    assert sum(item in shield_slow_sql_instances[-1].root_causes for item in ('ILLEGAL_SQL'))
