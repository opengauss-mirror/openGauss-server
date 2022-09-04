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
from unittest import mock
import pytest

import numpy as np

from dbmind import global_vars
from dbmind.app.diagnosis.query.slow_sql import analyzer
from dbmind.app.diagnosis.query.slow_sql import query_feature
from dbmind.app.diagnosis.query.slow_sql import query_info_source
from dbmind.app import monitoring
from dbmind.common.types.slow_query import SlowQuery
from dbmind.app.diagnosis.query.slow_sql.analyzer import SlowSQLAnalyzer
from dbmind.app.diagnosis.query.slow_sql.featurelib import load_feature_lib, get_feature_mapper
from dbmind.app.diagnosis.query.slow_sql.significance_detection import average_base, ks_base, sum_base
from dbmind.service import dai

big_current_data = [10, 10, 10, 10, 10]
big_history_data = [1, 1, 1, 1, 1]

small_current_data = [2, 1, 3, 1, 0]
small_history_data = [1, 1, 1, 1, 1]

ilegal_current_data = []
ilegal_history_data = [1, 1, 1, 1, 1]

configs = configparser.ConfigParser()
configs.add_section('detection_params')
configs.set('detection_params', 'disk_usage_threshold', '0.3')
configs.set('detection_params', 'mem_usage_threshold', '0.4')
configs.set('detection_params', 'cpu_usage_threshold', '0.3')
configs.set('detection_params', 'tps_threshold', '2000')
configs.set('detection_params', 'p80_threshold', '260')
configs.set('detection_params', 'io_capacity_threshold', '0.6')
configs.set('detection_params', 'io_delay_threshold', '0.9')
configs.set('detection_params', 'io_wait_threshold', '1000')
configs.set('detection_params', 'load_average_threshold', '0.6')
configs.set('detection_params', 'iops_threshold', '1000')
configs.set('detection_params', 'handler_occupation_threshold', '0.6')
configs.set('detection_params', 'disk_ioutils_threshold', '3')
configs.set('detection_params', 'connection_rate_threshold', '1000')
configs.set('detection_params', 'connection_usage_threshold', '0.6')
configs.set('detection_params', 'package_drop_rate_threshold', '100')
configs.set('detection_params', 'package_error_rate_threshold', '100')
configs.set('detection_params', 'bgwriter_rate_threshold', '0.2')
configs.set('detection_params', 'replication_write_diff_threshold', '100000')
configs.set('detection_params', 'replication_sent_diff_threshold', '100000')
configs.set('detection_params', 'replication_replay_diff_threshold', '100000')
configs.set('detection_params', 'thread_occupy_rate_threshold', '0.95')
configs.set('detection_params', 'idle_session_occupy_rate_threshold', '0.3')

configs.add_section('slow_sql_threshold')
configs.set('slow_sql_threshold', 'tuple_number_threshold', '5000')
configs.set('slow_sql_threshold', 'table_total_size_threshold', '2048')
configs.set('slow_sql_threshold', 'fetch_tuples_threshold', '10000')
configs.set('slow_sql_threshold', 'returned_rows_threshold', '1000')
configs.set('slow_sql_threshold', 'updated_tuples_threshold', '1000')
configs.set('slow_sql_threshold', 'deleted_tuples_threshold', '1000')
configs.set('slow_sql_threshold', 'inserted_tuples_threshold', '1000')
configs.set('slow_sql_threshold', 'hit_rate_threshold', '0.95')
configs.set('slow_sql_threshold', 'dead_rate_threshold', '0.2')
configs.set('slow_sql_threshold', 'index_number_threshold', '3')
configs.set('slow_sql_threshold', 'index_number_rate_threshold', '0.6')
configs.set('slow_sql_threshold', 'update_statistics_threshold', '60')
configs.set('slow_sql_threshold', 'nestloop_rows_threshold', '10000')
configs.set('slow_sql_threshold', 'hashjoin_rows_threshold', '10000')
configs.set('slow_sql_threshold', 'groupagg_rows_threshold', '10000')
configs.set('slow_sql_threshold', 'cost_rate_threshold', '0.4')
configs.set('slow_sql_threshold', 'plan_time_occupy_rate_threshold', '0.3')
configs.set('slow_sql_threshold', 'used_index_tuples_rate_threshold', '0.2')


slow_sql_instance = SlowQuery(db_host='127.0.0.1', db_port='8080', db_name='database1', schema_name='schema1',
                              query='update schema1.table1 set age=30 where id=3', start_timestamp=1640139691000,
                              duration_time=1000, track_parameter=True, plan_time=1000, parse_time=20, db_time=2000,
                              hit_rate=0.8, fetch_rate=0.98, cpu_time=14200, data_io_time=1231243,
                              template_id=12432453234, query_plan=None,
                              sort_count=13, sort_mem_used=12.43, sort_spill_count=3, hash_count=0, hash_mem_used=0,
                              hash_spill_count=0, lock_wait_count=10, lwlock_wait_count=20, n_returned_rows=1,
                              n_tuples_returned=100000, n_tuples_fetched=0, n_tuples_deleted=0, n_tuples_inserted=0,
                              n_tuples_updated=0)
slow_sql_instance.tables_name = {'schema1': ['table1']}


def mock_get_param(param):
    return configs.getfloat('detection_params', param)


def mock_get_threshold(param):
    return configs.getfloat('slow_sql_threshold', param)


@pytest.fixture
def mock_get_funcntion(monkeypatch):
    monkeypatch.setattr(monitoring, 'get_param', mock.Mock(side_effect=lambda x: mock_get_param(param=x)))
    monkeypatch.setattr(monitoring, 'get_threshold', mock.Mock(side_effect=lambda x: mock_get_threshold(param=x)))


class MockedComplexQueryContext(query_info_source.QueryContext):
    def __init__(self, slow_sql_instance): 
        super().__init__(slow_sql_instance)

    @staticmethod
    def acquire_pg_class():
        pg_class_1 = query_info_source.PgClass()
        pg_class_2 = query_info_source.PgClass()
        pg_class_3 = query_info_source.PgClass()
        pg_class_1.db_name = 'database1'
        pg_class_1.schema_name = 'schma1'
        pg_class_1.relname = 'table1'
        pg_class_2.db_name = 'database1'
        pg_class_2.schema_name = 'schma1'
        pg_class_2.relname = 'table2'
        pg_class_3.db_name = 'database1'
        pg_class_3.schema_name = 'schma2'
        pg_class_3.relname = 'table3'
        return [pg_class_1, pg_class_2, pg_class_3]

    @staticmethod
    def acquire_fetch_interval():
        return 5

    @staticmethod
    def acquire_lock_info():
        lock_info = query_info_source.LockInfo()
        lock_info.db_host = '127.0.0.1'
        lock_info.locked_query = ['update schema1.table1 set age=30 where id=3']
        lock_info.locked_query_start = [1640139691000]
        lock_info.locker_query = ['update schema1.table1 set age=80 where id=3']
        lock_info.locker_query_start = [1640139690000]
        return lock_info

    @staticmethod
    def acquire_tables_structure_info():
        table_info = query_info_source.TableStructure()
        table_info.db_host = '127.0.0.1'
        table_info.db_port = '5432'
        table_info.db_name = 'database1'
        table_info.schema_name = 'schema1'
        table_info.table_name = 'table1'
        table_info.dead_tuples = 80000
        table_info.live_tuples = 100000
        table_info.dead_rate = 0.45
        table_info.last_autovacuum = 1640139691000
        table_info.last_autoanalyze = 1640139691000
        table_info.analyze = 1640139691000
        table_info.vacuum = 1640139691000
        table_info.table_size = 1000000
        table_info.skew_ratio = 0.5
        table_info.index = {'index1': ['col1'], 'index2': ['col2'], 'index3': ['col3']}
        table_info.redundant_index = ['redundant_index1', 'redundant_index2', 'redundant_index3', 'redundant_index4']
        return [table_info]

    @staticmethod
    def acquire_pg_settings():
        enable_nestloop_setting = query_info_source.PgSetting()
        enable_nestloop_setting.name = 'enable_nestloop'
        enable_nestloop_setting.vartype = 'bool'
        enable_nestloop_setting.setting = True
        pg_setting_info['enable_nestloop'] = enable_nestloop_setting 

        return pg_setting_info

    @staticmethod
    def acquire_database_info():
        db_info = query_info_source.DatabaseInfo()
        db_info.db_host = '127.0.0.1'
        db_info.db_port = '8080'
        db_info.history_tps = [100, 100]
        db_info.current_tps = [100000, 100000]
        db_info.max_conn = 100
        db_info.used_conn = 99
        db_info.thread_pool = {'worker_info_default': 100, 'worker_info_idle': 90, 'session_info_total': 50, 'session_info_idle': 40}
        return db_info

    @staticmethod
    def acquire_wait_event():
        wait_event_info = query_info_source.WaitEvent()
        wait_event_info.node_name = 'node1'
        wait_event_info.type = 'IO_EVENT'
        wait_event_info.event = 'CopyFileWrite'
        return [wait_event_info] 

    @staticmethod
    def acquire_system_info():
        system_info = query_info_source.SystemInfo()
        system_info.db_host = '127.0.0.1'
        system_info.db_port = '8080'
        system_info.iops = 100000
        system_info.ioutils = {'sdm-0': 0.9}
        system_info.iocapacity = 100000
        system_info.iowait = 0.9
        system_info.cpu_usage = 0.9
        system_info.mem_usage = 0.9
        system_info.disk_usage = {'sdm-0': 0.8}
        system_info.process_fds_rate = 0.8
        system_info.load_average = 0.9
        system_info.io_read_delay_time = 1000000
        system_info.io_write_delay_time = 1000000
        system_info.io_queue_number = 100000
        return system_info

    @staticmethod
    def acquire_network_info():
        network_info = query_info_source.NetWorkInfo()
        network_info.receive_bytes = 10000000
        network_info.transmit_bytes = 10000000
        network_info.transmit_drop = 0.9 
        network_info.transmit_error = 0.9
        network_info.transmit_packets = 1000000
        network_info.receive_drop = 0.9
        network_info.receive_error = 0.9 
        network_info.receive_packets = 1000000
        return network_info
    
    @staticmethod
    def acquire_bgwriter_info():
        bgwriter_info = query_info_source.BgWriter()
        bgwriter_info.buffers_checkpoint = 100
        bgwriter_info.buffers_clean = 100
        bgwriter_info.buffers_backend = 100000000
        bgwriter_info.buffers_alloc = 100
        return bgwriter_info

    @staticmethod
    def acquire_pg_replication_info():
        pg_replication_info = query_info_source.PgReplicationInfo()
        pg_replication_info.application_name = 'WalSender to Standby[dn_6002]' 
        pg_replication_info.pg_replication_lsn = 1667524422792
        pg_replication_info.pg_replication_write_diff = 10000000
        pg_replication_info.pg_replication_sent_diff = 100000000
        pg_replication_info.pg_replication_flush_diff = 10000000
        pg_replication_info.pg_replication_replay_diff = 10000000
        return [pg_replication_info]

    @staticmethod
    def acquire_sql_count_info():
        gs_sql_count_info = query_info_source.GsSQLCountInfo()
        gs_sql_count_info.select_count = 1000
        gs_sql_count_info.delete_count = 1000
        gs_sql_count_info.insert_count = 1000
        gs_sql_count_info.update_count = 1000
        return gs_sql_count_info

    @staticmethod
    def acquire_rewritten_sql():
        return ''

    @staticmethod
    def acquire_recommend_index():
        return 'schema: schema1, table: table1, column: id'

    @staticmethod
    def acquire_timed_task():
        timed_task_info = query_info_source.TimedTask()
        timed_task_info.job_id = 1
        timed_task_info.priv_user = 'user'
        timed_task_info.dbname = 'database1'
        timed_task_info.job_status = 1
        timed_task_info.last_start_date = 1640139688000
        timed_task_info.last_end_date = 1640139693000
        return [timed_task_info] 


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
    assert not check_res_1
    assert check_res_2
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
    assert len(feature_mapping) == 32


def test_vector_distance():
    feature_lib = load_feature_lib()
    features, causes, weight_matrix = feature_lib['features'], feature_lib['labels'], feature_lib['weight_matrix']
    feature_instance1 = np.array([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    feature_instance2 = np.array([1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    distance = analyzer._vector_distance(feature_instance1, features[0], 1, weight_matrix)
    assert round(distance, 4) == 0.9981
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
    feature = np.array([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    nearest_feature = analyzer._calculate_nearest_feature(feature)
    assert len(nearest_feature) == 1
    assert nearest_feature[0][0] == 1
    assert nearest_feature[0][1] == 13


def test_query_feature(mock_get_funcntion):
    query_context = MockedComplexQueryContext(slow_sql_instance)
    feature_generator = query_feature.QueryFeature(query_context)
    feature_generator.initialize_metrics()
    features, detail, suggestions = feature_generator() 
    assert features == [1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 0, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 1]


def test_slow_analyzer(mock_get_funcntion):
    query_context = MockedComplexQueryContext(slow_sql_instance)
    analyzer = SlowSQLAnalyzer()
    analyzer.run(query_context)
    assert 'ABNORMAL_SEQSCAN_OPERATOR' in query_context.slow_sql_instance.root_causes
    assert 'LOW_REPLICATION_EFFICIENT' in query_context.slow_sql_instance.root_causes
    assert 'LOW_CHECKPOINT_EFFICIENT' in query_context.slow_sql_instance.root_causes
    assert 'LOCK_CONTENTION_SQL' in query_context.slow_sql_instance.root_causes
    assert 'LARGE_DEAD_RATE' in query_context.slow_sql_instance.root_causes
    assert 'UPDATED_REDUNDANT_INDEX' in query_context.slow_sql_instance.root_causes
    assert 'LARGE_FETCHED_TUPLES' in query_context.slow_sql_instance.root_causes
    assert 'LOAD_CONCENTRATION' in query_context.slow_sql_instance.root_causes
    assert 'EXTERNAL_SORT' in query_context.slow_sql_instance.root_causes
    assert 'ANALYZE_CONFLICT' in query_context.slow_sql_instance.root_causes
    assert 'VACUUM_CONFLICT' in query_context.slow_sql_instance.root_causes
    assert 'SMALL_SHARED_BUFFER_SQL' in query_context.slow_sql_instance.root_causes
    assert 'ABNORMAL_PLAN_TIME' in query_context.slow_sql_instance.root_causes
    assert 'ABNORMAL_THREAD_POOL' in query_context.slow_sql_instance.root_causes
    assert 'SYSTEM_RESOURCE' in query_context.slow_sql_instance.root_causes
    
