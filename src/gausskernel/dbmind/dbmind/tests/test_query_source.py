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

from unittest import mock
import pytest
import configparser

from dbmind import global_vars

configs = configparser.ConfigParser()
configs.add_section('SELF-MONITORING')
configs.set('SELF-MONITORING', 'result_storage_retention', '30')
configs.set('SELF-MONITORING', 'detection_interval', '30')
configs.set('SELF-MONITORING', 'last_detection_time', '30')
configs.set('SELF-MONITORING', 'forecasting_future_time', '30')
configs.set('SELF-MONITORING', 'golden_kpi', 'os_cpu_usage, os_mem_usage, os_disk_usage')
configs.add_section('SELF-OPTIMIZATION')
configs.set('SELF-OPTIMIZATION', 'max_reserved_period', '30')
configs.set('SELF-OPTIMIZATION', 'max_template_num', '30')
configs.set('SELF-OPTIMIZATION', 'optimization_interval', '30')
configs.set('SELF-OPTIMIZATION', 'kill_slow_query', 'True')
configs.set('SELF-OPTIMIZATION', 'max_index_storage', '100')
configs.set('SELF-OPTIMIZATION', 'max_index_num', '100')
configs.add_section('SELF-HEALING')
configs.set('SELF-HEALING', 'enable_self_healing', 'True')
configs.add_section('SELF-DIAGNOSIS')
configs.set('SELF-DIAGNOSIS', 'diagnosis_time_window', '30')
global_vars.configs = configs

from dbmind.app.diagnosis.query.slow_sql import query_info_source
from dbmind.common.types import Sequence, SlowQuery
from dbmind.service import dai

pg_class_relsize_dict = {'datname': 'database1', 'nspname': 'schema1', 'relname': 'table1', 
                         'relkind': 'r', 'relhasindex': 'True', 'relsize': 100}
pg_lock_sql_locked_times_dict = {'locked_query': 'update table2 set age=20 where id=3',
                                 'locked_query_start': 1640139695,
                                 'locker_query': 'delete from table2 where id=3',
                                 'locker_query_start': 1640139690}
pg_tables_structure_dead_rate_dict = {'datname': 'database1', 'schemaname': 'schema1', 'relname': 'table1'}
pg_tables_structure_n_live_tup_dict = {'datname': 'database1', 'schemaname': 'schema1', 'relname': 'table1'}
pg_tables_structure_n_dead_tup_dict = {'datname': 'database1', 'schemaname': 'schema1', 'relname': 'table1'}
pg_tables_structure_column_number_dict = {'datname': 'database1', 'schemaname': 'schema1', 'relname': 'table1'}
pg_tables_structure_last_vacuum_dict = {'datname': 'database1', 'schemaname': 'schema1', 'relname': 'table1'}
pg_tables_structure_last_analyze_dict = {'datname': 'database1', 'schemaname': 'schema1', 'relname': 'table1'}
pg_tables_structure_last_autovacuum_dict = {'datname': 'database1', 'schemaname': 'schema1', 'relname': 'table1'}
pg_tables_structure_last_autoanalyze_dict = {'datname': 'database1', 'schemaname': 'schema1', 'relname': 'table1'}
pg_never_used_indexes_index_size_dict = {'datname': 'database1', 'schemaname': 'schema1', 'relname': 'table1',
                                         'indexrelname': 'table_index1'}
pg_table_skewness_skewstddev_dict = {'datname': 'database1', 'schemaname': 'schema1', 'relname': 'table1', 'skewratio': 0.3}

pg_tables_structure_dict = {'datname': 'database1', 'schemaname': 'schema1', 'relname': 'table1',
                            'n_live_tup': 10000,
                            'n_dead_tup': 100, 'dead_rate': 0.01, 'last_vacuum': None,
                            'last_autovacuum': None,
                            'last_analyze': None, 'last_autoanalyze': None}
pg_settings_dict = {'name': 'shared_buffers', 'vartype': 'int64', 'setting': 100}
gaussdb_qps_by_instance_dict = {'instance': '127.0.0.1:5432'}
pg_connections_max_conn_dict = {'instance': '127.0.0.1:5432'}
pg_connections_used_conn_dict = {'instance': '127.0.0.1:5432'}
pg_wait_events_last_updated_dict = {'nodename': 'node1', 'type': 'IO_EVENT', 'event': 'CopyFileWrite'}
pg_thread_pool_listener_dict = {'worker_info': 'default: 250 new: 0 expect: 250 actual: 250 idle: 250 pending: 0',
                                'session_info': 'total: 4 waiting: 0 running:0 idle: 4', 'group_id': 1, 'listener': 1}
pg_tables_size_bytes_dict = {'datname': 'database1', 'nspname': 'schema1', 'relname': 'tables'}
pg_index_idx_scan_dict = {'datname': 'database1', 'nspname': 'schema1', 'tablename': 'table1', 
                          'relname': 'index1', 'indexdef': 'CREATE INDEX index1 ON table1 USING btree (col1) TABLESPACE pg_default'}


os_disk_iops_dict = {'instance': '127.0.0.1:5432'}
os_disk_ioutils_dict = {'instance': '127.0.0.1:5432', 'device': 'sdm-0'}
os_disk_usage_dict = {'instance': '127.0.0.1:5432', 'device': 'sdm-0'}
os_cpu_iowait_dict = {'instance': '127.0.0.1:5432'}
os_disk_iocapacity_dict = {'instance': '127.0.0.1:5432'}
os_cpu_usage_dict = {'instance': '127.0.0.1:5432'}
os_mem_usage_dict = {'instance': '127.0.0.1:5432'}
node_load1_dict = {'instance': '127.0.0.1:5432'}
load_average_dict = {'instance': '127.0.0.1:5432'}
io_write_delay_time_dict = {'instance': '127.0.0.1:5432', 'device': 'sdm-0'}
io_read_delay_time_dict = {'instance': '127.0.0.1:5432', 'device': 'sdm-0'}
io_queue_number_time_dict = {'instance': '127.0.0.1:5432', 'device': 'sdm-0'}
node_process_fds_rate_dict = {'instance': '127.0.0.1:5432'}

node_network_receive_bytes_dict = {'instance': '127.0.0.1'}
node_network_transmit_bytes_dict = {'instance': '127.0.0.1'}
node_network_receive_drop_dict = {'instance': '127.0.0.1'}
node_network_transmit_drop_dict = {'instance': '127.0.0.1'}
node_network_receive_packets_dict = {'instance': '127.0.0.1'}
node_network_transmit_packets_dict = {'instance': '127.0.0.1'}
node_network_receive_error_dict = {'instance': '127.0.0.1'}
node_network_transmit_error_dict = {'instance': '127.0.0.1'}

pg_stat_bgwriter_buffers_checkpoint_dict = {'instance': '127.0.0.1:5432'}
pg_stat_bgwriter_buffers_clean_dict = {'instance': '127.0.0.1:5432'}
pg_stat_bgwriter_buffers_backend_dict = {'instance': '127.0.0.1:5432'}
pg_stat_bgwriter_buffers_alloc_dict = {'instance': '127.0.0.1:5432'}

pg_replication_lsn_dict = {'instance': '127.0.0.1:5432', 'application_name': 'WalSender to Standby[dn_6002]'}
pg_replication_sent_diff_dict = {'instance': '127.0.0.1:5432', 'application_name': 'WalSender to Standby[dn_6002]'}
pg_replication_write_diff_dict = {'instance': '127.0.0.1:5432', 'application_name': 'WalSender to Standby[dn_6002]'}
pg_replication_flush_diff_dict = {'instance': '127.0.0.1:5432', 'application_name': 'WalSender to Standby[dn_6002]'}
pg_replication_replay_diff_dict = {'instance': '127.0.0.1:5432', 'application_name': 'WalSender to Standby[dn_6002]'}

gs_sql_count_select_dict = {'instance': '127.0.0.1:5432'}
gs_sql_count_update_dict = {'instance': '127.0.0.1:5432'}
gs_sql_count_delete_dict = {'instance': '127.0.0.1:5432'}
gs_sql_count_insert_dict = {'instance': '127.0.0.1:5432'}

timed_task_dict = {'job_id': 1, 'priv_user': 'user', 'dbname': 'database',
                   'job_status': 1, 'last_start_date': 1640139694, 'last_end_date': 1640139695}

pg_class_relsize_seq = Sequence(timestamps=(1640139695000,),
                                values=(1000,),
                                name='pg_class_relsize',
                                step=5,
                                labels=pg_class_relsize_dict)

pg_lock_sql_locked_times_seq = Sequence(timestamps=(1640139695000,),
                                        values=(1000,),
                                        name='pg_lock_sql_locked_times',
                                        step=5,
                                        labels=pg_lock_sql_locked_times_dict)
pg_tables_structure_dead_rate_seq = Sequence(timestamps=(1640139695000,),
                                             values=(0.4,),
                                             name='pg_tables_structure_dead_rate',
                                             step=5,
                                             labels=pg_tables_structure_dead_rate_dict)
pg_tables_structure_n_live_tup_seq = Sequence(timestamps=(1640139695000,),
                                              values=(1000,),
                                              name='pg_tables_structure_n_live_tup',
                                              step=5,
                                              labels=pg_tables_structure_n_live_tup_dict)
pg_tables_structure_n_dead_tup_seq = Sequence(timestamps=(1640139695000,),
                                              values=(1000,),
                                              name='pg_tables_structure_n_dead_tup',
                                              step=5,
                                              labels=pg_tables_structure_n_dead_tup_dict)
pg_tables_structure_column_number_seq = Sequence(timestamps=(1640139695000,),
                                                 values=(3,),
                                                 name='pg_tables_structure_column_number',
                                                 step=5,
                                                 labels=pg_tables_structure_column_number_dict)
pg_tables_structure_last_vacuum_seq = Sequence(timestamps=(1640139695000,),
                                               values=(1640139695000,),
                                               name='pg_tables_structure_last_vacuum',
                                               step=5,
                                               labels=pg_tables_structure_last_vacuum_dict)
pg_tables_structure_last_analyze_seq = Sequence(timestamps=(1640139695000,),
                                                values=(1640139695000,),
                                                name='pg_tables_structure_last_analyze',
                                                step=5,
                                                labels=pg_tables_structure_last_analyze_dict)
pg_tables_structure_last_autovacuum_seq = Sequence(timestamps=(1640139695000,),
                                                   values=(1640139695000,),
                                                   name='pg_tables_structure_last_autovacuum',
                                                   step=5,
                                                   labels=pg_tables_structure_last_autovacuum_dict)
pg_tables_structure_last_autoanalyze_seq = Sequence(timestamps=(1640139695000,),
                                                    values=(1640139695000,),
                                                    name='pg_tables_structure_last_autoanalyze',
                                                    step=5,
                                                    labels=pg_tables_structure_last_autoanalyze_dict)

pg_table_skewness_skewstddev_seq = Sequence(timestamps=(1640139695000,),
                                            values=(0.4,),
                                            name='pg_tables_structure_last_autoanalyze',
                                            step=5,
                                            labels=pg_table_skewness_skewstddev_dict)
pg_tables_size_bytes_seq = Sequence(timestamps=(1640139695000,),
                                    values=(1024 * 1024,),
                                    name='pg_tables_size_bytes',
                                    step=5,
                                    labels=pg_tables_size_bytes_dict)

pg_index_idx_scan_seq = Sequence(timestamps=(1640139695000,),
                                 values=(10000,),
                                 name='pg_index_idx_scan',
                                 step=5,
                                 labels=pg_index_idx_scan_dict)

pg_never_used_indexes_index_size_seq = Sequence(timestamps=(1640139695000,),
                                                values=(1000,),
                                                name='pg_never_used_indexes_index_size',
                                                step=5,
                                                labels=pg_never_used_indexes_index_size_dict)
pg_settings_setting_seq = Sequence(timestamps=(1640139695000,),
                           values=(100,),
                           name='pg_settings',
                           step=5,
                           labels=pg_settings_dict)
gaussdb_qps_by_instance_seq = Sequence(timestamps=(1640139695000,),
                                       values=(1000,),
                                       name='gaussdb_qps_by_instance',
                                       step=5,
                                       labels=gaussdb_qps_by_instance_dict)

pg_connections_max_conn_seq = Sequence(timestamps=(1640139695000,),
                                       values=(100,),
                                       name='pg_connections_max_conn',
                                       step=5,
                                       labels=pg_connections_max_conn_dict)

pg_connections_used_conn_seq = Sequence(timestamps=(1640139695000,),
                                        values=(10,),
                                        name='pg_connections_used_conn',
                                        step=5,
                                        labels=pg_connections_used_conn_dict)
pg_thread_pool_listener_seq = Sequence(timestamps=(1640139695000,),
                                       values=(10,),
                                       name='pg_thread_pool_listener',
                                       step=5,
                                       labels=pg_thread_pool_listener_dict)
pg_wait_events_last_updated_seq = Sequence(timestamps=(1640139695000,),
                                           values=(1640139695000,),
                                           name='pg_wait_events_last_updated',
                                           step=5,
                                           labels=pg_wait_events_last_updated_dict)
os_disk_iops_seq = Sequence(timestamps=(1640139695000, 1640139700000, 1640139705000),
                            values=(1000, 1000, 1000),
                            name='os_disk_iops',
                            step=5,
                            labels=os_disk_iops_dict)

os_disk_ioutils_seq = Sequence(timestamps=(1640139695000, 1640139700000, 1640139705000),
                               values=(0.5, 0.3, 0.2),
                               name='os_disk_ioutils',
                               step=5,
                               labels=os_disk_ioutils_dict)
os_disk_usage_seq = Sequence(timestamps=(1640139695000, 1640139700000, 1640139705000),
                               values=(0.5, 0.3, 0.2),
                               name='os_disk_ioutils',
                               step=5,
                               labels=os_disk_usage_dict)

os_cpu_iowait_seq = Sequence(timestamps=(1640139695000,),
                             values=(0.15,),
                             name='os_cpu_iowait',
                             step=5,
                             labels=os_cpu_iowait_dict)

os_disk_iocapacity_seq = Sequence(timestamps=(1640139695000,),
                                  values=(200,),
                                  name='os_disk_iocapacity',
                                  step=5,
                                  labels=os_disk_iocapacity_dict)

os_cpu_usage_seq = Sequence(timestamps=(1640139695000,),
                            values=(0.2,),
                            name='os_cpu_usage',
                            step=5,
                            labels=os_cpu_usage_dict)

load_average_seq = Sequence(timestamps=(1640139695000,),
                            values=(0.3,),
                            name='load_average',
                            step=5,
                            labels=node_load1_dict)

os_mem_usage_seq = Sequence(timestamps=(1640139695000,),
                            values=(0.2,),
                            name='os_mem_usage',
                            step=5,
                            labels=os_mem_usage_dict)

node_load1_seq = Sequence(timestamps=(1640139695000,),
                          values=(0.3,),
                          name='node_load1',
                          step=5,
                          labels=node_load1_dict)

io_write_delay_time_seq = Sequence(timestamps=(1640139695000,),
                                   values=(100,),
                                   name='io_write_delay_time',
                                   step=5,
                                   labels=io_write_delay_time_dict)

io_read_delay_time_seq = Sequence(timestamps=(1640139695000,),
                                  values=(100,),
                                  name='io_read_delay_time',
                                  step=5,
                                  labels=io_read_delay_time_dict)
io_queue_number_seq = Sequence(timestamps=(1640139695000,),
                               values=(100,),
                               name='io_queue_number',
                               step=5,
                               labels=io_queue_number_time_dict)

node_process_fds_rate_seq = Sequence(timestamps=(1640139695000,),
                                     values=(0.3,),
                                     name='node_process_fds_rate',
                                     step=5,
                                     labels=node_process_fds_rate_dict)
node_network_receive_bytes_seq = Sequence(timestamps=(1640139695000,),
                                          values=(1000,),
                                          name='node_network_receive_bytes',
                                          step=5,
                                          labels=node_network_receive_bytes_dict)
node_network_transmit_bytes_seq = Sequence(timestamps=(1640139695000,),
                                           values=(1000,),
                                           name='node_network_transmit_bytes',
                                           step=5,
                                           labels=node_network_transmit_bytes_dict)
node_network_receive_drop_seq = Sequence(timestamps=(1640139695000,),
                                         values=(0.3,),
                                         name='node_network_receive_drop',
                                         step=5,
                                         labels=node_network_receive_drop_dict)
node_network_transmit_drop_seq = Sequence(timestamps=(1640139695000,),
                                          values=(0.3,),
                                          name='node_network_transmit_drop',
                                          step=5,
                                          labels=node_network_transmit_drop_dict)
node_network_receive_packets_seq = Sequence(timestamps=(1640139695000,),
                                            values=(100,),
                                            name='node_network_receive_packets',
                                            step=5,
                                            labels=node_network_receive_packets_dict)
node_network_transmit_packets_seq = Sequence(timestamps=(1640139695000,),
                                             values=(100,),
                                             name='node_network_transmit_packets',
                                             step=5,
                                             labels=node_network_transmit_packets_dict)
node_network_receive_error_seq = Sequence(timestamps=(1640139695000,),
                                          values=(0.3,),
                                          name='node_network_receive_error',
                                          step=5,
                                          labels=node_network_receive_error_dict)
node_network_transmit_error_seq = Sequence(timestamps=(1640139695000,),
                                           values=(0.3,),
                                           name='node_network_transmit_error',
                                           step=5,
                                           labels=node_network_transmit_error_dict)

pg_stat_bgwriter_buffers_checkpoint_seq = Sequence(timestamps=(1640139695000,),
                                                   values=(10,),
                                                   name='pg_stat_bgwriter_checkpoint_avg_sync_time',
                                                   step=5,
                                                   labels=pg_stat_bgwriter_buffers_checkpoint_dict)
pg_stat_bgwriter_buffers_clean_seq = Sequence(timestamps=(1640139695000,),
                                              values=(20,),
                                              name='pg_stat_bgwriter_buffers_clean',
                                              step=5,
                                              labels=pg_stat_bgwriter_buffers_clean_dict)
pg_stat_bgwriter_buffers_backend_seq = Sequence(timestamps=(1640139695000,),
                                                values=(30,),
                                                name='pg_stat_bgwriter_buffers_backend',
                                                step=5,
                                                labels=pg_stat_bgwriter_buffers_backend_dict)
pg_stat_bgwriter_buffers_alloc_seq = Sequence(timestamps=(1640139695000,),
                                              values=(40,),
                                              name='pg_stat_bgwriter_buffers_alloc',
                                              step=5,
                                              labels=pg_stat_bgwriter_buffers_alloc_dict)

pg_replication_lsn_seq = Sequence(timestamps=(1640139695000,),
                                  values=(1000,),
                                  name='pg_replication_lsn',
                                  step=5,
                                  labels=pg_replication_lsn_dict)
pg_replication_sent_diff_seq = Sequence(timestamps=(1640139695000,),
                                        values=(1000,),
                                        name='pg_replication_sent_diff',
                                        step=5,
                                        labels=pg_replication_sent_diff_dict)

pg_replication_write_diff_seq = Sequence(timestamps=(1640139695000,),
                                         values=(1000,),
                                         name='pg_replication_write_diff',
                                         step=5,
                                         labels=pg_replication_write_diff_dict)

pg_replication_flush_diff_seq = Sequence(timestamps=(1640139695000,),
                                         values=(1000,),
                                         name='pg_replication_flush_diff',
                                         step=5,
                                         labels=pg_replication_flush_diff_dict)

pg_replication_replay_diff_seq = Sequence(timestamps=(1640139695000,),
                                          values=(1000,),
                                          name='pg_replication_replay_diff',
                                          step=5,
                                          labels=pg_replication_replay_diff_dict)
gs_sql_count_select_seq = Sequence(timestamps=(1640139695000,),
                                   values=(1000,),
                                   name='gs_sql_count_select',
                                   step=5,
                                   labels=gs_sql_count_select_dict)
gs_sql_count_update_seq = Sequence(timestamps=(1640139695000,),
                                   values=(1000,),
                                   name='gs_sql_count_update',
                                   step=5,
                                   labels=gs_sql_count_update_dict)

gs_sql_count_delete_seq = Sequence(timestamps=(1640139695000,),
                                   values=(1000,),
                                   name='gs_sql_count_delete',
                                   step=5,
                                   labels=gs_sql_count_delete_dict)
gs_sql_count_insert_seq = Sequence(timestamps=(1640139695000,),
                                   values=(1000,),
                                   name='gs_sql_count_insert',
                                   step=5,
                                   labels=gs_sql_count_insert_dict)
db_timed_task_failure_count_seq = Sequence(timestamps=(1640139695000,),
                                           values=(1000,),
                                           name='timed_task',
                                           step=5,
                                           labels=timed_task_dict)


slow_sql_instance = SlowQuery(db_host='127.0.0.1', db_port='8080', db_name='database1', schema_name='schema1',
                              query='select count(*) from schema1.table1', start_timestamp=1640139690,
                              duration_time=1000, track_parameter=True, plan_time=100, parse_time=20, db_time=2000,
                              hit_rate=0.99, fetch_rate=0.98, cpu_time=14200, data_io_time=1231243,
                              template_id=12432453234, query_plan=None,
                              sort_count=13, sort_mem_used=12.43, sort_spill_count=3, hash_count=0, hash_mem_used=0,
                              hash_spill_count=0, lock_wait_count=0, lwlock_wait_count=0, n_returned_rows=1,
                              n_tuples_returned=100000, n_tuples_fetched=0, n_tuples_deleted=0, n_tuples_inserted=0,
                              n_tuples_updated=0)
slow_sql_instance.tables_name = {'schema1': ['table1']}


class MockedFetcher(dai.LazyFetcher):
    def __init__(self, metric, start_time=None, end_time=None):
        super().__init__(metric)
        self.metric = metric
        self.start_time = start_time
        self.end_time = end_time

    def _fetch_sequence(self, start_time=None, end_time=None, step=None):
        self.metric = f"{self.metric}_seq"
        return [globals().get(self.metric, None)]


@pytest.fixture
def mock_get_slow_queries(monkeypatch, mock_dai):
    monkeypatch.setattr(mock_dai, 'get_latest_metric_sequence',
                        mock.Mock(side_effect=lambda x, y: MockedFetcher(metric=x)))
    monkeypatch.setattr(mock_dai, 'get_metric_sequence', mock.Mock(side_effect=lambda x, y, z: MockedFetcher(metric=x)))


def test_query_source_1(mock_get_slow_queries):
    query_source = query_info_source.QueryContextFromTSDB(slow_sql_instance)
    pg_class = query_source.acquire_pg_class()
    pg_lock_sql = query_source.acquire_lock_info()
    pg_tables_structure = query_source.acquire_tables_structure_info()
    database_info = query_source.acquire_database_info()
    fetch_interval = query_source.acquire_fetch_interval()
    system_info = query_source.acquire_system_info()
    pg_stat_replication_info = query_source.acquire_pg_replication_info()
    pg_bgwriter_info = query_source.acquire_bgwriter_info()
    node_network_info = query_source.acquire_network_info()
    gs_sql_count_info = query_source.acquire_sql_count_info()
    timed_task_info = query_source.acquire_timed_task()
    wait_event_info = query_source.acquire_wait_event()
    pg_settings_info = query_source.acquire_pg_settings()
    assert pg_class[0].db_host == '127.0.0.1'
    assert pg_class[0].db_port == '8080'
    assert pg_class[0].db_name == 'database1'
    assert pg_class[0].schema_name == 'schema1'
    assert pg_class[0].relname == 'table1'
    assert pg_class[0].relkind == 'r'
    assert pg_tables_structure[0].db_host == '127.0.0.1'
    assert pg_tables_structure[0].db_port == '8080'
    assert pg_tables_structure[0].db_name == 'database1'
    assert pg_tables_structure[0].schema_name == 'schema1'
    assert pg_tables_structure[0].table_name == 'table1'
    assert pg_tables_structure[0].dead_rate == 0.4
    assert pg_tables_structure[0].live_tuples == 1000
    assert pg_tables_structure[0].dead_tuples == 1000
    assert pg_tables_structure[0].column_number == 3
    assert pg_tables_structure[0].vacuum == 1640139695000
    assert pg_tables_structure[0].last_autovacuum == 1640139695000
    assert pg_tables_structure[0].analyze == 1640139695000
    assert pg_tables_structure[0].last_autoanalyze == 1640139695000
    assert pg_tables_structure[0].table_size == 1
    assert pg_tables_structure[0].index == {'index1': ['col1']}
    assert pg_tables_structure[0].redundant_index == ['table_index1']
    assert pg_tables_structure[0].skew_ratio == 0.3
    assert pg_tables_structure[0].skew_stddev == 0.4
    assert pg_settings_info.get('shared_buffers').name == 'shared_buffers'
    assert pg_settings_info.get('shared_buffers').vartype == 'int64'
    assert pg_settings_info.get('shared_buffers').setting == 100

    assert database_info.history_tps[0] == 1000
    assert database_info.current_tps[0] == 1000
    assert database_info.max_conn == 100
    assert database_info.used_conn == 10
    assert database_info.thread_pool.get('worker_info_default') == 250
    assert database_info.thread_pool.get('worker_info_idle') == 250
    assert database_info.thread_pool.get('session_info_total') == 4
    assert database_info.thread_pool.get('session_info_waiting') == 0
    assert database_info.thread_pool.get('session_info_running') == 0
    assert database_info.thread_pool.get('session_info_idle') == 4

    assert wait_event_info[0].node_name == 'node1'
    assert wait_event_info[0].type == 'IO_EVENT'
    assert wait_event_info[0].event == 'CopyFileWrite'
    assert wait_event_info[0].last_updated == 1640139695000
   
    assert system_info.iops == 1000
    assert system_info.ioutils == {'sdm-0': 0.5}
    assert system_info.iocapacity == 200
    assert system_info.iowait == 0.15
    assert system_info.cpu_usage == 0.2
    assert system_info.mem_usage == 0.2
    assert system_info.load_average == 0.3
    assert system_info.io_read_delay == {'sdm-0': 100.0}
    assert system_info.io_write_delay == {'sdm-0': 100.0}
    assert system_info.io_queue_number == {'sdm-0': 100.0}

    assert node_network_info.receive_bytes == 1000
    assert node_network_info.transmit_bytes == 1000
    assert node_network_info.receive_drop == 0.3
    assert node_network_info.transmit_drop == 0.3
    assert node_network_info.receive_packets == 100
    assert node_network_info.transmit_packets == 100
    assert node_network_info.receive_error == 0.3
    assert node_network_info.transmit_error == 0.3

    assert pg_bgwriter_info.buffers_checkpoint == 10
    assert pg_bgwriter_info.buffers_clean == 20
    assert pg_bgwriter_info.buffers_backend == 30
    assert pg_bgwriter_info.buffers_alloc == 40

    assert pg_stat_replication_info[0].application_name == 'WalSender to Standby[dn_6002]'
    assert pg_stat_replication_info[0].pg_replication_lsn == 1000
    assert pg_stat_replication_info[0].pg_replication_write_diff == 1000
    assert pg_stat_replication_info[0].pg_replication_sent_diff == 1000
    assert pg_stat_replication_info[0].pg_replication_replay_diff == 1000
    assert pg_stat_replication_info[0].pg_replication_flush_diff == 1000

    assert gs_sql_count_info.select_count == 1000
    assert gs_sql_count_info.delete_count == 1000
    assert gs_sql_count_info.update_count == 1000
    assert gs_sql_count_info.insert_count == 1000

    assert fetch_interval == 5

    assert pg_lock_sql.locked_query[0] == 'update table2 set age=20 where id=3'
    assert pg_lock_sql.locked_query_start[0] == 1640139695
    assert pg_lock_sql.locker_query[0] == 'delete from table2 where id=3'
    assert pg_lock_sql.locker_query_start[0] == 1640139690

    assert timed_task_info[0].job_id == 1
    assert timed_task_info[0].priv_user == 'user'
    assert timed_task_info[0].dbname == 'database'
    assert timed_task_info[0].job_status == 1
    assert timed_task_info[0].last_start_date == 1640139694000
    assert timed_task_info[0].last_end_date == 1640139695000


def test_query_source_2():
    query_source = query_info_source.QueryContextFromDriver(slow_sql_instance)
    pg_class = query_source.acquire_pg_class()
    pg_lock_sql = query_source.acquire_lock_info()
    pg_tables_structure = query_source.acquire_tables_structure_info()
    database_info = query_source.acquire_database_info()
    system_info = query_source.acquire_system_info()
    pg_stat_replication_info = query_source.acquire_pg_replication_info()
    pg_bgwriter_info = query_source.acquire_bgwriter_info()
    node_network_info = query_source.acquire_network_info()
    gs_sql_count_info = query_source.acquire_sql_count_info()
    timed_task_info = query_source.acquire_timed_task()
    wait_event_info = query_source.acquire_wait_event()
    pg_settings_info = query_source.acquire_pg_settings()
    rewritten_sql = query_source.acquire_rewritten_sql()
    recommend_sql = query_source.acquire_recommend_index()
    assert isinstance(pg_class, list) and len(pg_class) == 0
    assert isinstance(pg_lock_sql, query_info_source.LockInfo)
    assert isinstance(pg_tables_structure, list) and len(pg_tables_structure) == 0
    assert isinstance(pg_settings_info, dict) and len(pg_settings_info) == 0
    assert isinstance(system_info, query_info_source.SystemInfo)
    assert isinstance(node_network_info, query_info_source.NetWorkInfo)
    assert isinstance(pg_bgwriter_info, query_info_source.BgWriter)
    assert isinstance(pg_stat_replication_info, list) and len(pg_stat_replication_info) == 0
    assert isinstance(gs_sql_count_info, query_info_source.GsSQLCountInfo)
    assert isinstance(timed_task_info, list) and len(timed_task_info) == 0
    assert isinstance(wait_event_info, list) and len(wait_event_info) == 0
    assert isinstance(database_info, query_info_source.DatabaseInfo)
    assert recommend_sql == '' 
    assert rewritten_sql == ''
