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
from typing import List
from unittest import mock

from dbmind.app.diagnosis.query.slow_sql import query_info_source
from dbmind.app.diagnosis.query.slow_sql.query_info_source import TableStructure, DatabaseInfo, LockInfo, SystemInfo
from dbmind.common.types import Sequence
from dbmind.common.types.misc import SlowQuery
from dbmind.service import dai
from dbmind.service.dai import _AbstractLazyFetcher


def test_table_structure():
    table_info = TableStructure()
    table_info.db_host = '127.0.0.1'
    table_info.db_port = '8080'
    table_info.db_name = 'user'
    table_info.schema_name = 'public'
    table_info.table_name = 'table_1'
    table_info.dead_tuples = 1000
    table_info.live_tuples = 100000
    table_info.dead_rate = 0.9
    table_info.last_autovacuum = None
    table_info.last_autoanalyze = None
    table_info.vacuum = None
    table_info.analyze = None
    table_info.table_size = 10000
    table_info.index = ['index1', 'index2']
    table_info.redundant_index = ['redundant_index']
    assert table_info.db_host == '127.0.0.1'
    assert table_info.db_port == '8080'
    assert table_info.db_name == 'user'
    assert table_info.schema_name == 'public'
    assert table_info.table_name == 'table_1'
    assert table_info.dead_tuples == 1000
    assert table_info.live_tuples == 100000
    assert table_info.dead_rate == 0.9
    assert table_info.last_autovacuum is None
    assert table_info.last_autoanalyze is None
    assert table_info.vacuum is None
    assert table_info.analyze is None
    assert table_info.table_size == 10000
    assert table_info.index == ['index1', 'index2']
    assert table_info.redundant_index == ['redundant_index']


def test_database_info():
    database_info = DatabaseInfo()
    assert database_info.db_host is None
    assert database_info.db_port is None
    assert database_info.history_tps == []
    assert database_info.current_tps == []
    assert database_info.max_conn == 0
    assert database_info.used_conn == 0


def test_lock_info():
    lock_info = LockInfo()
    assert lock_info.db_host is None
    assert lock_info.db_port is None
    assert lock_info.locked_query == []
    assert lock_info.locked_query_start == []
    assert lock_info.locker_query == []
    assert lock_info.locker_query_end == []


def test_system_info():
    system_info = SystemInfo()
    system_info.db_host = '127.0.0.1'
    system_info.db_port = '8080'
    system_info.iops = 100
    system_info.ioutils = {}
    system_info.iocapacity = 1.0
    system_info.iowait = 2.0
    system_info.cpu_usage = 3.0
    system_info.mem_usage = 4.0
    system_info.load_average = 5.0
    assert system_info.db_host == '127.0.0.1'
    assert system_info.db_port == '8080'
    assert system_info.iops == 100
    assert system_info.ioutils == {}
    assert system_info.iocapacity == 1.0
    assert system_info.iowait == 2.0
    assert system_info.cpu_usage == 3.0
    assert system_info.mem_usage == 4.0
    assert system_info.load_average == 5.0


pg_class_relsize_dict = {'datname': 'database1', 'nspname': 'schema1', 'relname': 'table1', 'relkind': 'r'}
pg_lock_sql_locked_times_dict = {'locked_query': 'update table2 set age=20 where id=3',
                                 'locked_query_start': 1640139695,
                                 'locker_query': 'delete from table2 where id=3',
                                 'locker_query_start': 1640139690}
pg_tables_expansion_rate_dead_rate_dict = {'datname': 'database1', 'schemaname': 'schema1', 'relname': 'table1',
                                           'n_live_tup': 10000,
                                           'n_dead_tup': 100, 'dead_rate': 0.01, 'last_vacuum': None,
                                           'last_autovacuum': None,
                                           'last_analyze': None, 'last_autoanalyze': None}
pg_tables_size_bytes_dict = {'datname': 'database1', 'nspname': 'schema1', 'relname': 'tables'}
pg_index_idx_scan_dict = {'datname': 'database1', 'nspname': 'schema1', 'tablename': 'table1', 'relname': 'index1'}
pg_never_used_indexes_index_size_dict = {'datname': 'database1', 'schemaname': 'schema1', 'relname': 'table1',
                                         'indexrelname': 'table_index1'}
gaussdb_qps_by_instance_dict = {'instance': '127.0.0.1:5432'}
pg_connections_max_conn_dict = {'instance': '127.0.0.1:5432'}
pg_connections_used_conn_dict = {'instance': '127.0.0.1:5432'}
os_disk_iops_dict = {'instance': '127.0.0.1:5432'}
os_disk_ioutils_dict = {'instance': '127.0.0.1:5432', 'device': 'sdm-0'}
os_cpu_iowait_dict = {'instance': '127.0.0.1:5432'}
os_disk_iocapacity_dict = {'instance': '127.0.0.1:5432'}
os_cpu_usage_rate_dict = {'instance': '127.0.0.1:5432'}
os_mem_usage_dict = {'instance': '127.0.0.1:5432'}
node_load1_dict = {'instance': '127.0.0.1:5432'}

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

pg_tables_expansion_rate_dead_rate_seq = Sequence(timestamps=(1640139695000, 1640139700000, 1640139705000),
                                                  values=(0.1, 0.2, 0.3),
                                                  name='pg_tables_expansion_rate_dead_rate',
                                                  step=5,
                                                  labels=pg_tables_expansion_rate_dead_rate_dict)

pg_tables_size_bytes_seq = Sequence(timestamps=(1640139695000,),
                                    values=(10,),
                                    name='pg_tables_size_bytes',
                                    step=5,
                                    labels=pg_tables_size_bytes_dict)

pg_index_idx_scan_seq = Sequence(timestamps=(1640139695000,),
                                 values=(10000,),
                                 name='pg_index_idx_scan',
                                 step=5,
                                 labels=pg_index_idx_scan_dict)

pg_never_used_indexes_index_size_seq = Sequence(timestamps=(1640139695000,),
                                                values=(0,),
                                                name='pg_never_used_indexes_index_size',
                                                step=5,
                                                labels=pg_never_used_indexes_index_size_dict)

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

os_cpu_usage_rate_seq = Sequence(timestamps=(1640139695000,),
                                 values=(0.2,),
                                 name='os_cpu_usage',
                                 step=5,
                                 labels=os_cpu_usage_rate_dict)

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


class MockedFetcher(_AbstractLazyFetcher):
    def __init__(self, metric, start_time=None, end_time=None):
        super().__init__()
        self.metric = metric
        self.start_time = start_time
        self.end_time = end_time

    def _real_fetching_action(self) -> List[Sequence]:
        self.metric = f"{self.metric}_seq"
        return [globals().get(self.metric, None)]


dai.get_latest_metric_sequence = mock.Mock(side_effect=lambda x, y: MockedFetcher(metric=x))
dai.get_metric_sequence = mock.Mock(side_effect=lambda x, y, z: MockedFetcher(metric=x))


def test_query_source():
    # lock of tables_name
    slow_sql_instance = SlowQuery(db_host='127.0.0.1', db_port='8080', db_name='database1', schema_name='public',
                                  query='select count(*) from schema1.table1', start_timestamp=1640139690,
                                  duration_time=1000,
                                  hit_rate=0.99, fetch_rate=0.98, cpu_time=14200, data_io_time=1231243,
                                  template_id=12432453234,
                                  sort_count=13, sort_mem_used=12.43, sort_spill_count=3, hash_count=0, hash_mem_used=0,
                                  hash_spill_count=0, lock_wait_count=0, lwlock_wait_count=0, n_returned_rows=1,
                                  n_tuples_returned=100000, n_tuples_fetched=0, n_tuples_deleted=0, n_tuples_inserted=0,
                                  n_tuples_updated=0)
    slow_sql_instance.tables_name = {'schema1': ['table1']}
    query_source = query_info_source.QueryContext(slow_sql_instance)
    pg_class = query_source.acquire_pg_class()
    pg_lock_sql = query_source.acquire_lock_info()
    pg_tables_structure = query_source.acquire_tables_structure_info()
    database_info = query_source.acquire_database_info()
    fetch_interval = query_source.acquire_fetch_interval()
    system_info = query_source.acquire_system_info()
    assert pg_class.get('db_host') == '127.0.0.1'
    assert pg_class.get('db_port') == '8080'
    assert pg_class.get('database1') == {'schema1': ['table1']}
    assert pg_lock_sql.locked_query[0] == 'update table2 set age=20 where id=3'
    assert pg_lock_sql.locked_query_start[0] == 1640139695
    assert pg_lock_sql.locker_query[0] == 'delete from table2 where id=3'
    assert pg_lock_sql.locker_query_start[0] == 1640139690
    assert len(pg_tables_structure) == 1
    assert pg_tables_structure[0].db_name == 'database1'
    assert pg_tables_structure[0].schema_name == 'schema1'
    assert pg_tables_structure[0].table_name == 'table1'
    assert pg_tables_structure[0].dead_rate == 0.3
    assert pg_tables_structure[0].dead_tuples == 100
    assert pg_tables_structure[0].live_tuples == 10000
    assert pg_tables_structure[0].last_autovacuum == 0
    assert pg_tables_structure[0].last_autoanalyze == 0
    assert pg_tables_structure[0].analyze == 0
    assert pg_tables_structure[0].vacuum == 0
    assert database_info.history_tps[0] == 1000.0
    assert database_info.current_tps[0] == 1000.0
    assert database_info.max_conn == 100
    assert database_info.used_conn == 10
    assert system_info.iops == 1000
    assert system_info.ioutils == {'sdm-0': 0.5}
    assert system_info.iocapacity == 200
    assert system_info.iowait == 0.15
    assert system_info.cpu_usage == 0.2
    assert system_info.mem_usage == 0.2
    assert system_info.load_average == 0.3
    assert fetch_interval == 5
