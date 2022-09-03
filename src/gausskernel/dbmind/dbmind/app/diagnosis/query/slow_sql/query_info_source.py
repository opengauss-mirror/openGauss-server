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
import logging
import re
import math
from datetime import datetime, timedelta
from functools import wraps

from dbmind import global_vars
from dbmind.common.parser import plan_parsing
from dbmind.common.parser.sql_parsing import to_ts
from dbmind.common.utils import ExceptionCatcher
from dbmind.components.sql_rewriter import sql_rewriter
from dbmind.components.sql_rewriter.executor import Executor
from dbmind.global_vars import agent_rpc_client
from dbmind.service import dai
from dbmind.service.web import toolkit_rewrite_sql

exception_catcher = ExceptionCatcher(strategy='raise', name='SLOW QUERY')


def exception_follower(output=None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logging.exception("Function execution error: %s" % func.__name__)
                if callable(output):
                    return output()
                return output

        return wrapper

    return decorator


REQUIRED_PARAMETERS = ('shared_buffers', 'work_mem', 'maintenance_work_mem',
                       'max_process_memory', 'enable_nestloop', 'enable_hashjoin',
                       'enable_mergejoin', 'enable_indexscan', 'enable_hashagg', 'enable_sort',
                       'skew_option', 'block_size', 'recovery_min_apply_delay', 'max_connections')


class TableStructure:
    """Data structure to save table structure, contains the main information of the table structure such as
    database address, database name, schema name, table name, dead tuples, etc
    """

    def __init__(self):
        self.db_host = None
        self.db_port = None
        self.db_name = None
        self.schema_name = None
        self.table_name = None
        self.dead_tuples = 0
        self.live_tuples = 0
        self.dead_rate = 0.0
        # How to get field num for table
        self.column_number = 0
        # This field can be used as a flag for whether the statistics
        # are updated.
        self.last_autovacuum = 0
        self.last_autoanalyze = 0
        self.vacuum = 0
        self.analyze = 0
        self.table_size = 0
        self.index_size = 0
        self.index = {}
        self.redundant_index = []
        # Note: for the distributed database version, the following two indicators are meaningful
        self.skew_ratio = 0.0
        self.skew_stddev = 0.0


class LockInfo:
    """Data structure to save lock information such as database information, locker_query and locked_query, etc"""

    def __init__(self):
        self.db_host = None
        self.db_port = None
        self.locked_query = []
        self.locked_query_start = []
        self.locker_query = []
        self.locker_query_end = []


class DatabaseInfo:
    """Data structure to save database information such as database address and TPS, connection"""

    def __init__(self):
        self.db_host = None
        self.db_port = None
        self.history_tps = []
        self.current_tps = []
        self.max_conn = 1
        self.used_conn = 0
        self.thread_pool = {}


class GsSQLCountInfo:
    def __init__(self):
        self.node_name = None
        self.select_count = 0
        self.update_count = 0
        self.insert_count = 0
        self.delete_count = 0
        self.mergeinto_count = 0
        self.ddl_count = 0
        self.dcl_count = 0
        self.dml_count = 0


class SystemInfo:
    """Data structure to save system information such as database address, IOWAIT, IOCAPACITY, CPU_USAGE, etc"""

    def __init__(self):
        self.db_host = None
        self.db_port = None
        self.iops = 0
        self.ioutils = {}
        self.disk_usage = {}
        self.iocapacity = 0.0
        self.iowait = 0.0
        self.cpu_core_number = 1
        self.cpu_usage = 0.0
        self.mem_usage = 0.0
        self.load_average = 0.0
        self.process_fds_rate = 0.0
        self.io_read_delay = {}
        self.io_write_delay = {}
        self.io_queue_number = {}


class PgClass:
    def __init__(self):
        self.db_host = None
        self.db_port = None
        self.db_name = None
        self.schema_name = None
        self.relname = None
        self.relkind = None
        self.relpages = None
        self.reltuples = None
        self.relhasindex = None
        self.relsize = None


class PgSetting:
    """Data structure to save GUC Parameter"""

    def __init__(self):
        self.name = None
        self.setting = None
        self.unit = None
        self.min_val = None
        self.max_val = None
        self.vartype = None
        self.boot_val = None


class NetWorkInfo:
    """Data structure to save server network metrics"""

    def __init__(self):
        self.name = None
        self.receive_packets = 1.0
        self.transmit_packets = 1.0
        self.receive_drop = 0.0
        self.transmit_drop = 0.0
        self.transmit_error = 0.0
        self.receive_error = 0.0
        self.receive_bytes = 1.0
        self.transmit_bytes = 1.0


class PgReplicationInfo:
    def __init__(self):
        self.application_name = None
        self.pg_downstream_state_count = 0
        self.pg_replication_lsn = 0
        self.pg_replication_sent_diff = 0
        self.pg_replication_write_diff = 0
        self.pg_replication_flush_diff = 0
        self.pg_replication_replay_diff = 0


class BgWriter:
    def __init__(self):
        self.checkpoint_avg_sync_time = 0.0
        self.checkpoint_proactive_triggering_ratio = 0.0
        self.buffers_checkpoint = 0.0
        self.buffers_clean = 0.0
        self.buffers_backend = 0.0
        self.buffers_alloc = 0.0


class Index:
    def __init__(self):
        self.db_name = None
        self.schema_name = None
        self.table_name = None
        self.column_name = None
        self.index_name = None
        self.index_type = None

    def __repr__(self):
        return "database: %s, schema: %s, table: %s, column: %s" % (
            self.db_name, self.schema_name, self.table_name, self.column_name
        )


class TimedTask:
    def __init__(self):
        self.job_id = None
        self.priv_user = None
        self.dbname = None
        self.job_status = None
        self.last_start_date = 0
        self.last_end_date = 0
        self.failure_count = 0


class WaitEvent:
    def __init__(self):
        self.node_name = None
        self.type = None
        self.event = None
        self.wait = 0
        self.failed_wait = 0
        self.total_wait_time = 0
        self.last_updated = 0


class QueryContext:
    def __init__(self, slow_sql_instance):
        self.slow_sql_instance = slow_sql_instance
        self.is_sql_valid = True

    @exception_catcher
    def acquire_plan_parse(self):
        if self.slow_sql_instance.query_plan is not None:
            plan_parse = plan_parsing.Plan()
            plan_parse.parse(self.slow_sql_instance.query_plan)
            return plan_parse


def parse_field_from_indexdef(indexdef):
    if indexdef is None or not len(indexdef):
        return []
    pattern = re.compile(r'CREATE INDEX \w+ ON (?:\w+\.)?\w+ USING (?:btree|hash) \((.+)?\) TABLESPACE \w+')
    fields = pattern.match(indexdef)
    if fields:
        fields = [item.strip() for item in fields.groups()[0].split(',')]
        return fields
    return []


class QueryContextFromTSDB(QueryContext):
    """The object of slow query data processing factory"""

    def __init__(self, slow_sql_instance, **kwargs):
        """
        :param slow_sql_instance: The instance of slow query
        :param default_fetch_interval: fetch interval of data source
        :param expansion_factor: Ensure that the time expansion rate of the data can be collected
        :param retrieval_time: Historical index retrieval time
        """
        super().__init__(slow_sql_instance)
        self.fetch_interval = kwargs.get('default_fetch_interval', 15)
        self.expansion_factor = kwargs.get('expansion_factor', 8)
        self.retrieval_time = kwargs.get('retrieval_time', 5)
        self.query_start_time = datetime.fromtimestamp(self.slow_sql_instance.start_at / 1000)
        self.query_end_time = datetime.fromtimestamp(
            self.slow_sql_instance.start_at / 1000 +
            self.slow_sql_instance.duration_time / 1000 +
            self.expansion_factor * self.acquire_fetch_interval()
        )
        logging.debug('[SLOW QUERY] fetch start time: %s, fetch end time: %s', self.query_start_time,
                      self.query_end_time)
        logging.debug('[SLOW QUERY] fetch interval: %s', self.fetch_interval)
        if self.slow_sql_instance.query_plan is None and self.slow_sql_instance.track_parameter:
            self.slow_sql_instance.query_plan = self.acquire_plan(self.slow_sql_instance.query)
            if self.slow_sql_instance.query_plan is None:
                self.is_sql_valid = False

    @exception_follower(output=None)
    @exception_catcher
    def acquire_plan(self, query):
        query_plan = ''
        stmts = "set current_schema='%s';explain %s" % (self.slow_sql_instance.schema_name,
                                                        query)
        rows = agent_rpc_client.call('query_in_database',
                                     stmts,
                                     self.slow_sql_instance.db_name,
                                     return_tuples=True)
        for row in rows:
            query_plan += row[0] + '\n'
        if query_plan:
            return query_plan

    @exception_follower(output=list)
    @exception_catcher
    def acquire_pg_class(self) -> list:
        """Get all object information in the database"""
        pg_classes = []
        sequences = dai.get_metric_sequence('pg_class_relsize', self.query_start_time, self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
            datname=self.slow_sql_instance.db_name).fetchall()
        sequences = [sequence for sequence in sequences if sequence.labels]
        for sequence in sequences:
            pg_class = PgClass()
            pg_class.db_host = self.slow_sql_instance.db_host
            pg_class.db_port = self.slow_sql_instance.db_port
            pg_class.db_name = sequence.labels['datname']
            pg_class.schema_name = sequence.labels['nspname']
            pg_class.relname = sequence.labels['relname']
            pg_class.relkind = sequence.labels['relkind']
            pg_class.relhasindex = sequence.labels['relhasindex']
            pg_class.relsize = round(float(max(sequence.values)), 4)
            pg_classes.append(pg_class)
        return pg_classes

    @exception_follower(output=15)
    @exception_catcher
    def acquire_fetch_interval(self) -> int:
        """Get data source collection frequency"""
        sequence = dai.get_latest_metric_sequence("os_disk_iops", self.retrieval_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        logging.debug('[SLOW QUERY] acquire_fetch_interval: %s.', sequence)
        timestamps = sequence.timestamps
        if len(timestamps) >= 2:
            self.fetch_interval = int(timestamps[-1]) // 1000 - int(timestamps[-2]) // 1000
        return self.fetch_interval

    @exception_follower(output=LockInfo)
    @exception_catcher
    def acquire_lock_info(self) -> LockInfo:
        """Get lock information during slow SQL execution"""
        blocks_info = LockInfo()
        locks_sequences = dai.get_metric_sequence("pg_lock_sql_locked_times", self.query_start_time,
                                                  self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchall()
        locked_query, locked_query_start, locker_query, locker_query_start = [], [], [], []
        locks_sequences = [sequence for sequence in locks_sequences if sequence.labels]
        for locks_sequence in locks_sequences:
            locked_query.append(locks_sequence.labels.get('locked_query', 'Unknown'))
            locked_query_start.append(locks_sequence.labels.get('locked_query_start', 'Unknown'))
            locker_query.append(locks_sequence.labels.get('locker_query', 'Unknown'))
            locker_query_start.append(locks_sequence.labels.get('locker_query_start', 'Unknown'))
        blocks_info.locked_query = locked_query
        blocks_info.locked_query_start = locked_query_start
        blocks_info.locker_query = locker_query
        blocks_info.locker_query_start = locker_query_start

        return blocks_info

    @exception_follower(output=list)
    @exception_catcher
    def acquire_tables_structure_info(self) -> list:
        """Acquire table structure information related to slow query"""
        table_structure = []
        if not self.slow_sql_instance.tables_name:
            return table_structure
        for schema_name, tables_name in self.slow_sql_instance.tables_name.items():
            for table_name in tables_name:
                table_info = TableStructure()
                table_info.db_host = self.slow_sql_instance.db_host
                table_info.db_port = self.slow_sql_instance.db_port
                table_info.db_name = self.slow_sql_instance.db_name
                table_info.schema_name = schema_name
                table_info.table_name = table_name
                dead_rate_info = dai.get_metric_sequence("pg_tables_structure_dead_rate",
                                                         self.query_start_time,
                                                         self.query_end_time).from_server(
                    f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    schemaname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                live_tup_info = dai.get_metric_sequence("pg_tables_structure_n_live_tup",
                                                        self.query_start_time,
                                                        self.query_end_time).from_server(
                    f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    schemaname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                dead_tup_info = dai.get_metric_sequence("pg_tables_structure_n_dead_tup",
                                                        self.query_start_time,
                                                        self.query_end_time).from_server(
                    f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    schemaname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                columns_info = dai.get_metric_sequence("pg_tables_structure_column_number",
                                                       self.query_start_time,
                                                       self.query_end_time).from_server(
                    f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    schemaname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                last_vacuum_info = dai.get_metric_sequence("pg_tables_structure_last_vacuum",
                                                           self.query_start_time,
                                                           self.query_end_time).from_server(
                    f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    schemaname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                last_autovacuum_info = dai.get_metric_sequence("pg_tables_structure_last_autovacuum",
                                                               self.query_start_time,
                                                               self.query_end_time).from_server(
                    f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    schemaname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                last_analyze_info = dai.get_metric_sequence("pg_tables_structure_last_analyze",
                                                            self.query_start_time,
                                                            self.query_end_time).from_server(
                    f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    schemaname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                last_autoanalyze_info = dai.get_metric_sequence("pg_tables_structure_last_autoanalyze",
                                                                self.query_start_time,
                                                                self.query_end_time).from_server(
                    f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    schemaname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                pg_table_size_info = dai.get_metric_sequence("pg_tables_size_bytes", self.query_start_time,
                                                             self.query_end_time).from_server(
                    f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    nspname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                index_number_info = dai.get_metric_sequence("pg_index_idx_scan", self.query_start_time,
                                                            self.query_end_time).from_server(
                    f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    nspname=f"{schema_name}").filter(tablename=f"{table_name}").fetchall()
                redundant_index_info = dai.get_metric_sequence("pg_never_used_indexes_index_size",
                                                               self.query_start_time,
                                                               self.query_end_time).from_server(
                    f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    schemaname=f"{schema_name}").filter(relname=f"{table_name}").fetchall()
                table_skewratio_info = dai.get_metric_sequence("pg_table_skewness_skewstddev",
                                                               self.query_start_time,
                                                               self.query_end_time).from_server(
                    f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    schemaname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                if dead_rate_info.values:
                    table_info.dead_rate = round(float(dead_rate_info.values[0]), 4)
                if live_tup_info.values:
                    table_info.live_tuples = int(live_tup_info.values[0])
                if dead_tup_info.values:
                    table_info.dead_tuples = int(dead_tup_info.values[0])
                if columns_info.values:
                    table_info.column_number = int(columns_info.values[0])
                if last_analyze_info.values:
                    filtered_values = [float(item) for item in last_analyze_info.values if not math.isnan(float(item))]
                    table_info.analyze = int(max(filtered_values)) if filtered_values else 0
                if last_autoanalyze_info.values:
                    filtered_values = [float(item) for item in last_autoanalyze_info.values if not math.isnan(float(item))]
                    table_info.last_autoanalyze = int(max(filtered_values)) if filtered_values else 0
                if last_vacuum_info.values:
                    filtered_values = [float(item) for item in last_vacuum_info.values if not math.isnan(float(item))]
                    table_info.vacuum = int(max(filtered_values)) if filtered_values else 0
                if last_autovacuum_info.values:
                    filtered_values = [float(item) for item in last_autovacuum_info.values if not math.isnan(float(item))]
                    table_info.last_autovacuum = int(max(filtered_values)) if filtered_values else 0

                if pg_table_size_info.values:
                    table_info.table_size = round(float(max(pg_table_size_info.values)) / 1024 / 1024, 4)
                if index_number_info:
                    table_info.index = {item.labels['relname']: parse_field_from_indexdef(item.labels['indexdef'])
                                        for item in index_number_info if item.labels}
                if redundant_index_info:
                    table_info.redundant_index = [item.labels['indexrelname'] for item in redundant_index_info if
                                                  item.labels]
                if table_skewratio_info.values:
                    table_info.skew_ratio = round(float(table_skewratio_info.labels['skewratio']), 4)
                    table_info.skew_stddev = round(float(max(table_skewratio_info.values)), 4)
                table_structure.append(table_info)

        return table_structure

    @exception_follower(output=dict)
    @exception_catcher
    def acquire_pg_settings(self) -> dict:
        pg_settings = {}
        for parameter in REQUIRED_PARAMETERS:
            pg_setting = PgSetting()
            sequence = dai.get_metric_sequence("pg_settings_setting", self.query_start_time,
                                               self.query_end_time).from_server(
                f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                name=f"{parameter}").fetchone()
            if sequence.labels:
                pg_setting.name = parameter
                pg_setting.vartype = sequence.labels['vartype']
                if pg_setting.vartype in ('integer', 'int64', 'bool'):
                    pg_setting.setting = int(sequence.values[-1])
                if pg_setting.vartype == 'real':
                    pg_setting.setting = float(sequence.values[-1])
            pg_settings[parameter] = pg_setting
        return pg_settings

    @exception_follower(output=DatabaseInfo)
    @exception_catcher
    def acquire_database_info(self) -> DatabaseInfo:
        """Acquire table database information related to slow query"""
        database_info = DatabaseInfo()
        days_time_interval = 24 * 60 * 60
        cur_tps_sequences = dai.get_metric_sequence("gaussdb_qps_by_instance", self.query_start_time,
                                                    self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchone()
        his_tps_sequences = dai.get_metric_sequence("gaussdb_qps_by_instance",
                                                    self.query_start_time - timedelta(seconds=days_time_interval),
                                                    self.query_end_time - timedelta(
                                                        seconds=days_time_interval)).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchone()
        max_conn_sequence = dai.get_metric_sequence("pg_connections_max_conn", self.query_start_time,
                                                    self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchone()
        used_conn_sequence = dai.get_metric_sequence("pg_connections_used_conn", self.query_start_time,
                                                     self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchone()
        thread_pool_sequence = dai.get_metric_sequence("pg_thread_pool_listener", self.query_start_time,
                                                       self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchall()
        if his_tps_sequences.values:
            database_info.history_tps = [float(item) for item in his_tps_sequences.values]
        if cur_tps_sequences.values:
            database_info.current_tps = [float(item) for item in cur_tps_sequences.values]
        if max_conn_sequence.values:
            database_info.max_conn = int(max(max_conn_sequence.values))
        if used_conn_sequence.values:
            database_info.used_conn = int(max(used_conn_sequence.values))
        worker_info_default, worker_info_idle = 1, 0
        session_info_total, session_info_waiting = 1, 0
        session_info_running, session_info_idle = 0, 0
        if thread_pool_sequence:
            work_info_list = [item.labels['worker_info'] for item in thread_pool_sequence if item.labels]
            session_info_list = [item.labels['session_info'] for item in thread_pool_sequence if item.labels]
            worker_info_default = sum(int(re.search(r"default: (\d+)", item).group(1)) for item in work_info_list)
            worker_info_idle = sum(int(re.search(r"idle: (\d+)", item).group(1)) for item in work_info_list)
            session_info_total = sum(int(re.search(r"total: (\d+)", item).group(1)) for item in session_info_list)
            session_info_waiting = sum(int(re.search(r"waiting: (\d+)", item).group(1)) for item in session_info_list)
            session_info_running = sum(int(re.search(r"running:(\d+)", item).group(1)) for item in session_info_list)
            session_info_idle = sum(int(re.search(r"idle: (\d+)", item).group(1)) for item in session_info_list)
            logging.debug("[SLOW QUERY] acquire_database_info[thread pool]: %s  %s", str(work_info_list),
                          str(session_info_list))
        database_info.thread_pool['worker_info_default'] = worker_info_default
        database_info.thread_pool['worker_info_idle'] = worker_info_idle
        database_info.thread_pool['session_info_total'] = session_info_total
        database_info.thread_pool['session_info_waiting'] = session_info_waiting
        database_info.thread_pool['session_info_running'] = session_info_running
        database_info.thread_pool['session_info_idle'] = session_info_idle
        return database_info

    @exception_follower(output=list)
    @exception_catcher
    def acquire_wait_event(self) -> list:
        """Acquire database wait events"""
        wait_event = []
        wait_event_last_updated_info = dai.get_metric_sequence("pg_wait_events_last_updated",
                                                               self.query_start_time,
                                                               self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchall()
        for sequence in wait_event_last_updated_info:
            wait_event_info = WaitEvent()
            if not sequence.values:
                continue
            wait_event_info.node_name = sequence.labels['nodename']
            wait_event_info.type = sequence.labels['type']
            wait_event_info.event = sequence.labels['event']
            wait_event_info.last_updated = int(max(sequence.values))
            wait_event.append(wait_event_info)
        return wait_event

    @exception_follower(output=SystemInfo)
    @exception_catcher
    def acquire_system_info(self) -> SystemInfo:
        """Acquire system information on the database server """
        system_info = SystemInfo()
        iops_info = dai.get_metric_sequence("os_disk_iops", self.query_start_time, self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        ioutils_info = dai.get_metric_sequence("os_disk_ioutils", self.query_start_time,
                                               self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchall()
        disk_usage_info = dai.get_metric_sequence("os_disk_usage", self.query_start_time,
                                                  self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchall()
        iocapacity_info = dai.get_metric_sequence("os_disk_iocapacity", self.query_start_time,
                                                  self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        iowait_info = dai.get_metric_sequence("os_cpu_iowait", self.query_start_time, self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        cpu_usage_info = dai.get_metric_sequence("os_cpu_usage", self.query_start_time,
                                                 self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        mem_usage_info = dai.get_metric_sequence("os_mem_usage", self.query_start_time,
                                                 self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        load_average_info = dai.get_metric_sequence("load_average", self.query_start_time,
                                                    self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        io_write_delay_time_info = dai.get_metric_sequence("io_write_delay_time", self.query_start_time,
                                                           self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchall()
        io_read_delay_time_info = dai.get_metric_sequence("io_read_delay_time", self.query_start_time,
                                                          self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchall()
        io_queue_number_info = dai.get_metric_sequence("io_queue_number", self.query_start_time,
                                                       self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchall()
        process_fds_rate_info = dai.get_metric_sequence("node_process_fds_rate", self.query_start_time,
                                                        self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        cpu_process_number_info = dai.get_metric_sequence("os_cpu_processor_number", self.query_start_time,
                                                          self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        if iops_info.values:
            system_info.iops = int(max(iops_info.values))
        if process_fds_rate_info.values:
            system_info.process_fds_rate = round(float(max(process_fds_rate_info.values)), 4)
        if cpu_process_number_info:
            system_info.cpu_core_number = int(max(cpu_process_number_info.values))
        if ioutils_info:
            ioutils_dict = {item.labels['device']: round(float(max(item.values)), 4) for item in ioutils_info if
                            item.labels}
            system_info.ioutils = ioutils_dict
        if disk_usage_info:
            disk_usage_dict = {item.labels['device']: round(float(max(item.values)), 4) for item in disk_usage_info if
                               item.labels}
            system_info.disk_usage = disk_usage_dict
        if iocapacity_info.values:
            system_info.iocapacity = round(float(max(iocapacity_info.values)), 4)
        if iowait_info.values:
            system_info.iowait = round(float(max(iowait_info.values)), 4)
        if cpu_usage_info.values:
            system_info.cpu_usage = round(float(max(cpu_usage_info.values)), 4)
        if mem_usage_info.values:
            system_info.mem_usage = round(float(max(mem_usage_info.values)), 4)
        if load_average_info.values:
            system_info.load_average = round(float(max(load_average_info.values)), 4)
        if io_read_delay_time_info:
            system_info.io_read_delay = {item.labels['device']: round(float(max(item.values)), 4) for item in
                                         io_read_delay_time_info if
                                         item.labels}
        if io_write_delay_time_info:
            system_info.io_write_delay = {item.labels['device']: round(float(max(item.values)), 4) for item in
                                          io_write_delay_time_info if
                                          item.labels}
        if io_queue_number_info:
            system_info.io_queue_number = {item.labels['device']: round(float(max(item.values)), 4) for item in
                                           io_queue_number_info if
                                           item.labels}
        return system_info

    @exception_follower(output=NetWorkInfo)
    @exception_catcher
    def acquire_network_info(self) -> NetWorkInfo:
        network_info = NetWorkInfo()
        node_network_receive_bytes_info = dai.get_metric_sequence('node_network_receive_bytes',
                                                                  self.query_start_time,
                                                                  self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        node_network_transmit_bytes_info = dai.get_metric_sequence('node_network_transmit_bytes',
                                                                   self.query_start_time,
                                                                   self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        node_network_receive_drop_info = dai.get_metric_sequence('node_network_receive_drop',
                                                                 self.query_start_time,
                                                                 self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        node_network_transmit_drop_info = dai.get_metric_sequence('node_network_transmit_drop',
                                                                  self.query_start_time,
                                                                  self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        node_network_receive_packets_info = dai.get_metric_sequence('node_network_receive_packets',
                                                                    self.query_start_time,
                                                                    self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        node_network_transmit_packets_info = dai.get_metric_sequence('node_network_transmit_packets',
                                                                     self.query_start_time,
                                                                     self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        node_network_transmit_error_info = dai.get_metric_sequence('node_network_transmit_error',
                                                                   self.query_start_time,
                                                                   self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        node_network_receive_error_info = dai.get_metric_sequence('node_network_receive_error',
                                                                  self.query_start_time,
                                                                  self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        if node_network_receive_bytes_info.values:
            network_info.receive_bytes = round(float(max(node_network_receive_bytes_info.values)), 4)
        if node_network_transmit_bytes_info.values:
            network_info.transmit_bytes = round(float(max(node_network_transmit_bytes_info.values)), 4)
        if node_network_receive_drop_info.values:
            network_info.receive_drop = round(float(max(node_network_receive_drop_info.values)), 4)
        if node_network_transmit_drop_info.values:
            network_info.transmit_drop = round(float(max(node_network_transmit_drop_info.values)), 4)
        if node_network_receive_packets_info.values:
            network_info.receive_packets = round(float(max(node_network_receive_packets_info.values)), 4)
        if node_network_transmit_packets_info.values:
            network_info.transmit_packets = round(float(max(node_network_transmit_packets_info.values)), 4)
        if node_network_transmit_error_info.values:
            network_info.transmit_error = round(float(max(node_network_transmit_error_info.values)), 4)
        if node_network_receive_error_info.values:
            network_info.receive_error = round(float(max(node_network_receive_error_info.values)), 4)

        return network_info

    @exception_follower(output=BgWriter)
    @exception_catcher
    def acquire_bgwriter_info(self) -> BgWriter:
        bgwriter_info = BgWriter()
        buffers_checkpoint_info = dai.get_metric_sequence('pg_stat_bgwriter_buffers_checkpoint',
                                                          self.query_start_time,
                                                          self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchone()
        buffers_clean_info = dai.get_metric_sequence('pg_stat_bgwriter_buffers_clean',
                                                     self.query_start_time,
                                                     self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchone()
        buffers_backend_info = dai.get_metric_sequence('pg_stat_bgwriter_buffers_backend',
                                                       self.query_start_time,
                                                       self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchone()
        buffers_alloc_info = dai.get_metric_sequence('pg_stat_bgwriter_buffers_alloc',
                                                     self.query_start_time,
                                                     self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchone()
        if buffers_checkpoint_info.values:
            bgwriter_info.buffers_checkpoint = round(float(max(buffers_checkpoint_info.values)), 4)
        if buffers_clean_info.values:
            bgwriter_info.buffers_clean = round(float(max(buffers_clean_info.values)), 4)
        if buffers_backend_info.values:
            bgwriter_info.buffers_backend = round(float(max(buffers_backend_info.values)), 4)
        if buffers_alloc_info.values:
            bgwriter_info.buffers_alloc = round(float(max(buffers_alloc_info.values)), 4)

        return bgwriter_info

    @exception_follower(output=list)
    @exception_catcher
    def acquire_pg_replication_info(self) -> list:
        pg_replication_info = []
        pg_replication_lsn_info = dai.get_metric_sequence('pg_replication_lsn',
                                                          self.query_start_time,
                                                          self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchall()
        pg_replication_sent_diff_info = dai.get_metric_sequence('pg_replication_sent_diff',
                                                                self.query_start_time,
                                                                self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchall()
        pg_replication_write_diff_info = dai.get_metric_sequence('pg_replication_write_diff',
                                                                 self.query_start_time,
                                                                 self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchall()
        pg_replication_flush_diff_info = dai.get_metric_sequence('pg_replication_flush_diff',
                                                                 self.query_start_time,
                                                                 self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchall()
        pg_replication_replay_diff_info = dai.get_metric_sequence('pg_replication_replay_diff',
                                                                  self.query_start_time,
                                                                  self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchall()

        pg_replication_lsn_info.sort(key=lambda x: x.labels['application_name'])
        pg_replication_sent_diff_info.sort(key=lambda x: x.labels['application_name'])
        pg_replication_write_diff_info.sort(key=lambda x: x.labels['application_name'])
        pg_replication_flush_diff_info.sort(key=lambda x: x.labels['application_name'])
        pg_replication_replay_diff_info.sort(key=lambda x: x.labels['application_name'])
        for lsn_info, write_diff_info, sent_diff_info, flush_diff_info, replay_diff_info in zip(
                pg_replication_lsn_info,
                pg_replication_write_diff_info,
                pg_replication_sent_diff_info,
                pg_replication_flush_diff_info,
                pg_replication_replay_diff_info):
            pg_replication = PgReplicationInfo()
            pg_replication.application_name = lsn_info.labels['application_name']
            pg_replication.pg_replication_lsn = int(max(lsn_info.values))
            pg_replication.pg_replication_write_diff = int(max(write_diff_info.values))
            pg_replication.pg_replication_sent_diff = int(max(sent_diff_info.values))
            pg_replication.pg_replication_flush_diff = int(max(flush_diff_info.values))
            pg_replication.pg_replication_replay_diff = int(max(replay_diff_info.values))
            pg_replication_info.append(pg_replication)
        return pg_replication_info

    @exception_follower(output=GsSQLCountInfo)
    @exception_catcher
    def acquire_sql_count_info(self) -> GsSQLCountInfo:
        gs_sql_count_info = GsSQLCountInfo()
        gs_sql_count_select = dai.get_metric_sequence('gs_sql_count_select',
                                                      self.query_start_time,
                                                      self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchone()
        gs_sql_count_insert = dai.get_metric_sequence('gs_sql_count_insert',
                                                      self.query_start_time,
                                                      self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchone()
        gs_sql_count_delete = dai.get_metric_sequence('gs_sql_count_delete',
                                                      self.query_start_time,
                                                      self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchone()
        gs_sql_count_update = dai.get_metric_sequence('gs_sql_count_update',
                                                      self.query_start_time,
                                                      self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchone()
        if gs_sql_count_select.values:
            gs_sql_count_info.select_count = int(max(gs_sql_count_select.values))
        if gs_sql_count_delete.values:
            gs_sql_count_info.delete_count = int(max(gs_sql_count_delete.values))
        if gs_sql_count_update.values:
            gs_sql_count_info.update_count = int(max(gs_sql_count_update.values))
        if gs_sql_count_insert.values:
            gs_sql_count_info.insert_count = int(max(gs_sql_count_insert.values))

        return gs_sql_count_info

    @exception_follower(output=str)
    @exception_catcher
    def acquire_rewritten_sql(self) -> str:
        if not self.slow_sql_instance.track_parameter or \
                not self.slow_sql_instance.query.strip().upper().startswith('SELECT'):
            return ''
        rewritten_flags = []
        rewritten_sql = toolkit_rewrite_sql(self.slow_sql_instance.db_name,
                                            self.slow_sql_instance.query,
                                            rewritten_flags=rewritten_flags,
                                            if_format=False)
        flag = rewritten_flags[0]
        if not flag:
            return ''
        rewritten_sql = rewritten_sql.replace('\n', ' ')
        rewritten_sql_plan = self.acquire_plan(rewritten_sql)
        old_sql_plan_parse = plan_parsing.Plan()
        rewritten_sql_plan_parse = plan_parsing.Plan()
        old_sql_plan_parse.parse(self.slow_sql_instance.query_plan)
        rewritten_sql_plan_parse.parse(rewritten_sql_plan)
        if old_sql_plan_parse.root_node.total_cost > rewritten_sql_plan_parse.root_node.total_cost:
            return rewritten_sql
        return ''

    @exception_follower(output=str)
    @exception_catcher
    def acquire_recommend_index(self) -> str:
        if not self.slow_sql_instance.track_parameter or \
                not self.slow_sql_instance.query.strip().upper().startswith('SELECT'):
            return ''
        recommend_indexes = []
        query = self.slow_sql_instance.query.replace('\'', '\'\'')
        stmt = "set current_schema=%s;select * from gs_index_advise('%s')" % (self.slow_sql_instance.schema_name,
                                                                              query)
        rows = agent_rpc_client.call('query_in_database',
                                     stmt,
                                     self.slow_sql_instance.db_name,
                                     return_tuples=True)
        for row in rows:
            if row[2]:
                index = Index()
                index.db_name = self.slow_sql_instance.db_name
                index.schema_name = row[0]
                index.table_name = row[1]
                index.column_name = row[2]
                recommend_indexes.append(str(index))
        return ';'.join(recommend_indexes)

    @exception_follower(output=list)
    @exception_catcher
    def acquire_timed_task(self) -> list:
        timed_task_list = []
        sequences = dai.get_metric_sequence('db_timed_task_failure_count', self.query_start_time,
                                            self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
            dbname=self.slow_sql_instance.db_name).fetchall()
        sequences = [sequence for sequence in sequences if sequence.labels]
        for sequence in sequences:
            timed_task = TimedTask()
            logging.debug('[SLOW QUERY] acquire_timed_task_info: %s.', sequence)
            timed_task.job_id = sequence.labels['job_id']
            timed_task.priv_user = sequence.labels['priv_user']
            timed_task.dbname = sequence.labels['dbname']
            timed_task.job_status = sequence.labels['job_status']
            timed_task.last_start_date = to_ts(sequence.labels['last_start_date']) * 1000
            timed_task.last_end_date = to_ts(sequence.labels['last_end_date']) * 1000
            timed_task_list.append(timed_task)
            logging.info("timed_task_date: %s, %s, %s", timed_task.last_start_date, timed_task.last_end_date,
                         timed_task.__dict__)
        return timed_task_list
