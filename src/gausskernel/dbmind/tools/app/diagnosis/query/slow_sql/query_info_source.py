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
from datetime import datetime, timedelta
from typing import Dict, List

from dbmind.common.parser.sql_parsing import is_num, str2int
from dbmind.common.utils import ExceptionCatch
from dbmind.service import dai

excetpion_catcher = ExceptionCatch(strategy='exit', name='SLOW QUERY')


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
        self.last_autovacuum = 0
        self.last_autoanalyze = 0
        self.vacuum = 0
        self.analyze = 0
        self.table_size = 0
        self.index = []
        self.redundant_index = []


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
        self.max_conn = 0
        self.used_conn = 0


class SystemInfo:
    """Data structure to save system information such as database address, IOWAIT, IOCAPACITY, CPU_USAGE, etc"""

    def __init__(self):
        self.db_host = None
        self.db_port = None
        self.iops = 0.0
        self.ioutils = {}
        self.iocapacity = 0.0
        self.iowait = 0.0
        self.cpu_usage = 0.0
        self.mem_usage = 0.0
        self.load_average = 0.0


class QueryContext:
    """The object of slow query data processing factory"""

    def __init__(self, slow_sql_instance, default_fetch_interval=15, expansion_factor=5,
                 retrieval_time=5):
        """
        :param slow_sql_instance: The instance of slow query
        :param default_fetch_interval: fetch interval of data source
        :param expansion_factor: Ensure that the time expansion rate of the data can be collected
        :param retrieval_time: Historical index retrieval time
        """
        self.fetch_interval = default_fetch_interval
        self.expansion_factor = expansion_factor
        self.retrieval_time = retrieval_time
        self.slow_sql_instance = slow_sql_instance
        self.query_start_time = datetime.fromtimestamp(self.slow_sql_instance.start_at / 1000)
        self.query_end_time = datetime.fromtimestamp(
            self.slow_sql_instance.start_at / 1000 +
            self.slow_sql_instance.duration_time / 1000 +
            self.expansion_factor * self.acquire_fetch_interval()
        )
        logging.debug('[SLOW QUERY] slow sql info: %s', slow_sql_instance.__dict__)
        logging.debug('[SLOW QUERY] fetch start time: %s, fetch end time: %s', self.query_start_time, self.query_end_time)
        logging.debug('[SLOW QUERY] fetch interval: %s', self.fetch_interval)


    @excetpion_catcher
    def acquire_pg_class(self) -> Dict:
        """Get all object information in the database"""
        pg_class = {}
        sequences = dai.get_metric_sequence('pg_class_relsize', self.query_start_time, self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchall()
        for sequence in sequences:
            pg_class['db_host'] = self.slow_sql_instance.db_host
            pg_class['db_port'] = self.slow_sql_instance.db_port
            db_name = sequence.labels['datname']
            schema_name = sequence.labels['nspname']
            table_name = sequence.labels['relname']
            if db_name not in pg_class:
                pg_class[db_name] = {}
                pg_class[db_name][schema_name] = []
                pg_class[db_name][schema_name].append(table_name)
            elif schema_name not in pg_class[db_name]:
                pg_class[db_name][schema_name] = []
                pg_class[db_name][schema_name].append(table_name)
            else:
                pg_class[db_name][schema_name].append(table_name)
        return pg_class

    @excetpion_catcher
    def acquire_fetch_interval(self) -> int:
        """Get data source collection frequency"""
        sequence = dai.get_latest_metric_sequence("os_disk_iops", self.retrieval_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        logging.debug('[SLOW QUERY] acquire_fetch_interval: %s.', sequence)
        timestamps = sequence.timestamps
        if len(timestamps) >= 2:
            self.fetch_interval = int(timestamps[-1]) // 1000 - int(timestamps[-2]) // 1000
        return self.fetch_interval

    @excetpion_catcher
    def acquire_lock_info(self) -> LockInfo:
        """Get lock information during slow SQL execution"""
        blocks_info = LockInfo()
        locks_sequences = dai.get_metric_sequence("pg_lock_sql_locked_times", self.query_start_time,
                                                  self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchall()
        logging.debug('[SLOW QUERY] acquire_lock_info: %s.', locks_sequences)
        locked_query, locked_query_start, locker_query, locker_query_start = [], [], [], []
        for locks_sequence in locks_sequences:
            logging.debug('[SLOW QUERY] acquire_lock_info: %s.', locks_sequence)
            locked_query.append(locks_sequence.labels.get('locked_query', 'Unknown'))
            locked_query_start.append(locks_sequence.labels.get('locked_query_start', 'Unknown'))
            locker_query.append(locks_sequence.labels.get('locker_query', 'Unknown'))
            locker_query_start.append(locks_sequence.labels.get('locker_query_start', 'Unknown'))
        blocks_info.locked_query = locked_query
        blocks_info.locked_query_start = locked_query_start
        blocks_info.locker_query = locker_query
        blocks_info.locker_query_start = locker_query_start

        return blocks_info

    @excetpion_catcher
    def acquire_tables_structure_info(self) -> List:
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
                pg_stat_user_tables_info = dai.get_metric_sequence("pg_tables_expansion_rate_dead_rate",
                                                                   self.query_start_time,
                                                                   self.query_end_time).from_server(
                    f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    schemaname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                logging.debug('[SLOW QUERY] acquire_tables_structure[pg_stat_user_tables]: %s.', pg_stat_user_tables_info)
                pg_table_size_info = dai.get_metric_sequence("pg_tables_size_bytes", self.query_start_time,
                                                             self.query_end_time).from_server(
                    f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    nspname=f"{schema_name}").filter(relname=f"{table_name}").fetchone()
                logging.debug('[SLOW QUERY] acquire_tables_structure[pg_table_size]: %s.', pg_table_size_info)
                index_number_info = dai.get_metric_sequence("pg_index_idx_scan", self.query_start_time,
                                                            self.query_end_time).from_server(
                    f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    nspname=f"{schema_name}").filter(tablename=f"{table_name}").fetchall()
                logging.debug('[SLOW QUERY] acquire_tables_structure[index_number]: %s.', index_number_info)
                redundant_index_info = dai.get_metric_sequence("pg_never_used_indexes_index_size",
                                                               self.query_start_time,
                                                               self.query_end_time).from_server(
                    f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").filter(
                    datname=f"{self.slow_sql_instance.db_name}").filter(
                    schemaname=f"{schema_name}").filter(relname=f"{table_name}").fetchall()
                logging.debug('[SLOW QUERY] acquire_tables_structure[redundant_index]: %s.', redundant_index_info)
                if pg_stat_user_tables_info.values:
                    table_info.dead_tuples = int(pg_stat_user_tables_info.labels['n_dead_tup'])
                    table_info.live_tuples = int(pg_stat_user_tables_info.labels['n_live_tup'])
                    table_info.last_autovacuum = str2int(pg_stat_user_tables_info.labels['last_autovacuum']) * 1000 if \
                        is_num(pg_stat_user_tables_info.labels['last_autovacuum']) else 0
                    table_info.last_autoanalyze = str2int(pg_stat_user_tables_info.labels['last_autoanalyze']) * 1000 if \
                        is_num(pg_stat_user_tables_info.labels['last_autoanalyze']) else 0
                    table_info.vacuum = str2int(pg_stat_user_tables_info.labels['last_vacuum']) * 1000 if is_num(
                        pg_stat_user_tables_info.labels[
                            'last_vacuum']) else 0
                    table_info.analyze = str2int(pg_stat_user_tables_info.labels['last_analyze']) * 1000 if is_num(
                        pg_stat_user_tables_info.labels[
                            'last_analyze']) else 0
                    table_info.dead_rate = round(float(max(pg_stat_user_tables_info.values)), 4)
                if pg_table_size_info.values:
                    table_info.table_size = round(float(max(pg_table_size_info.values)) / 1024 / 1024, 4)
                if index_number_info:
                    table_info.index = [item.labels['relname'] for item in index_number_info if item.labels]
                if redundant_index_info:
                    table_info.redundant_index = [item.labels['indexrelname'] for item in redundant_index_info]
                table_structure.append(table_info)

        return table_structure

    @excetpion_catcher
    def acquire_database_info(self) -> DatabaseInfo:
        """Acquire table database information related to slow query"""
        database_info = DatabaseInfo()
        days_time_interval = 24 * 60 * 60
        cur_tps_sequences = dai.get_metric_sequence("gaussdb_qps_by_instance", self.query_start_time,
                                                    self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchone()
        logging.debug('[SLOW QUERY] acquire_database_info[cur_tps]: %s.', cur_tps_sequences)
        his_tps_sequences = dai.get_metric_sequence("gaussdb_qps_by_instance",
                                                    self.query_start_time - timedelta(seconds=days_time_interval),
                                                    self.query_end_time - timedelta(
                                                        seconds=days_time_interval)).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchone()
        logging.debug('[SLOW QUERY] acquire_database_info[his_tps]: %s.', his_tps_sequences)
        max_conn_sequence = dai.get_metric_sequence("pg_connections_max_conn", self.query_start_time,
                                                    self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchone()
        logging.debug('[SLOW QUERY] acquire_database_info[max_conn]: %s.', max_conn_sequence)
        used_conn_sequence = dai.get_metric_sequence("pg_connections_used_conn", self.query_start_time,
                                                     self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}:{self.slow_sql_instance.db_port}").fetchone()
        logging.debug('[SLOW QUERY] acquire_database_info[used_conn]: %s.', used_conn_sequence)
        if his_tps_sequences.values:
            database_info.history_tps = [float(item) for item in his_tps_sequences.values]
        if cur_tps_sequences.values:
            database_info.current_tps = [float(item) for item in cur_tps_sequences.values]
        if max_conn_sequence.values:
            database_info.max_conn = int(max(max_conn_sequence.values))
            database_info.used_conn = int(max(used_conn_sequence.values))

        return database_info

    @excetpion_catcher
    def acquire_system_info(self) -> SystemInfo:
        """Acquire system information on the database server """
        system_info = SystemInfo()
        iops_info = dai.get_metric_sequence("os_disk_iops", self.query_start_time, self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        logging.debug('[SLOW QUERY] acquire_database_info[iops]: %s.', iops_info)
        ioutils_info = dai.get_metric_sequence("os_disk_ioutils", self.query_start_time,
                                               self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchall()
        logging.debug('[SLOW QUERY] acquire_database_info[ioutils]: %s.', ioutils_info)
        iocapacity_info = dai.get_metric_sequence("os_disk_iocapacity", self.query_start_time,
                                                  self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        logging.debug('[SLOW QUERY] acquire_database_info[iocapacity]: %s.', iocapacity_info)
        iowait_info = dai.get_metric_sequence("os_cpu_iowait", self.query_start_time, self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        logging.debug('[SLOW QUERY] acquire_database_info[iowait]: %s.', iowait_info)
        cpu_usage_info = dai.get_metric_sequence("os_cpu_usage", self.query_start_time,
                                                 self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        logging.debug('[SLOW QUERY] acquire_database_info[cpu_usage]: %s.', cpu_usage_info)
        mem_usage_info = dai.get_metric_sequence("os_mem_usage", self.query_start_time,
                                                 self.query_end_time).from_server(
            f"{self.slow_sql_instance.db_host}").fetchone()
        logging.debug('[SLOW QUERY] acquire_database_info[mem_usage]: %s.', mem_usage_info)
        load_average_info = dai.get_metric_sequence("node_load1", self.query_start_time, self.query_end_time).filter(
            instance=f"{self.slow_sql_instance.db_host}:9100").fetchone()
        logging.debug('[SLOW QUERY] acquire_database_info[load_average]: %s.', load_average_info)
        system_info.iops = int(max(iops_info.values))
        ioutils_dict = {item.labels['device']: round(float(max(item.values)), 4) for item in ioutils_info}
        system_info.ioutils = ioutils_dict
        system_info.iocapacity = round(float(max(iocapacity_info.values)), 4)
        system_info.iowait = round(float(max(iowait_info.values)), 4)
        system_info.cpu_usage = round(float(max(cpu_usage_info.values)), 4)
        system_info.mem_usage = round(float(max(mem_usage_info.values)), 4)
        system_info.load_average = round(float(max(load_average_info.values)), 4)

        return system_info
