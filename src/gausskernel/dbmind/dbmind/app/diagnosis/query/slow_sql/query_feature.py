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
from dbmind.common.parser import sql_parsing

from dbmind.app import monitoring
from ..slow_sql import significance_detection
from ..slow_sql.query_info_source import QueryContext


def _search_table_structures(table_structures, table_name):
    for table_structure in table_structures:
        if table_structure.table_name == table_name:
            return table_structure


def _search_in_existing_indexes(index_info, seqscan_info):
    result = []
    for index_name, index_columns in index_info.items():
        if set(index_columns) & set(seqscan_info.columns):
            result.append({'index_name': index_name, 'index_column': index_columns})
    return result


class QueryFeature:
    """
    Feature processing factory
    """

    def __init__(self, query_context: QueryContext = None):
        """
        :param query_context context including the necessary information of metrics when SQL occurs

        self.table_structure: data structure to save table structure
        self.lock_info: data structure to save lock information of slow query
        self.database_info: data structure to save lock information of database info such as QPS, CONNECTION, etc
        self.system_info: data structure to save system information
        self.detail: data structure to save diagnosis information
        """
        self.slow_sql_instance = query_context.slow_sql_instance
        self.query_context = query_context
        self.table_structure = None
        self.lock_info = None
        self.database_info = None
        self.system_info = None
        self.network_info = None
        self.replication_info = None
        self.pg_setting_info = None
        self.pg_bgwriter_info = None
        self.table_skewness_info = None
        self.sql_count_info = None
        self.plan_parse_info = None
        self.recommend_index_info = None
        self.rewritten_sql_info = None
        self.timed_task_info = None
        self.wait_event = None
        self.detail = {}
        self.suggestion = {}

    def initialize_metrics(self):
        """Initialize the data structure such as database_info, table_structure, lock_info, etc"""
        self.database_info = self.query_context.acquire_database_info()
        self.table_structure = self.query_context.acquire_tables_structure_info()
        self.lock_info = self.query_context.acquire_lock_info()
        self.system_info = self.query_context.acquire_system_info()
        self.network_info = self.query_context.acquire_network_info()
        self.replication_info = self.query_context.acquire_pg_replication_info()
        self.pg_bgwriter_info = self.query_context.acquire_bgwriter_info()
        self.sql_count_info = self.query_context.acquire_sql_count_info()
        self.rewritten_sql_info = self.query_context.acquire_rewritten_sql()
        self.recommend_index_info = self.query_context.acquire_recommend_index()
        self.plan_parse_info = self.query_context.acquire_plan_parse()
        self.timed_task_info = self.query_context.acquire_timed_task()
        self.wait_event = self.query_context.acquire_wait_event()

    @property
    def select_type(self) -> bool:
        """Determine whether it is a select statement"""
        filter_query = self.slow_sql_instance.query.strip().lower()
        if filter_query.startswith('select'):
            return True
        return False

    @property
    def update_type(self) -> bool:
        """Determine whether it is a update statement"""
        filter_query = self.slow_sql_instance.query.strip().lower()
        if filter_query.startswith('update'):
            return True
        return False

    @property
    def delete_type(self) -> bool:
        """Determine whether it is a delete statement"""
        filter_query = self.slow_sql_instance.query.strip().lower()
        if filter_query.startswith('delete'):
            return True
        return False

    @property
    def insert_type(self) -> bool:
        """Determine whether it is a insert statement"""
        filter_query = self.slow_sql_instance.query.strip().lower()
        if filter_query.startswith('insert'):
            return True
        return False

    @property
    def query_block(self) -> bool:
        """Determine whether the query is blocked during execution"""
        if not any((self.slow_sql_instance.lock_wait_count, self.slow_sql_instance.lwlock_wait_count)):
            return False
        for index, query_info in enumerate(self.lock_info.locked_query):
            # The field query of SlowQuery object has been already standardized.
            if self.slow_sql_instance.query == sql_parsing.standardize_sql(query_info):
                self.detail['lock_info'] = "SQL was blocked by: '%s'" % self.lock_info.locker_query[index]
                return True
        if self.slow_sql_instance.lock_wait_count and not self.slow_sql_instance.lwlock_wait_count:
            self.detail['lock_info'] = "lock count: %s" % self.slow_sql_instance.lock_wait_count
        elif not self.slow_sql_instance.lock_wait_count and self.slow_sql_instance.lwlock_wait_count:
            self.detail['lock_info'] = "lwlock count: %s" % self.slow_sql_instance.lwlock_wait_count
        else:
            self.detail['lock_info'] = "lock count %s, lwlock count %s" % (
                self.slow_sql_instance.lock_wait_count, self.slow_sql_instance.lwlock_wait_count)
        return True

    @property
    def large_table(self) -> bool:
        """Determine whether the query related table is large"""
        if not self.table_structure:
            return False
        tuples_info = {f"{item.schema_name}:{item.table_name}": {'live_tuples': item.live_tuples,
                                                                 'dead_tuples': item.dead_tuples,
                                                                 'table_size': item.table_size}
                       for item in self.table_structure}
        self.detail['large_table'] = {}
        for table_name, table_info in tuples_info.items():
            if table_info['live_tuples'] + table_info['dead_tuples'] > monitoring.get_threshold('tuple_number_threshold') or \
                    table_info['table_size'] > monitoring.get_threshold('table_total_size_threshold'):
                table_info['table_size'] = "%sMB" % table_info['table_size']
                self.detail['large_table'][table_name] = table_info
        if self.detail.get('large_table'):
            return True
        return False

    @property
    def large_dead_tuples(self) -> bool:
        """Determine whether the query related table has too many dead tuples"""
        if not self.table_structure or not self.large_table or self.insert_type:
            return False
        dead_rate_info = {f"{item.schema_name}:{item.table_name}": item.dead_rate for item in
                          self.table_structure}
        self.detail['dead_rate'] = {}
        if self.plan_parse_info is None:
            for table_name, dead_rate in dead_rate_info.items():
                if dead_rate > monitoring.get_threshold('dead_rate_threshold'):
                    self.detail['dead_rate'][table_name] = dead_rate
        else:
            seqscan_operators = self.plan_parse_info.find_operators('Seq Scan', accurate=True)
            indexscan_operators = self.plan_parse_info.find_operators('Index Scan', accurate=True)
            indexonlyscan_operators = self.plan_parse_info.find_operators('Index Only Scan', accurate=True)
            seqscan_tables = [item.table for item in seqscan_operators]
            indexscan_tables = [item.table for item in indexscan_operators + indexonlyscan_operators]
            for table in seqscan_tables + indexscan_tables:
                for table_info in self.table_structure:
                    if table_info.table_name == table and table_info.dead_rate > monitoring.get_threshold(
                            'dead_rate_threshold'):
                        self.detail['dead_rate'][
                            f"{table_info.schema_name}:{table_info.table_name}"] = table_info.dead_rate
        if self.detail.get('dead_rate'):
            return True
        return False

    @property
    def large_fetch_tuples(self) -> bool:
        """Determine whether the query related table has too many fetch tuples"""
        if self.plan_parse_info is not None:
            returned_rows = self.plan_parse_info.root_node.rows
            scan_info = self.plan_parse_info.find_operators('Scan')
            self.detail['fetched_tuples'] = []
            self.detail['fetched_tuples_rate'] = []
            self.detail['returned_rows'] = returned_rows
            for node in scan_info:
                table = node.table
                fetch_rows = node.rows
                table_structure = _search_table_structures(self.table_structure, table)
                if table_structure is None and len(self.table_structure) == 1:
                    table = self.table_structure[0]
                if fetch_rows > monitoring.get_threshold('fetch_tuples_threshold'):
                    self.detail['fetched_tuples'].append({table: fetch_rows})
                    if table_structure is not None and table_structure.live_tuples > 0:
                        self.detail['fetched_tuples_rate'].append(
                            {table: round(fetch_rows / table_structure.live_tuples, 4)})
                    else:
                        self.detail['fetched_tuples_rate'].append({table: 'UNKNOWN'})
            if self.detail['fetched_tuples']:
                return True
            return False
        fetched_tuples = self.slow_sql_instance.n_tuples_fetched
        returned_tuples = self.slow_sql_instance.n_tuples_returned
        returned_rows = self.slow_sql_instance.n_returned_rows
        if fetched_tuples + returned_tuples > monitoring.get_threshold('fetch_tuples_threshold') or \
                returned_rows > monitoring.get_threshold('returned_rows_threshold'):
            self.detail['returned_rows'] = returned_rows
            self.detail['fetched_tuples'] = fetched_tuples + returned_tuples
            self.detail['fetched_tuples_rate'] = 'UNKNOWN'
            return True
        return False

    @property
    def lower_hit_ratio(self) -> bool:
        """Determine whether the query related table has lower hit ratio"""
        self.detail['hit_rate'] = self.slow_sql_instance.hit_rate
        if not self.large_table or self.insert_type:
            return False
        if self.slow_sql_instance.hit_rate < monitoring.get_threshold('hit_rate_threshold'):
            return True
        return False

    @property
    def redundant_index(self) -> bool:
        """Determine whether the query related table has too redundant index"""
        if not self.table_structure or not self.large_table:
            return False
        redundant_index_info = {f"{item.schema_name}:{item.table_name}": item.redundant_index for item in
                                self.table_structure}
        self.detail['redundant_index'] = {}
        if self.plan_parse_info is not None:
            indexscan_operators = self.plan_parse_info.find_operators('Index Scan')
            indexonlyscan_operators = self.plan_parse_info.find_operators('Index Only Scan')
            indexscan_indexlist = [item.index for item in indexscan_operators + indexonlyscan_operators]
        else:
            indexscan_indexlist = []
        for table_name, redundant_index_list in redundant_index_info.items():
            not_use_redundant_index_list = []
            for redundant_index in redundant_index_list:
                if redundant_index not in indexscan_indexlist:
                    not_use_redundant_index_list.append(redundant_index)
            if not_use_redundant_index_list:
                self.detail['redundant_index'][table_name] = not_use_redundant_index_list
        if self.detail.get('redundant_index'):
            return True
        return False

    @property
    def update_redundant_index(self) -> bool:
        """Determine whether the update query related table has redundant index"""
        return self.update_type and self.redundant_index

    @property
    def insert_redundant_index(self) -> bool:
        """Determine whether the insert query related table has redundant index"""
        return self.insert_type and self.redundant_index

    @property
    def delete_redundant_index(self) -> bool:
        """Determine whether the delete query related table has redundant index"""
        return self.delete_type and self.redundant_index

    @property
    def large_updated_tuples(self) -> bool:
        """Determine whether the query related table has large update tuples"""
        if self.update_type and self.plan_parse_info is not None:
            update_info = self.plan_parse_info.find_operators('Update')
            for node in update_info:
                table = node.table
                rows = node.rows
                table_structure = _search_table_structures(self.table_structure, table)
                if rows > monitoring.get_threshold('updated_tuples_threshold'):
                    self.detail['updated_tuples'] = {table: rows}
                    if table_structure is not None and table_structure.live_tuples > 0:
                        self.detail['updated_tuples_rate'] = {table: round(
                            rows / table_structure.live_tuples, 4)}
                    else:
                        self.detail['updated_tuples_rate'] = {table: 'UNKNOWN'}
                    return True

        updated_tuples = self.slow_sql_instance.n_tuples_updated
        if updated_tuples > monitoring.get_threshold('updated_tuples_threshold'):
            self.detail['updated_tuples'] = updated_tuples
            if len(self.table_structure) == 1 and self.table_structure[0].live_tuples > 0:
                self.detail['updated_tuples_rate'] = round(updated_tuples / self.table_structure[0].live_tuples, 4)
            self.detail['updated_tuples_rate'] = 'UNKNOWN'
            return True

        return False

    @property
    def large_inserted_tuples(self) -> bool:
        """Determine whether the query related table has large insert tuples"""
        if self.insert_type and self.plan_parse_info is not None:
            insert_info = self.plan_parse_info.find_operators('Insert')
            for node in insert_info:
                table = node.table
                rows = node.rows
                table_structure = _search_table_structures(self.table_structure, table)
                if rows > monitoring.get_threshold('inserted_tuples_threshold'):
                    self.detail['inserted_tuples'] = {table: rows}
                    if table_structure is not None and table_structure.live_tuples > 0:
                        self.detail['inserted_tuples_rate'] = {table: round(
                            rows / table_structure.live_tuples, 4)}
                    else:
                        self.detail['inserted_tuples_rate'] = {table: 'UNKNOWN'}
                    return True

        inserted_tuples = self.slow_sql_instance.n_tuples_inserted
        if inserted_tuples > monitoring.get_threshold('inserted_tuples_threshold'):
            self.detail['inserted_tuples'] = inserted_tuples
            if len(self.table_structure) == 1 and self.table_structure[0].live_tuples > 0:
                self.detail['inserted_tuples_rate'] = round(inserted_tuples / self.table_structure[0].live_tuples, 4)
            self.detail['inserted_tuples_rate'] = 'UNKNOWN'
            return True

        return False

    @property
    def large_index_number(self) -> bool:
        """Determine whether the query related table has too many indexes"""
        if not self.table_structure:
            return False
        self.detail['index'] = {}
        for table in self.table_structure:
            if len(table.index) > monitoring.get_threshold('index_number_threshold') and \
                    len(table.index) / table.column_number > monitoring.get_threshold('index_number_rate_threshold'):
                self.detail['index'][table.table_name] = f"{table.table_name} has {len(table.index)} index.\n"
        if self.detail.get('index'):
            return True
        return False

    @property
    def index_number_insert(self) -> bool:
        """Determine whether the insert query related table has too many indexes"""
        return self.insert_type and self.large_index_number

    @property
    def large_deleted_tuples(self) -> bool:
        """Determine whether the query related table has too many delete tuples"""
        if self.delete_type and self.plan_parse_info is not None:
            delete_info = self.plan_parse_info.find_operators('Delete')
            for node in delete_info:
                table = node.table
                rows = node.rows
                table_structure = _search_table_structures(self.table_structure, table)
                if rows > monitoring.get_threshold('deleted_tuples_threshold'):
                    self.detail['deleted_tuples'] = {table: rows}
                    if table_structure is not None and table_structure.live_tuples > 0:
                        self.detail['deleted_tuples_rate'] = {table: round(
                            rows / table_structure.live_tuples, 4)}
                    else:
                        self.detail['deleted_tuples_rate'] = {table: 'UNKNOWN'}
                    return True

        deleted_tuples = self.slow_sql_instance.n_tuples_deleted
        if deleted_tuples > monitoring.get_threshold('deleted_tuples_threshold'):
            self.detail['deleted_tuples'] = deleted_tuples
            if len(self.table_structure) == 1 and self.table_structure[0].live_tuples > 0:
                self.detail['deleted_tuples_rate'] = round(deleted_tuples / self.table_structure[0].live_tuples, 4)
            self.detail['deleted_tuples_rate'] = 'UNKNOWN'
            return True

        return False

    @property
    def external_sort(self) -> bool:
        """Determine whether the query related table has external sort"""
        if self.slow_sql_instance.sort_count and not self.slow_sql_instance.sort_mem_used:
            self.detail['external_sort'] = f"The probability of falling disk behavior during execution is " \
                                           f"{self.slow_sql_instance.sort_count}% "
            return True
        elif self.slow_sql_instance.hash_count and not self.slow_sql_instance.hash_mem_used:
            self.detail['external_sort'] = f"The probability of falling disk behavior during execution is " \
                                           f"{self.slow_sql_instance.hash_count}%"
            return True
        elif self.slow_sql_instance.sort_spill_count:
            self.detail['external_sort'] = f"The probability of falling disk behavior during execution is " \
                                           f"{self.slow_sql_instance.sort_spill_count}%"
            return True
        elif self.slow_sql_instance.hash_spill_count:
            self.detail['external_sort'] = f"The probability of falling disk behavior during execution is " \
                                           f"{self.slow_sql_instance.hash_spill_count}%"
            return True
        else:
            return False

    @property
    def vacuum_operation(self) -> bool:
        """Determine whether the query related table has vacuum operation"""
        if not self.table_structure or not self.large_table:
            return False
        # Based on the general rules, it is found that the affected time is 6 seconds is more reasonable
        probable_time = 6 * 1000
        auto_vacuum_info = {f"{item.schema_name}:{item.table_name}": item.last_autovacuum for item in
                            self.table_structure}
        user_vacuum_info = {f"{item.schema_name}:{item.table_name}": item.vacuum for item in
                            self.table_structure}
        self.detail['autovacuum'] = {}
        self.detail['vacuum'] = {}
        for table_name, autovacuum_time in auto_vacuum_info.items():
            if self.slow_sql_instance.start_at <= autovacuum_time <= self.slow_sql_instance.start_at + \
                    self.slow_sql_instance.duration_time or \
                    autovacuum_time < self.slow_sql_instance.start_at < autovacuum_time + probable_time:
                self.detail['autovacuum'][table_name] = autovacuum_time

        for table_name, vacuum_time in user_vacuum_info.items():
            if self.slow_sql_instance.start_at <= vacuum_time <= self.slow_sql_instance.start_at + \
                    self.slow_sql_instance.duration_time or \
                    vacuum_time < self.slow_sql_instance.start_at < vacuum_time + probable_time:
                self.detail['vacuum'][table_name] = vacuum_time
        if self.detail.get('autovacuum') or self.detail.get('vacuum'):
            return True
        return False

    @property
    def analyze_operation(self) -> bool:
        """Determine whether the query related table has analyze operation"""
        if not self.table_structure or not self.large_table:
            return False
        # Based on the general rules, it is found that the affected time is 6 seconds is more reasonable
        probable_time = 6 * 1000
        auto_analyze_info = {f"{item.schema_name}:{item.table_name}": item.last_autoanalyze for item in
                             self.table_structure}
        user_analyze_info = {f"{item.schema_name}:{item.table_name}": item.analyze for item in
                             self.table_structure}
        self.detail['autoanalyze'] = {}
        self.detail['analyze'] = {}
        for table_name, autoanalyze_time in auto_analyze_info.items():
            if self.slow_sql_instance.start_at <= autoanalyze_time <= self.slow_sql_instance.start_at + \
                    self.slow_sql_instance.duration_time or \
                    autoanalyze_time < self.slow_sql_instance.start_at < autoanalyze_time + probable_time:
                self.detail['autoanalyze'][table_name] = autoanalyze_time
        for table_name, analyze_time in user_analyze_info.items():
            if self.slow_sql_instance.start_at <= analyze_time <= self.slow_sql_instance.start_at + \
                    self.slow_sql_instance.duration_time or \
                    analyze_time < self.slow_sql_instance.start_at < analyze_time + probable_time:
                self.detail['analyze'][table_name] = analyze_time
        if self.detail.get('autoanalyze') or self.detail.get('analyze'):
            return True
        return False

    @property
    def tps_significant_change(self) -> bool:
        """Determine whether the QPS of the related table has a mutation"""
        cur_database_tps = self.database_info.current_tps
        his_database_tps = self.database_info.history_tps
        if not his_database_tps and not cur_database_tps:
            return False
        elif not his_database_tps and cur_database_tps:
            if max(cur_database_tps) > monitoring.get_param('tps_threshold'):
                self.detail['tps'] = round(max(cur_database_tps), 4)
                return True
            else:
                return False
        elif his_database_tps and not cur_database_tps:
            return False
        else:
            if significance_detection.detect(cur_database_tps, his_database_tps) and max(
                    cur_database_tps) > monitoring.get_param('tps_threshold'):
                self.detail['tps'] = round(max(cur_database_tps), 4)
                return True
            return False

    @property
    def large_iops(self) -> bool:
        """Determine whether the QPS of the related table has large IOPS"""
        if self.system_info.iops > monitoring.get_param('iops_threshold'):
            self.detail['system_cause']['iops'] = self.system_info.iops
            return True
        return False

    @property
    def large_ioutils(self) -> bool:
        """Determine whether the QPS of the related table has large IOUTILS"""
        ioutils_dict = {}
        for device, ioutils in self.system_info.ioutils.items():
            if ioutils > monitoring.get_param('disk_ioutils_threshold'):
                ioutils_dict[device] = ioutils
        if ioutils_dict:
            self.detail['system_cause']['ioutils'] = ioutils_dict
            return True
        return False

    @property
    def large_iocapacity(self) -> bool:
        """Determine whether the QPS of the related table has large IOCAPACITY"""
        if self.system_info.iocapacity > monitoring.get_param('io_capacity_threshold'):
            self.detail['system_cause']['iocapacity'] = self.system_info.iocapacity
            return True
        return False

    @property
    def large_io_delay_time(self) -> bool:
        """Determine whether the QPS of the related table has large IOUTILS"""
        io_delay_dict = {}
        for device, read_delay_time in self.system_info.io_read_delay.items():
            if read_delay_time > monitoring.get_param('io_delay_threshold'):
                device = f"{device}-read"
                io_delay_dict[device] = read_delay_time
        for device, write_delay_time in self.system_info.io_write_delay.items():
            if write_delay_time > monitoring.get_param('io_delay_threshold'):
                device = f"{device}-write"
                io_delay_dict[device] = write_delay_time
        if io_delay_dict:
            self.detail['system_cause']['io_delay'] = io_delay_dict
            return True
        return False

    @property
    def abnormal_io_condition(self) -> bool:
        if self.large_iops or self.large_ioutils or self.large_ioutils or self.large_io_delay_time:
            return True
        return False

    @property
    def large_iowait(self) -> bool:
        """Determine whether the QPS of the related table has large IOWAIT"""
        if self.system_info.iowait > monitoring.get_param('io_wait_threshold'):
            self.detail['system_cause']['iowait'] = self.system_info.iowait
            return True
        return False

    @property
    def large_load_average(self) -> bool:
        """Determine whether the QPS of the related table has large LOAD AVERAGE"""
        if self.system_info.load_average / self.system_info.cpu_core_number > monitoring.get_param('load_average_threshold'):
            self.detail['system_cause']['load_average'] = self.system_info.load_average
            return True
        return False

    @property
    def large_cpu_usage(self) -> bool:
        """Determine whether the QPS of the related table has large CPU USAGE"""
        if self.system_info.cpu_usage > monitoring.get_param('cpu_usage_threshold'):
            self.detail['system_cause']['cpu_usage'] = self.system_info.cpu_usage
            return True
        return False

    @property
    def large_process_fds(self) -> bool:
        """Determinate whether the fds of process is too large"""
        if self.system_info.process_fds_rate > monitoring.get_param('handler_occupation_threshold'):
            self.detail['system_cause']['process_fds_rate'] = self.system_info.process_fds_rate
            return True
        return False

    @property
    def large_mem_usage(self) -> bool:
        """Determine whether the QPS of the related table has large MEM USAGE"""
        if self.system_info.mem_usage > monitoring.get_param('mem_usage_threshold'):
            self.detail['system_cause']['mem_usage'] = self.system_info.mem_usage
            return True
        return False

    @property
    def large_disk_usage(self) -> bool:
        """Determine whether the QPS of the related table has large IOUTILS"""
        disk_usage_dict = {}
        for device, disk_usage in self.system_info.ioutils.items():
            if disk_usage > monitoring.get_param('disk_usage_threshold'):
                disk_usage_dict[device] = disk_usage
        if disk_usage_dict:
            self.detail['system_cause']['disk_usage'] = disk_usage_dict
            return True
        return False

    @property
    def abnormal_thread_pool(self) -> bool:
        """Determine whether the rate of thread is too large"""
        if not self.database_info.thread_pool:
            return False
        if self.database_info.thread_pool['worker_info_default'] > 0 and \
                self.database_info.thread_pool['session_info_total'] > 0:
            if 1 - self.database_info.thread_pool['worker_info_idle'] / \
                    self.database_info.thread_pool['worker_info_default'] > monitoring.get_param(
                'thread_occupy_rate_threshold') or \
                    self.database_info.thread_pool['session_info_idle'] / \
                    self.database_info.thread_pool['session_info_total'] > monitoring.get_param(
                    'idle_session_occupy_rate_threshold'):
                self.detail['thread_pool'] = "occupy rate of thread pool: %s, idle rate of session: %s" % (
                    round(1 - self.database_info.thread_pool['worker_info_idle'] /
                          self.database_info.thread_pool['worker_info_default'], 4),
                    round(self.database_info.thread_pool['session_info_idle'] /
                          self.database_info.thread_pool['session_info_total'], 4))
                return True
        return False

    @property
    def large_connection_occupy_rate(self) -> bool:
        """Determine whether the rate of connection is too large"""
        if not self.pg_setting_info:
            return False
        if self.pg_setting_info['max_connections'].setting > 0 and \
                self.database_info.used_conn / self.pg_setting_info['max_connections'].setting > \
                monitoring.get_param('connection_usage_threshold'):
            self.detail['connection_rate'] = round(
                self.database_info.used_conn / self.pg_setting_info['max_connections'].setting, 4)
            return True
        return False

    @property
    def abnormal_wait_event(self) -> bool:
        """Determinate the efficient of double write"""
        self.detail['wait_event'] = {}
        typical_operators = []
        if self.plan_parse_info is not None:
            if self.plan_parse_info.find_operators('Sort'):
                typical_operators.append('Sort - write file')
                typical_operators.append('Sort')
            if self.plan_parse_info.find_operators('nestloop'):
                typical_operators.append('NestLoop')
        for wait_event in self.wait_event:
            last_updated = wait_event.last_updated
            if self.slow_sql_instance.start_at < last_updated < \
                    self.slow_sql_instance.start_at + self.slow_sql_instance.duration_time:
                if wait_event == 'Sort - write file' and wait_event in typical_operators:
                    self.detail['wait_event'][wait_event] = last_updated
                elif wait_event == 'NestLoop' and wait_event in typical_operators:
                    self.detail['wait_event'][wait_event] = last_updated
                else:
                    self.detail['wait_event'][wait_event] = last_updated
        if self.detail['wait_event']:
            return True
        return False

    @property
    def lack_of_statistics(self) -> bool:
        if not self.table_structure:
            return False
        auto_analyze_info = {f"{item.schema_name}:{item.table_name}": item.last_autoanalyze for item in
                             self.table_structure}
        manual_analyze_info = {f"{item.schema_name}:{item.table_name}": item.analyze for item in
                               self.table_structure}

        self.detail['update_statistics'] = {}
        for table_name, auto_analyze_time in auto_analyze_info.items():
            if auto_analyze_time == 0 and manual_analyze_info.get(table_name, 0) == 0:
                self.detail['update_statistics'][table_name] = "Table statistics not updated"
            else:
                not_auto_analyze_time = (self.slow_sql_instance.start_at - auto_analyze_time)
                not_manual_analyze_time = (self.slow_sql_instance.start_at - manual_analyze_info.get(table_name, 0))
                if min(not_auto_analyze_time, not_manual_analyze_time) > monitoring.get_threshold(
                        'update_statistics_threshold') * 1000:
                    not_update_statistic_time = min(not_manual_analyze_time, not_auto_analyze_time)
                    self.detail['update_statistics'][table_name] = "%ss" % (not_update_statistic_time / 1000)
        if self.detail['update_statistics']:
            return True
        return False

    @property
    def abnormal_network_status(self) -> bool:
        node_network_transmit_drop = self.network_info.transmit_drop
        node_network_receive_drop = self.network_info.receive_drop
        node_network_transmit_error = self.network_info.transmit_error
        node_network_receive_error = self.network_info.receive_error
        node_network_transmit_packets = self.network_info.transmit_packets
        node_network_receive_packets = self.network_info.receive_packets
        if node_network_receive_drop / node_network_receive_packets > monitoring.get_param('package_drop_rate_threshold'):
            self.detail['system_cause']['package_receive_drop_rate'] = round(
                node_network_receive_drop / node_network_receive_packets, 4)
        if node_network_transmit_drop / node_network_transmit_packets > monitoring.get_param('package_drop_rate_threshold'):
            self.detail['system_cause']['package_transmit_drop_rate'] = round(
                node_network_transmit_drop / node_network_transmit_packets, 4)
        if node_network_receive_error / node_network_receive_packets > monitoring.get_param('package_error_rate_threshold'):
            self.detail['system_cause']['package_receive_error_rate'] = round(
                node_network_receive_error / node_network_receive_packets, 4)
        if node_network_transmit_error / node_network_transmit_packets > monitoring.get_param('package_error_rate_threshold'):
            self.detail['system_cause']['package_transmit_error_rate'] = round(
                node_network_transmit_error / node_network_transmit_packets, 4)
        if self.detail['system_cause'].get('package_receive_drop_rate', 0.0) or \
                self.detail['system_cause'].get('package_transmit_drop_rate', 0.0) or \
                self.detail['system_cause'].get('package_receive_error_rate', 0.0) or \
                self.detail['system_cause'].get('package_transmit_error_rate', 0.0):
            return True
        return False

    @property
    def abnormal_write_buffers(self) -> bool:
        checkpoint_buffers = self.pg_bgwriter_info.buffers_checkpoint
        writer_process_buffers = self.pg_bgwriter_info.buffers_clean
        backend_write_buffers = self.pg_bgwriter_info.buffers_backend
        if checkpoint_buffers + writer_process_buffers > 0:
            if checkpoint_buffers + writer_process_buffers > 0 and \
                    backend_write_buffers / (checkpoint_buffers + writer_process_buffers) > monitoring.get_param(
                    'bgwriter_rate_threshold'):
                self.detail['bgwriter_rate'] = round(
                    backend_write_buffers / (checkpoint_buffers + writer_process_buffers), 4)
                return True
        return False

    @property
    def abnormal_replication(self) -> bool:
        self.detail['replication'] = {}
        for pg_replication in self.replication_info:
            application_name = pg_replication.application_name
            replication_write_diff = pg_replication.pg_replication_write_diff
            replication_sent_diff = pg_replication.pg_replication_sent_diff
            replication_replay_diff = pg_replication.pg_replication_replay_diff
            if replication_sent_diff > monitoring.get_param('replication_sent_diff_threshold'):
                self.detail['replication']['sent_diff'] = f"The sent diff between primary and " \
                                                          f"standby({application_name}) replication " \
                                                          f"is too large: {replication_sent_diff}"
            if replication_write_diff > monitoring.get_param('replication_write_diff_threshold'):
                self.detail['replication']['write_diff'] = f"The write diff between primary and " \
                                                           f"standby({application_name}) replication " \
                                                           f"is too large: {replication_write_diff}"
            if replication_replay_diff > monitoring.get_param('replication_replay_diff_threshold'):
                self.detail['replication']['replay_diff'] = f"The replay diff between primary and " \
                                                            f"standby({application_name}) replication " \
                                                            f"is too large: {replication_replay_diff}"
        if self.detail['replication'].get('sent_diff') or \
                self.detail['replication'].get('write_diff') or \
                self.detail['replication'].get('replay_diff'):
            return True
        return False

    @property
    def abnormal_seqscan_operator(self) -> bool:
        """Notice: We can not get the exact schema information of the table by using the execution plan.
           Hence, there would be a scenario of mistakenly matching.
        """
        info = {}
        if self.recommend_index_info:
            self.suggestion['seqscan'] = '{%s}' % str(self.recommend_index_info)
            self.detail['seqscan'] = 'missing required index'
            return True
        if self.plan_parse_info is not None and self.plan_parse_info.root_node.total_cost > 0:
            seqscan_info = self.plan_parse_info.find_operators('Seq Scan', accurate=True)
            plan_total_cost = self.plan_parse_info.root_node.total_cost
            abnormal_seqscan_info = [item for item in seqscan_info if
                                     item.total_cost / plan_total_cost > monitoring.get_threshold('cost_rate_threshold')]
            for node in abnormal_seqscan_info:
                for table_info in self.table_structure:
                    if table_info.table_name == node.table:
                        index_cond = _search_in_existing_indexes(table_info.index, node)
                        if not index_cond:
                            if node.columns and node.rows / (table_info.live_tuples + 0.1) < \
                                    monitoring.get_threshold('used_index_tuples_rate_threshold'):
                                info[node.name] = \
                                    f"{node.table} may lack necessary index on " \
                                    f"{','.join(node.columns)}."
                            else:
                                info[node.name] = f"{node.table} may " \
                                                          f"need necessary large scan"
                        else:
                            # in some condition the index may become invalid: 'like %xxx' '==NULL' 'field*10>100' ...
                            info[node.name] = f"The field row may have created an index, " \
                                                      f"but it does not work for some reason, index detail: " \
                                                      f"{str(index_cond)}"
                        break
                if node.name not in info:
                    info[node.name] = "missing required information, please manual analysis"
        if info:
            self.detail['seqscan'] = 'NULL'
            self.suggestion['seqscan'] = info
            return True

        return False

    @property
    def abnormal_nestloop_operator(self) -> bool:
        if self.plan_parse_info is None:
            return False
        has_typical_scence = False
        nestloop_info = self.plan_parse_info.find_operators('Nested Loop')
        plan_total_cost = self.plan_parse_info.root_node.total_cost
        if plan_total_cost <= 0:
            return False
        abnormal_nestloop_info = [item for item in nestloop_info if
                                  item.total_cost / plan_total_cost > monitoring.get_threshold('cost_rate_threshold')]
        for node in abnormal_nestloop_info:
            child1, child2 = node.children
            if child1.rows > monitoring.get_threshold(
                    'nestloop_rows_threshold') and child2.rows > monitoring.get_threshold('nestloop_rows_threshold'):
                has_typical_scence = True
                break
        if has_typical_scence:
            self.detail['nestloop'] = 'nestloop is not suitable for join operations on large tables'
            self.suggestion['nestloop'] = 'nestloop is suitable for the record set of the driving table is relatively small ' \
                                          '(<10000) and the inner table needs to have an efficient access method, try hashjoin instead'
        if not has_typical_scence and abnormal_nestloop_info:
            self.detail['nestloop'] = 'the cost of nestloop is too large'
            self.suggestion['nestloop'] = 'try hashjoin or other operator to replace nestloop'
        if self.detail.get('nestloop'):
            return True
        return False

    @property
    def abnormal_hashjoin_operator(self) -> bool:
        if self.plan_parse_info is None:
            return False
        has_typical_scence = False
        hashjoin_info = self.plan_parse_info.find_operators('Hash Join')
        plan_total_cost = self.plan_parse_info.root_node.total_cost
        if plan_total_cost <= 0:
            return False
        abnormal_hashjoin_info = [item for item in hashjoin_info if
                                  item.total_cost / plan_total_cost > monitoring.get_threshold('cost_rate_threshold')]
        for node in abnormal_hashjoin_info:
            child1, child2 = node.children
            if child1.rows < monitoring.get_threshold(
                    'hashjoin_rows_threshold') or child2.rows < monitoring.get_threshold(
                'hashjoin_rows_threshold'):
                has_typical_scence = True
                break
        if has_typical_scence:
            self.detail['hashjoin'] = 'hashjoin is suitable for join operations on large table'
        if not has_typical_scence and abnormal_hashjoin_info:
            self.detail['hashjoin'] = 'the cost of hashjoin is too large'
        if self.detail.get('hashjoin'): 
            self.suggestion['hashjoin'] = 'try nestloop or other operator to replace hashjoin'
            return True
        return False

    @property
    def abnormal_groupagg_operator(self) -> bool:
        if self.plan_parse_info is None:
            return False
        has_typical_scence = False
        groupagg_info = self.plan_parse_info.find_operators('GroupAggregate')
        plan_total_cost = self.plan_parse_info.root_node.total_cost
        if plan_total_cost <= 0:
            return False
        abnormal_groupagg_info = [item for item in groupagg_info if
                                  item.total_cost / plan_total_cost > monitoring.get_threshold('cost_rate_threshold')]
        for node in abnormal_groupagg_info:
            if node.rows > monitoring.get_threshold('groupagg_rows_threshold'):
                has_typical_scence = True
                break
        if has_typical_scence:        
            self.detail['groupagg'] = 'groupagg is not suitable for large result sets'
        if not has_typical_scence and abnormal_groupagg_info: 
            self.detail['groupagg'] = 'the cost of groupagg is too large'
        if self.detail.get('groupagg'):        
            self.suggestion['groupagg'] = 'try hashagg or other operator to replace groupagg'
            return True
        return False

    @property
    def rewrite_sql(self):
        if self.rewritten_sql_info:
            self.suggestion['rewritten_sql'] = self.rewritten_sql_info
            return True
        return False

    @property
    def timed_task(self):
        if not self.pg_setting_info:
            return False
        if self.pg_setting_info['job_queue_processes'] == 0:
            return False
        timed_task_list = self.timed_task_info()
        abnormal_timed_task = []
        for timed_task in timed_task_list:
            if max(timed_task.last_start_date, self.slow_sql_instance.start_at) < \
                    min(timed_task.last_end_date,
                        self.slow_sql_instance.start_at + self.slow_sql_instance.duration_time):
                abnormal_timed_task.append("job_id(%s), priv_user(%s), job_status(%s)" % (timed_task.job_id,
                                                                                          timed_task.priv_user,
                                                                                          timed_task.job_status))
        if abnormal_timed_task:
            self.detail['timed_task'] = ';'.join(abnormal_timed_task)
            return True
        return False

    @property
    def abnormal_plan_time(self):
        if self.slow_sql_instance.db_time != 0 and \
                self.slow_sql_instance.plan_time / self.slow_sql_instance.db_time > \
                monitoring.get_threshold('plan_time_occupy_rate_threshold'):
            return True
        return False

    def __call__(self):
        self.detail['system_cause'] = {}
        self.detail['plan'] = {}
        features = [self.query_block,
                    self.large_dead_tuples,
                    self.large_fetch_tuples,
                    self.lower_hit_ratio,
                    self.update_redundant_index,
                    self.insert_redundant_index,
                    self.delete_redundant_index,
                    self.large_updated_tuples,
                    self.large_inserted_tuples,
                    self.large_deleted_tuples,
                    self.index_number_insert,
                    self.external_sort,
                    self.vacuum_operation,
                    self.analyze_operation,
                    self.tps_significant_change,
                    self.abnormal_io_condition,
                    self.large_iowait,
                    self.large_load_average,
                    self.large_cpu_usage,
                    self.large_process_fds,
                    self.large_mem_usage,
                    self.large_disk_usage,
                    self.abnormal_network_status,
                    self.abnormal_thread_pool,
                    self.large_connection_occupy_rate,
                    self.abnormal_wait_event,
                    self.lack_of_statistics,
                    self.abnormal_write_buffers,
                    self.abnormal_replication,
                    self.abnormal_seqscan_operator,
                    self.abnormal_nestloop_operator,
                    self.abnormal_hashjoin_operator,
                    self.abnormal_groupagg_operator,
                    self.rewrite_sql,
                    self.timed_task,
                    self.abnormal_plan_time]
        features = [int(item) for item in features]
        return features, self.detail, self.suggestion
