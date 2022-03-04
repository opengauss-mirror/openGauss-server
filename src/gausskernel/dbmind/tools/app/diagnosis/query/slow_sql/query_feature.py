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
from dbmind import global_vars
from dbmind.common.parser.sql_parsing import sql_processing
from dbmind.common.types.misc import SlowQuery
from ..slow_sql.query_info_source import QueryContext
from ..slow_sql import significance_detection


def _get_threshold(name: str) -> [float, int]:
    return global_vars.dynamic_configs.get('slow_sql_threshold', name)


class QueryFeature:
    """
    Feature processing factory
    """
    def __init__(self, slow_sql_instance: SlowQuery, data_factory: QueryContext = None):
        """
        :param slow_sql_instance: The instance of slow query
        :param data_factory: The factory of slow query data processing factory
        self.table_structure: data structure to save table structure
        self.lock_info: data structure to save lock information of slow query
        self.database_info: data structure to save lock information of database info such as QPS, CONNECTION, etc
        self.system_info: data structure to save system information
        self.detail: data structure to save diagnosis information
        """
        self.slow_sql_instance = slow_sql_instance
        self.data_factory = data_factory
        self.table_structure = None
        self.lock_info = None
        self.database_info = None
        self.system_info = None
        self.detail = {}

    def initialize_metrics(self):
        """Initialize the data structure such as database_info, table_structure, lock_info, etc"""
        self.database_info = self.data_factory.acquire_database_info()
        self.table_structure = self.data_factory.acquire_tables_structure_info()
        self.lock_info = self.data_factory.acquire_lock_info()
        self.system_info = self.data_factory.acquire_system_info()

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
    def other_type(self) -> bool:
        """Determine whether it is a other statement, return True if it not start with select. delete, insert, update"""
        if not self.select_type and not self.insert_type and not self.update_type and not self.delete_type:
            return True
        return False

    @property
    def query_block(self) -> bool:
        """Determine whether the query is blocked during execution"""
        if not any((self.slow_sql_instance.lock_wait_count, self.slow_sql_instance.lwlock_wait_count)):
            return False
        for index, query_info in enumerate(self.lock_info.locked_query):
            if sql_processing(self.slow_sql_instance.query) == sql_processing(query_info):
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
        tuples_info = {f"{item.schema_name}:{item.table_name}": [item.live_tuples, item.dead_tuples] for item in
                       self.table_structure}
        self.detail['large_table'] = {}
        for table_name, tuples_number_list in tuples_info.items():
            if sum(tuples_number_list) > _get_threshold('tuple_number_limit'):
                self.detail['large_table'][table_name] = tuples_number_list
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
        for table_name, dead_rate in dead_rate_info.items():
            if dead_rate > _get_threshold('dead_rate_limit'):
                self.detail['dead_rate'][table_name] = dead_rate
        if self.detail.get('dead_rate'):
            return True
        return False

    @property
    def large_fetch_tuples(self) -> bool:
        """Determine whether the query related table has too many fetch tuples"""
        fetched_tuples = self.slow_sql_instance.n_tuples_fetched
        returned_tuples = self.slow_sql_instance.n_tuples_returned
        if not self.table_structure or max([item.live_tuples for item in self.table_structure]) == 0:
            if fetched_tuples + returned_tuples > _get_threshold('fetch_tuples_limit'):
                self.detail['fetched_tuples'] = fetched_tuples + returned_tuples
                self.detail['fetched_tuples_rate'] = 'UNKNOWN'
                return True
            return False
        live_tuples_list = {f"{item.schema_name}:{item.table_name}": item.live_tuples for item in
                            self.table_structure}
        if (fetched_tuples + returned_tuples) / max(live_tuples_list.values()) > _get_threshold('fetch_rate_limit'):
            self.detail['fetched_tuples'] = fetched_tuples + returned_tuples
            self.detail['fetched_tuples_rate'] = round(
                (fetched_tuples + returned_tuples) / max(live_tuples_list.values()), 4)
            return True
        else:
            return False

    @property
    def large_returned_rows(self) -> bool:
        """Determine whether the query related table has too many returned tuples"""
        returned_rows = self.slow_sql_instance.n_returned_rows
        if not self.table_structure or max([item.live_tuples for item in self.table_structure]) == 0:
            if returned_rows > _get_threshold('returned_rows_limit'):
                self.detail['returned_rows'] = returned_rows
                self.detail['returned_rows_rate'] = 'UNKNOWN'
                return True
            return False
        live_tuples_list = {f"{item.schema_name}:{item.table_name}": item.live_tuples for item in
                            self.table_structure}
        if returned_rows / max(live_tuples_list.values()) > _get_threshold(
                'returned_rate_limit'):
            self.detail['returned_rows'] = returned_rows
            self.detail['returned_rows_rate'] = round(returned_rows / max(live_tuples_list.values()), 4)
            return True
        else:
            return False

    @property
    def lower_hit_ratio(self) -> bool:
        """Determine whether the query related table has lower hit ratio"""
        self.detail['hit_rate'] = self.slow_sql_instance.hit_rate
        if not self.large_table or self.insert_type:
            return False
        if self.slow_sql_instance.hit_rate < _get_threshold('hit_rate_limit'):
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
        for table_name, redundant_index_list in redundant_index_info.items():
            if redundant_index_list:
                self.detail['redundant_index'][table_name] = redundant_index_list
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
        updated_tuples = self.slow_sql_instance.n_tuples_updated
        if not self.table_structure or max([item.live_tuples for item in self.table_structure]) == 0:
            if updated_tuples > _get_threshold('updated_tuples_limit'):
                self.detail['updated_tuples'] = updated_tuples
                self.detail['updated_tuples_rate'] = 'UNKNOWN'
                return True
            return False
        live_tuples_list = {f"{item.schema_name}:{item.table_name}": item.live_tuples for item in
                            self.table_structure}
        if updated_tuples / max(live_tuples_list.values()) > _get_threshold(
                'updated_rate_limit'):
            self.detail['updated_tuples'] = updated_tuples
            self.detail['updated_tuples_rate'] = round(updated_tuples / max(live_tuples_list.values()), 4)
            return True
        else:
            return False

    @property
    def large_inserted_tuples(self) -> bool:
        """Determine whether the query related table has large insert tuples"""
        inserted_tuples = self.slow_sql_instance.n_tuples_inserted
        if not self.table_structure or max([item.live_tuples for item in self.table_structure]) == 0:
            if inserted_tuples > _get_threshold('inserted_tuples_limit'):
                self.detail['inserted_tuples'] = inserted_tuples
                self.detail['inserted_tuples_rate'] = 'UNKNOWN'
                return True
            return False
        live_tuples_list = {f"{item.schema_name}:{item.table_name}": item.live_tuples for item in
                            self.table_structure}
        if inserted_tuples / max(live_tuples_list.values()) > _get_threshold(
                'inserted_rate_limit'):
            self.detail['inserted_tuples'] = inserted_tuples
            self.detail['inserted_tuples_rate'] = round(inserted_tuples / max(live_tuples_list.values()), 4)
            return True
        else:
            return False

    @property
    def large_index_number(self) -> bool:
        """Determine whether the query related table has too many index"""
        if not self.table_structure:
            return False
        index_info = {f"{item.schema_name}:{item.table_name}": item.index for item in
                      self.table_structure}
        self.detail['index'] = {}
        for table_name, index_list in index_info.items():
            if len(index_list) > _get_threshold('index_number_limit'):
                self.detail['index'][table_name] = index_list
        if self.detail.get('index'):
            return True
        return False

    @property
    def index_number_insert(self) -> bool:
        """Determine whether the insert query related table has too many index"""
        return self.insert_type and self.large_index_number

    @property
    def large_deleted_tuples(self) -> bool:
        """Determine whether the query related table has too many delete tuples"""
        deleted_tuples = self.slow_sql_instance.n_tuples_deleted
        if not self.table_structure or max([item.live_tuples for item in self.table_structure]) == 0:
            if deleted_tuples > _get_threshold('deleted_tuples_limit'):
                self.detail['deleted_tuples'] = deleted_tuples
                self.detail['deleted_tuples_rate'] = 'UNKNOWN'
                return True
            return False
        live_tuples_list = {f"{item.schema_name}:{item.table_name}": item.live_tuples for item in
                            self.table_structure}
        if deleted_tuples / max(live_tuples_list.values()) > _get_threshold(
                'deleted_rate_limit'):
            self.detail['deleted_tuples'] = deleted_tuples
            self.detail['deleted_tuples_rate'] = round(deleted_tuples / max(live_tuples_list.values()), 4)
            return True
        else:
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
        auto_vacuum_info = {f"{item.schema_name}:{item.table_name}": item.last_autovacuum for item in
                            self.table_structure}
        user_vacuum_info = {f"{item.schema_name}:{item.table_name}": item.vacuum for item in
                            self.table_structure}
        self.detail['autovacuum'] = {}
        for table_name, autovacuum_time in auto_vacuum_info.items():
            if self.slow_sql_instance.start_at <= autovacuum_time <= self.slow_sql_instance.start_at + \
                    self.slow_sql_instance.duration_time:
                self.detail['autovacuum'][table_name] = autovacuum_time
        for table_name, vacuum_time in user_vacuum_info.items():
            if self.slow_sql_instance.start_at <= vacuum_time <= self.slow_sql_instance.start_at + \
                    self.slow_sql_instance.duration_time:
                self.detail['autovacuum'][table_name] = vacuum_time
        if self.detail.get('autovacuum'):
            return True
        return False

    @property
    def analyze_operation(self) -> bool:
        """Determine whether the query related table has analyze operation"""
        if not self.table_structure or not self.large_table:
            return False
        auto_analyze_info = {f"{item.schema_name}:{item.table_name}": item.last_autoanalyze for item in
                             self.table_structure}
        user_analyze_info = {f"{item.schema_name}:{item.table_name}": item.analyze for item in
                             self.table_structure}
        self.detail['autoanalyze'] = {}
        for table_name, autoanalyze_time in auto_analyze_info.items():
            if self.slow_sql_instance.start_at <= autoanalyze_time <= self.slow_sql_instance.start_at + \
                    self.slow_sql_instance.duration_time:
                self.detail['autoanalyze'][table_name] = autoanalyze_time
        for table_name, analyze_time in user_analyze_info.items():
            if self.slow_sql_instance.start_at <= analyze_time <= self.slow_sql_instance.start_at + \
                    self.slow_sql_instance.duration_time:
                self.detail['autoanalyze'][table_name] = analyze_time
        if self.detail.get('autoanalyze'):
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
            if max(cur_database_tps) > _get_threshold('tps_limit'):
                self.detail['tps'] = round(max(cur_database_tps), 4)
                return True
            else:
                return False
        elif his_database_tps and not cur_database_tps:
            return False
        else:
            if significance_detection.detect(cur_database_tps, his_database_tps) and max(
                    cur_database_tps) > _get_threshold('tps_limit'):
                self.detail['tps'] = round(max(cur_database_tps), 4)
                return True
            return False

    @property
    def large_iops(self) -> bool:
        """Determine whether the QPS of the related table has large IOPS"""
        if self.system_info.iops > _get_threshold('iops_limit'):
            self.detail['system_cause']['iops'] = self.system_info.iops
            return True
        return False

    @property
    def large_iowait(self) -> bool:
        """Determine whether the QPS of the related table has large IOWAIT"""
        if self.system_info.iowait > _get_threshold('iowait_limit'):
            self.detail['system_cause']['iowait'] = self.system_info.iowait
            return True
        return False

    @property
    def large_ioutils(self) -> bool:
        """Determine whether the QPS of the related table has large IOUTILS"""
        ioutils_dict = {}
        for device, ioutils in self.system_info.ioutils.items():
            if ioutils > _get_threshold('ioutils_limit'):
                ioutils_dict[device] = ioutils
        if ioutils_dict:
            self.detail['system_cause']['ioutils'] = ioutils_dict
            return True
        return False

    @property
    def large_iocapacity(self) -> bool:
        """Determine whether the QPS of the related table has large IOCAPACITY"""
        if self.system_info.iocapacity > _get_threshold('iocapacity_limit'):
            self.detail['system_cause']['iocapacity'] = self.system_info.iocapacity
            return True
        return False

    @property
    def large_cpu_usage(self) -> bool:
        """Determine whether the QPS of the related table has large CPU USAGE"""
        if self.system_info.cpu_usage > _get_threshold('cpu_usage_limit'):
            self.detail['system_cause']['cpu_usage'] = self.system_info.cpu_usage
            return True
        return False

    @property
    def large_load_average(self) -> bool:
        """Determine whether the QPS of the related table has large LOAD AVERAGE"""
        if self.system_info.load_average > _get_threshold('load_average_rate_limit'):
            self.detail['system_cause']['load_average'] = self.system_info.load_average
            return True
        return False

    def __call__(self):
        self.detail['system_cause'] = {}
        features = [self.query_block,
                    self.large_dead_tuples,
                    self.large_fetch_tuples,
                    self.large_returned_rows,
                    self.lower_hit_ratio,
                    self.update_redundant_index,
                    self.insert_redundant_index,
                    self.delete_redundant_index,
                    self.large_updated_tuples,
                    self.large_inserted_tuples,
                    self.index_number_insert,
                    self.large_deleted_tuples,
                    self.external_sort,
                    self.vacuum_operation,
                    self.analyze_operation,
                    self.tps_significant_change,
                    self.large_iowait,
                    self.large_iops,
                    self.large_load_average,
                    self.large_cpu_usage,
                    self.large_ioutils,
                    self.large_iocapacity]
        features = [int(item) for item in features]
        return features, self.detail
