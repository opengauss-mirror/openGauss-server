"""
Copyright (c) 2020 Huawei Technologies Co.,Ltd.

openGauss is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:

         http://license.coscl.org.cn/MulanPSL2

THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
"""

from .utils import cached_property


class WORKLOAD_TYPE:
    TYPES = ['ap', 'tp', 'htap']

    AP = TYPES[0]
    TP = TYPES[1]
    HTAP = TYPES[2]


class OpenGaussMetric:
    def __init__(self, db):
        """
        Obtain the feature information of the database instance for generating reports and recommending knobs.
        :param db: Use db_agent to interact with the database.
        """
        self._db = db
        self._scenario = None

    def __getitem__(self, item):
        """Get GUC from database instance."""
        value = self._db.get_knob_value(item)
        try:
            return float(value)
        except ValueError:
            return value

    def _get_numeric_metric(self, sql):
        result = self._db.exec_statement(sql)

        if len(result) > 0:
            return float(result[0][0])
        else:
            return 0

    def set_scenario(self, scenario):
        if scenario not in WORKLOAD_TYPE.TYPES:
            raise ValueError('The scenario parameter must be one of %s.' % WORKLOAD_TYPE.TYPES)

        self._scenario = scenario

    @property
    def used_mem(self):
        # We make total used memory as regular.
        # main mem: max_connections * (work_mem + temp_buffers) + shared_buffers + wal_buffers
        sql = "select " \
              "setting " \
              "from pg_settings " \
              "where name in ('max_connections', 'work_mem', 'temp_buffers', 'shared_buffers', 'wal_buffers') " \
              "order by name;"
        res = self._db.exec_statement(sql)
        values = map(lambda x: int(x[0]), res)
        max_conn, s_buff, t_buff, w_buff, work_mem = values
        total_mem = max_conn * (work_mem / 64 + t_buff / 128) + s_buff / 64 + w_buff / 4096  # unit: MB
        return total_mem * 1024  # unit: kB

    @property
    def cache_hit_rate(self):
        # You could define used internal state here.
        # this is a demo, cache_hit_rate, we will use it while tuning shared_buffer.
        cache_hit_rate_sql = "select blks_hit / (blks_read + blks_hit + 0.001) " \
                             "from pg_stat_database " \
                             "where datname = '{}';".format(self._db.db_name)
        return self._get_numeric_metric(cache_hit_rate_sql)

    @property
    def uptime(self):
        return self._get_numeric_metric(
            "select extract(epoch from now()-pg_postmaster_start_time()) / 60 / 60;")  # unit: hour

    @property
    def current_connections(self):
        return self._get_numeric_metric(
            "select count(1) from pg_stat_activity where client_port is not null;")

    @property
    def average_connection_age(self):
        return self._get_numeric_metric("select extract(epoch from avg(now()-backend_start)) as age "
                                        "from pg_stat_activity where client_port is not null;")  # unit: second

    @property
    def all_database_size(self):
        return self._get_numeric_metric(
            "select sum(pg_database_size(datname)) / 1024 from pg_database;")  # unit: kB

    @property
    def max_processes(self):
        return int(self["max_connections"]) + int(self["autovacuum_max_workers"])

    @property
    def track_activity_size(self):
        return int(self["track_activity_query_size"]) / 1024 * self.max_processes  # unit kB

    @property
    def current_prepared_xacts_count(self):
        return self._get_numeric_metric("select count(1) from pg_prepared_xacts;")

    @property
    def current_locks_count(self):
        return self._get_numeric_metric(
            "select count(1) from pg_locks where transactionid in (select transaction from pg_prepared_xacts)")

    @property
    def checkpoint_dirty_writing_time_window(self):
        return int(self["checkpoint_timeout"]) * float(self["checkpoint_completion_target"])  # unit: second

    @property
    def checkpoint_proactive_triggering_ratio(self):
        return self._get_numeric_metric(
            "select checkpoints_req / (checkpoints_timed + checkpoints_req) from pg_stat_bgwriter;"
        )

    @property
    def checkpoint_avg_sync_time(self):
        return self._get_numeric_metric(
            "select checkpoint_sync_time / (checkpoints_timed + checkpoints_req) from pg_stat_bgwriter;"
        )

    @property
    def shared_buffer_heap_hit_rate(self):
        return self._get_numeric_metric(
            "select sum(heap_blks_hit)*100/(sum(heap_blks_read)+sum(heap_blks_hit)+1) from pg_statio_all_tables ;")

    @property
    def shared_buffer_toast_hit_rate(self):
        return self._get_numeric_metric(
            "select sum(toast_blks_hit)*100/(sum(toast_blks_read)+sum(toast_blks_hit)+1) from pg_statio_all_tables ;"
        )

    @property
    def shared_buffer_tidx_hit_rate(self):
        return self._get_numeric_metric(
            "select sum(tidx_blks_hit)*100/(sum(tidx_blks_read)+sum(tidx_blks_hit)+1) from pg_statio_all_tables ;"
        )

    @property
    def shared_buffer_idx_hit_rate(self):
        return self._get_numeric_metric(
            "select sum(idx_blks_hit)*100/(sum(idx_blks_read)+sum(idx_blks_hit)+1) from pg_statio_all_tables ;"
        )

    @property
    def temp_file_size(self):
        return self._get_numeric_metric(
            "select max(temp_bytes / temp_files) / 1024 from pg_stat_database where temp_files > 0;"
        )  # unit: kB

    @property
    def read_write_ratio(self):
        return self._get_numeric_metric(
            "select tup_returned / (tup_inserted + tup_updated + tup_deleted + 0.001) "
            "from pg_stat_database where datname = '%s';" % self._db.db_name
        )

    @property
    def search_modify_ratio(self):
        return self._get_numeric_metric(
            "select (tup_returned + tup_inserted) / (tup_updated + tup_deleted + 0.01) "
            "from pg_stat_database where datname = '%s';" % self._db.db_name
        )

    @property
    def fetched_returned_ratio(self):
        return self._get_numeric_metric(
            "select tup_fetched / (tup_returned + 0.01) "
            "from pg_stat_database where datname = '%s';" % self._db.db_name
        )

    @property
    def rollback_commit_ratio(self):
        return self._get_numeric_metric(
            "select xact_rollback / (xact_commit + 0.01) "
            "from pg_stat_database where datname = '%s';" % self._db.db_name
        )

    @property
    def read_tup_speed(self):
        return self._get_numeric_metric(
            "select tup_returned / (extract (epoch from (now() - stats_reset))) "
            "from pg_stat_database where datname = '%s';" % self._db.db_name
        )

    @property
    def write_tup_speed(self):
        return self._get_numeric_metric(
            "select (tup_inserted + tup_updated + tup_deleted) / (extract (epoch from (now() - stats_reset))) "
            "from pg_stat_database where datname = '%s';" % self._db.db_name
        )

    @cached_property
    def nb_gaussdb(self):
        return int(self._db.exec_command_on_host("ps -ux | grep gaussd[b] | wc -l"))

    @cached_property
    def os_mem_total(self):
        mem = self._db.exec_command_on_host("free -k | awk 'NR==2{print $2}'")  # unit kB
        return int(mem)

    @cached_property
    def min_free_mem(self):
        kbytes = self._db.exec_command_on_host("cat /proc/sys/vm/min_free_kbytes")
        return int(kbytes)  # unit: kB

    @cached_property
    def os_cpu_count(self):
        cores = self._db.exec_command_on_host("lscpu | grep 'CPU(s)' | head -1 | awk '{print $2}'")
        return int(cores)

    @cached_property
    def is_hdd(self):
        mount_point = self._db.exec_command_on_host(
            "df %s | awk '{print $6}' | awk 'NR==2{print}'" % self._db.data_path)
        return self._db.exec_command_on_host(
            "lsblk -o rota,mountpoint | grep %s | awk '{print $1}'" % mount_point) == '1'  # 1 means hdd, 0 means ssd.

    @cached_property
    def is_64bit(self):
        return self._db.exec_command_on_host("uname -i").find('64') > 0

    @property
    def current_free_mem(self):
        return int(self._db.exec_command_on_host("free -k | awk 'NR==2{print $4 + $6}'"))  # unit: kB

    @cached_property
    def dirty_background_bytes(self):
        return int(self._db.exec_command_on_host("cat /proc/sys/vm/dirty_background_bytes"))

    @cached_property
    def block_size(self):
        return self._get_numeric_metric(
            "select setting / 1024 from pg_settings where name = 'block_size';"
        )  # unit: kB

    @property
    def load_average(self):
        result = self._db.exec_command_on_host(
            "cat /proc/loadavg"
        )
        if result:
            return list(map(lambda v: float(v) / self.os_cpu_count,
                            result.strip().split(" ")[:3]))
        else:
            return "Cannot read /proc/loadavg."

    @cached_property
    def ap_index(self):
        """
        The larger the index value, the more obvious the characteristics of the AP scenario.

        The following rules were defined based on collected data
        and DBA experience because of insufficient sample data.
        In the future, we'll use machine learning models instead.

        :return: Float type. The AP index ranges from 0 to 10.
        """
        fetch_return_ratio = self.fetched_returned_ratio
        rollback_commit_ratio = self.rollback_commit_ratio
        search_modify_ratio = self.search_modify_ratio
        read_write_ratio = self.read_write_ratio

        # We use the voting method to calculate the final index value.
        index = 0

        # A smaller fetch_return_ratio value indicates an AP scenario.
        if fetch_return_ratio <= 0.01:
            index += 2
        elif fetch_return_ratio <= 0.1:
            index += 1
        elif fetch_return_ratio <= 0.5:
            index += 0.5

        # A smaller rollback_commit_ratio value indicates an AP scenario.
        if rollback_commit_ratio <= 0.01:
            index += 2.5
        elif rollback_commit_ratio <= 0.02:
            index += 2
        elif rollback_commit_ratio <= 0.03:
            index += 1.5
        elif rollback_commit_ratio <= 0.04:
            index += 1
        elif rollback_commit_ratio <= 0.05:
            index += 0.5

        # A larger search_modify_ratio value indicates an AP scenario.
        # e.g The search_modify_ratio value of TPC-C is often smaller than 100.
        if search_modify_ratio >= 1e5:
            index += 2
        elif search_modify_ratio >= 1e4:
            index += 1.5
        elif search_modify_ratio >= 1e3:
            index += 1
        else:
            index += search_modify_ratio / 100

        # A larger read_write_ratio value indicates an AP scenario.
        if read_write_ratio >= 1e5:
            index += 1.5
        elif read_write_ratio >= 1e4:
            index += 1
        elif read_write_ratio >= 1e2:
            index += 0.5

        # Only large-scale materialization operations can cause temporary files.
        # This operation is a typical feature of AP scenarios,
        # but temporary files are not always generated.
        # Temporary files are only generated because of insufficient work_mem.
        if self.temp_file_size > 0:
            index += 1

        # TP scenario jobs often achieve high buffer hit ratios.
        if self.cache_hit_rate < 0.9:
            index += 0.5

        # In the TP scenario, the proportion of active checkpoints is higher.
        if self.checkpoint_proactive_triggering_ratio <= 0.1:
            index += 0.5
        elif self.checkpoint_proactive_triggering_ratio <= 0.3:
            index += 0.2

        return index

    @cached_property
    def workload_type(self):
        if self._scenario is not None:
            return self._scenario

        if self.ap_index > 6:
            return WORKLOAD_TYPE.AP
        elif self.ap_index <= 3:
            return WORKLOAD_TYPE.TP
        else:
            return WORKLOAD_TYPE.HTAP

    @cached_property
    def enable_autovacuum(self):
        setting = self._db.exec_statement(
            "select setting from pg_settings where name = 'autovacuum';"
        )[0][0]
        return setting == 'on'

    def get_internal_state(self):
        return [self.cache_hit_rate, self.load_average[0]]

    def reset(self):
        self._db.exec_statement("SELECT pg_stat_reset();")

    def to_dict(self):
        rv = dict()
        for name in dir(self):
            if name.startswith('_'):  # Avoid reading private variables.
                continue

            value = getattr(self, name)
            if type(value) in (list, int, float, bool, str):
                rv[name] = value

        return rv
