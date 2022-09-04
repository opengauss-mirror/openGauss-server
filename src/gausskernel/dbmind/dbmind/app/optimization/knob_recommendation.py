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
import logging

from dbmind.common.types import Sequence
from dbmind.components.xtuner.tuner.character import AbstractMetric
from dbmind.components.xtuner.tuner.recommend import recommend_knobs as rk
from dbmind.components.xtuner.tuner.utils import cached_property
from dbmind.service import dai
from dbmind.service.utils import SequenceUtils


def try_to_get_latest_metric_value(metric_name, **conditions):
    seqs = dai.get_latest_metric_value(metric_name).filter(**conditions).fetchall()
    if len(seqs) > 0:
        return seqs
    # If we can't get latest sequences, we expand retrieve interval.
    max_attempts = 3
    starting_attempt_minutes = 5  # minutes
    i = 0
    while len(seqs) == 0 and i < max_attempts:
        seqs = dai.get_latest_metric_sequence(
            metric_name, minutes=starting_attempt_minutes * (2 ** i)  # double every time
        ).filter(**conditions).fetchall()

    if len(seqs) == 0:
        return []

    # aggregation
    rv = []
    for seq in seqs:
        avg = sum(seq.values) / len(seq)
        new_seq = Sequence(
            timestamps=(seq.timestamps[-1],),
            values=(avg,),
            name=seq.name,
            step=seq.step,
            labels=seq.labels
        )
        rv.append(new_seq)
    return rv


def get_database_hosts():
    database_hosts = set()
    seqs = try_to_get_latest_metric_value("pg_uptime")
    for seq in seqs:
        host = SequenceUtils.from_server(seq).split(":")[0]
        database_hosts.add(host)
    return database_hosts


def recommend_knobs():
    metric = TSDBMetric()
    hosts = get_database_hosts()
    result = dict()
    for host in hosts:
        try:
            metric.set_host(host)
            knobs = rk("recommend", metric)
            result[host] = [knobs, metric.to_dict()]
        except Exception as e:
            logging.warning(
                'Cannot recommend knobs for the host %s maybe because of information lack.' % host, exc_info=e
            )
    return result


class TSDBMetric(AbstractMetric):

    def __init__(self):
        self.database_host = None
        AbstractMetric.__init__(self)

    def __getitem__(self, item):
        """Get GUC from database instance."""
        value = self.fetch_current_guc_value(item)
        try:
            return float(value)
        except ValueError:
            return value

    @cached_property
    def most_xact_db(self):
        seqs = try_to_get_latest_metric_value("pg_db_xact_commit")
        database = 'postgres'
        max_xact_commit = -float("inf")
        for seq in seqs:
            host = SequenceUtils.from_server(seq).split(":")[0]
            if self.database_host != host:
                continue
            xact_commit = seq.values[0]
            if xact_commit > max_xact_commit:
                database = seq.labels.get("datname")
                max_xact_commit = xact_commit

        return database

    def set_host(self, database_host):
        self.database_host = database_host

    def get_one_value_from_seqs_according_to_database_host(self, seqs, default_val=None):
        val = default_val
        for seq in seqs:
            host = SequenceUtils.from_server(seq).split(":")[0]
            val = seq.values[0]
            if self.database_host == host:
                return val

        return val

    def fetch_current_guc_value(self, guc_name, default_val=-1):
        seqs = try_to_get_latest_metric_value("pg_settings_setting", name=guc_name)
        return self.get_one_value_from_seqs_according_to_database_host(seqs, default_val=default_val)

    def fetch_metric_value_on_most_xact_database(self, metric_name, default_val=None):
        seqs = try_to_get_latest_metric_value(metric_name, datname=self.most_xact_db)
        return self.get_one_value_from_seqs_according_to_database_host(seqs, default_val=default_val)

    def fetch_metric_value(self, metric_name, default_val=None):
        seqs = try_to_get_latest_metric_value(metric_name)
        return self.get_one_value_from_seqs_according_to_database_host(seqs, default_val=default_val)

    def fetch_the_sum_of_metric_values(self, metric_name):
        rv = list()
        seqs = try_to_get_latest_metric_value(metric_name)
        for seq in seqs:
            host = SequenceUtils.from_server(seq).split(":")[0]
            if self.database_host == host:
                rv.append(seq.values[0])
        return sum(rv)

    @property
    def cache_hit_rate(self):
        # You could define used internal state here.
        # this is a demo, cache_hit_rate, we will use it while tuning shared_buffer.
        pg_db_blks_hit = self.fetch_metric_value_on_most_xact_database("pg_db_blks_hit")
        pg_db_blks_read = self.fetch_metric_value_on_most_xact_database("pg_db_blks_read")
        cache_hit_rate = pg_db_blks_hit / (pg_db_blks_hit + pg_db_blks_read + 0.001)

        return cache_hit_rate

    @cached_property
    def is_64bit(self):
        seqs = try_to_get_latest_metric_value("node_uname_info", machine="x86_64")
        return self.get_one_value_from_seqs_according_to_database_host(seqs) is not None

    @property
    def uptime(self):
        return self.fetch_metric_value("pg_uptime")

    @property
    def current_connections(self):
        return self.fetch_metric_value_on_most_xact_database("pg_activity_count")

    @property
    def average_connection_age(self):
        return self.fetch_metric_value("pg_avg_time")  # unit: second

    @property
    def all_database_size(self):
        return self.fetch_the_sum_of_metric_values("pg_database_size_bytes")  # unit: kB

    @property
    def current_prepared_xacts_count(self):
        return self.fetch_metric_value("pg_prepared_xacts_count")

    @property
    def current_locks_count(self):
        return self.fetch_metric_value_on_most_xact_database("pg_lock_count")

    @property
    def checkpoint_proactive_triggering_ratio(self):
        return self.fetch_metric_value("pg_stat_bgwriter_checkpoint_proactive_triggering_ratio")

    @property
    def checkpoint_avg_sync_time(self):
        return self.fetch_metric_value("pg_stat_bgwriter_checkpoint_avg_sync_time")

    @property
    def shared_buffer_heap_hit_rate(self):
        return self.fetch_metric_value("pg_statio_all_tables_shared_buffer_heap_hit_rate")

    @property
    def shared_buffer_toast_hit_rate(self):
        return self.fetch_metric_value("pg_statio_all_tables_shared_buffer_toast_hit_rate")

    @property
    def shared_buffer_tidx_hit_rate(self):
        return self.fetch_metric_value("pg_statio_all_tables_shared_buffer_tidx_hit_rate")

    @property
    def shared_buffer_idx_hit_rate(self):
        return self.fetch_metric_value("pg_statio_all_tables_shared_buffer_idx_hit_rate")

    @property
    def temp_file_size(self):
        return self.fetch_metric_value("pg_stat_database_temp_file_size")  # unit: kB

    @property
    def read_write_ratio(self):
        tup_returned = self.fetch_metric_value_on_most_xact_database("pg_db_tup_returned")
        tup_inserted = self.fetch_metric_value_on_most_xact_database("pg_db_tup_inserted")
        tup_updated = self.fetch_metric_value_on_most_xact_database("pg_db_tup_updated")
        tup_deleted = self.fetch_metric_value_on_most_xact_database("pg_db_tup_deleted")
        res = tup_returned / (tup_inserted + tup_updated + tup_deleted + 0.001)

        return res

    @property
    def search_modify_ratio(self):
        tup_returned = self.fetch_metric_value_on_most_xact_database("pg_db_tup_returned")
        tup_inserted = self.fetch_metric_value_on_most_xact_database("pg_db_tup_inserted")
        tup_updated = self.fetch_metric_value_on_most_xact_database("pg_db_tup_updated")
        tup_deleted = self.fetch_metric_value_on_most_xact_database("pg_db_tup_deleted")
        res = (tup_returned + tup_inserted) / (tup_updated + tup_deleted + 0.01)

        return res

    @property
    def fetched_returned_ratio(self):
        tup_returned = self.fetch_metric_value_on_most_xact_database("pg_db_tup_returned")
        tup_fetched = self.fetch_metric_value_on_most_xact_database("pg_db_tup_fetched")
        res = tup_fetched / (tup_returned + 0.01)

        return res

    @property
    def rollback_commit_ratio(self):
        xact_commit = self.fetch_metric_value_on_most_xact_database("pg_db_xact_commit")
        xact_rollback = self.fetch_metric_value_on_most_xact_database("pg_db_xact_rollback")
        res = xact_rollback / (xact_commit + 0.01)

        return res

    @cached_property
    def os_cpu_count(self):
        cores = self.fetch_metric_value("os_cpu_processor_number")
        return int(cores)

    @property
    def current_free_mem(self):
        return self.fetch_metric_value("node_memory_MemFree_bytes")  # unit: kB

    @cached_property
    def os_mem_total(self):
        return self.fetch_metric_value("node_memory_MemTotal_bytes")  # unit: kB

    @cached_property
    def dirty_background_bytes(self):
        return self.fetch_metric_value("node_memory_Dirty_bytes")

    @cached_property
    def block_size(self):
        return self["block_size"]

    @property
    def load_average(self):
        load1 = self.fetch_metric_value("node_load1")  # unit: kB
        load5 = self.fetch_metric_value("node_load5")  # unit: kB
        load15 = self.fetch_metric_value("node_load15")  # unit: kB
        if load1:
            load1 = load1 / self.os_cpu_count
        if load5:
            load5 = load5 / self.os_cpu_count
        if load15:
            load15 = load15 / self.os_cpu_count

        return load1, load5, load15

    @cached_property
    def nb_gaussdb(self):
        rv = list()
        seqs = try_to_get_latest_metric_value("gaussdb_qps_by_instance")
        for seq in seqs:
            host = SequenceUtils.from_server(seq).split(":")[0]
            if self.database_host == host:
                rv.append(seq.values[0])
        return len(rv)

    @cached_property
    def is_hdd(self):
        return False
