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
"""Data Access Interface (DAI):

    - Wrap all data fetching operations from different sources;
    - The module is the main entry for all data;
    - The data obtained from here ensures that the format is clean and uniform;
    - The data has been preprocessed here.
"""
import logging
from abc import abstractmethod
from datetime import timedelta, datetime
from typing import List

from dbmind import global_vars
from dbmind.common.tsdb import TsdbClientFactory
from dbmind.common.types import Sequence
from dbmind.common.types.misc import SlowQuery
from dbmind.metadatabase import dao


# Singleton pattern with starving formula
# will capture exception in the main thread.
TsdbClientFactory.get_tsdb_client()

# Notice: 'DISTINGUISHING_INSTANCE_LABEL' is a magic string, i.e., our own name.
# Thus, not all collection agents (such as Prometheus's openGauss-exporter)
# distinguish different instance addresses through this one.
# Actually, this is a risky action for us, currently.
DISTINGUISHING_INSTANCE_LABEL = 'from_instance'


class _AbstractLazyFetcher:
    def __init__(self):
        # The default filter should contain some labels (or tags)
        # from user's config. Otherwise, there will be lots of data stream
        # fetching from the remote time series database, whereas, we don't need them.
        self.labels = dict.copy(global_vars.must_filter_labels or {})
        self.rv = None

    def filter(self, **kwargs):
        self.labels.update(kwargs)
        return self

    def from_server(self, host):
        self.labels[DISTINGUISHING_INSTANCE_LABEL] = host
        return self

    def fetchall(self):
        self.rv = self._real_fetching_action()
        return self.rv

    def fetchone(self):
        self.rv = self.rv or self._real_fetching_action()
        # If iterator has un-popped elements then return it,
        # otherwise return empty of the sequence.
        try:
            return self.rv.pop(0)
        except IndexError:
            return Sequence()

    @abstractmethod
    def _real_fetching_action(self) -> List[Sequence]:
        """Abstract interface which the sub-class only need
        implement it.
        """


class SequenceUtils:
    @staticmethod
    def from_server(s: Sequence):
        return s.labels.get(DISTINGUISHING_INSTANCE_LABEL)


# TODO: add reverse mapper.
def _map_metric(metric_name, to_internal_name=True):
    """Use metric_map.conf to map given metric_name
    so as to adapt to the different metric names from different collectors.
    """
    if global_vars.metric_map is None:
        logging.warning(
            'Cannot map the given metric since global_vars.metric_map is NoneType.'
        )
        return metric_name
    return global_vars.metric_map.get(metric_name, metric_name).strip()


def get_metric_type(metric_name):
    """Dummy"""
    return None


def get_metric_sequence(metric_name, start_time, end_time):
    """Get monitoring sequence from time-series database between
    start_time and end_time"""

    # TODO: step
    # step = auto_estimate_step(...)
    class _Abstract_LazyFetcherImpl(_AbstractLazyFetcher):
        def _real_fetching_action(self) -> List[Sequence]:
            return TsdbClientFactory.get_tsdb_client() \
                .get_metric_range_data(metric_name=_map_metric(metric_name),
                                       label_config=self.labels,
                                       start_time=start_time,
                                       end_time=end_time)

    return _Abstract_LazyFetcherImpl()


def get_latest_metric_sequence(metric_name, minutes):
    """Get the monitoring sequence from time-series database in
     the last #2 minutes."""
    # TODO: the datetime is not always the same as server's, so we
    #  should impl a sync mechanism.
    start_time = datetime.now() - timedelta(minutes=minutes)
    end_time = datetime.now()
    return get_metric_sequence(metric_name, start_time, end_time)


def get_latest_metric_value(metric_name):
    class _Abstract_LazyFetcherImpl(_AbstractLazyFetcher):
        def _real_fetching_action(self) -> List[Sequence]:
            return TsdbClientFactory.get_tsdb_client() \
                .get_current_metric_value(
                metric_name=metric_name,
                label_config=self.labels
            )

    return _Abstract_LazyFetcherImpl()


def save_forecast_sequence(metric_name, host, sequence):
    dao.forecasting_metrics.batch_insert_forecasting_metric(
        metric_name, host, sequence.values, sequence.timestamps,
        metric_type=get_metric_type(metric_name),
        node_id=None
    )


def save_slow_queries(slow_queries):
    for slow_query in slow_queries:
        dao.slow_queries.insert_slow_query(
            schema_name=slow_query.schema_name,
            db_name=slow_query.db_name,
            query=slow_query.query,
            start_at=slow_query.start_at,
            duration_time=slow_query.duration_time,
            hit_rate=slow_query.hit_rate, fetch_rate=slow_query.fetch_rate,
            cpu_time=slow_query.cpu_time, data_io_time=slow_query.data_io_time,
            root_cause=slow_query.root_causes, suggestion=slow_query.suggestions,
            template_id=slow_query.template_id
        )


def get_all_slow_queries(minutes):
    slow_queries = []
    sequences = get_latest_metric_sequence('pg_sql_statement_history_exc_time', minutes).fetchall()
    for sequence in sequences:
        from_instance = SequenceUtils.from_server(sequence)
        db_host, db_port = from_instance.split(':')
        db_name = sequence.labels['datname']
        schema_name = sequence.labels['schema'].split(',')[-1] \
            if ',' in sequence.labels['schema'] else sequence.labels['schema']
        query = sequence.labels['query']
        start_timestamp = int(sequence.labels['start_time'])
        duration_time = int(sequence.labels['finish_time']) - int(sequence.labels['start_time'])
        hit_rate = round(float(sequence.labels['hit_rate']), 4)
        fetch_rate = round(float(sequence.labels['fetch_rate']), 4)
        cpu_time = round(float(sequence.labels['cpu_time']), 4)
        data_io_time = round(float(sequence.labels['data_io_time']), 4)
        sort_count = round(float(sequence.labels['sort_count']), 4)
        sort_spill_count = round(float(sequence.labels['sort_spill_count']), 4)
        sort_mem_used = round(float((sequence.labels['sort_mem_used'])), 4)
        hash_count = round(float(sequence.labels['hash_count']), 4)
        hash_spill_count = round(float((sequence.labels['hash_spill_count'])), 4)
        hash_mem_used = round(float(sequence.labels['hash_mem_used']), 4)
        template_id = sequence.labels['unique_query_id']
        lock_wait_count = int(sequence.labels['lock_wait_count'])
        lwlock_wait_count = int(sequence.labels['lwlock_wait_count'])
        n_returned_rows = int(sequence.labels['n_returned_rows'])
        n_tuples_returned = int(sequence.labels['n_tuples_returned'])
        n_tuples_fetched = int(sequence.labels['n_tuples_fetched'])
        n_tuples_inserted = int(sequence.labels['n_tuples_inserted'])
        n_tuples_updated = int(sequence.labels['n_tuples_updated'])
        n_tuples_deleted = int(sequence.labels['n_tuples_deleted'])
        slow_sql_info = SlowQuery(
            db_host=db_host, db_port=db_port,
            schema_name=schema_name, db_name=db_name, query=query,
            start_timestamp=start_timestamp, duration_time=duration_time,
            hit_rate=hit_rate, fetch_rate=fetch_rate,
            cpu_time=cpu_time, data_io_time=data_io_time,
            sort_count=sort_count, sort_spill_count=sort_spill_count,
            sort_mem_used=sort_mem_used, hash_count=hash_count,
            hash_spill_count=hash_spill_count, hash_mem_used=hash_mem_used,
            template_id=template_id, lock_wait_count=lock_wait_count,
            lwlock_wait_count=lwlock_wait_count, n_returned_rows=n_returned_rows,
            n_tuples_returned=n_tuples_returned, n_tuples_fetched=n_tuples_fetched,
            n_tuples_inserted=n_tuples_inserted, n_tuples_updated=n_tuples_updated,
            n_tuples_deleted=n_tuples_deleted
        )

        slow_queries.append(slow_sql_info)
    return slow_queries
