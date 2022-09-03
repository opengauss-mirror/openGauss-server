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
from dbmind.common.tsdb import TsdbClientFactory
from dbmind.common.types import Alarm, ALARM_TYPES, ALARM_LEVEL, SlowQuery
from dbmind.common.types import RootCause
from dbmind.common.types import Sequence
from dbmind.metadatabase import create_metadatabase_schema
from dbmind.service import dai
from dbmind.service.utils import DISTINGUISHING_INSTANCE_LABEL
from dbmind.service.utils import SequenceUtils

create_metadatabase_schema()
golden_kpi = ('os_cpu_usage', 'os_mem_usage_rate',
              'gaussdb_qps_by_instance', 'gaussdb_dynamic_used_memory')


def test_range_metrics(mock_dai):
    minutes = 10
    dai.get_latest_metric_value('prometheus_remote_storage_highest_timestamp_in_seconds').fetchone()
    for metric in golden_kpi:
        results = dai.get_latest_metric_sequence(metric, minutes).fetchall()
        for sequence in results:
            assert sequence.name in golden_kpi
            assert sequence.length > 0
            host = SequenceUtils.from_server(sequence)
            assert host is not None and host != ''


def test_tsdb():
    for metric in golden_kpi:
        results = TsdbClientFactory.get_tsdb_client().get_current_metric_value(
            metric_name=metric
        )

        for sequence in results:
            assert isinstance(sequence, Sequence)

            from_instance = SequenceUtils.from_server(sequence)

            inner_results = TsdbClientFactory.get_tsdb_client().get_metric_range_data(
                metric_name=metric,
                label_config={DISTINGUISHING_INSTANCE_LABEL: from_instance},
                params={'step': '30s'}
            )
            for inner_result in inner_results:
                assert inner_result.name == metric
                assert len(inner_result) > 0


def test_save_xxx():
    host = '127.0.0.1'
    metric_name = 'test_metric'

    sequence = Sequence(tuple(range(0, 100)), tuple(range(100, 200)))
    dai.save_forecast_sequence(host, metric_name, sequence)

    future_alarm = Alarm(
        host=host,
        alarm_content="disk occupied will exceed percentage threshold",
        alarm_type=ALARM_TYPES.SYSTEM,
        metric_name=metric_name,
        alarm_level=ALARM_LEVEL.ERROR,
        alarm_cause=RootCause.get('DISK_WILL_SPILL')
    )
    dai.save_future_alarms([future_alarm, future_alarm, future_alarm])

    history_alarm = Alarm(
        host=host,
        metric_name=metric_name,
        alarm_content='found anomaly on %s' % metric_name,
        alarm_type=ALARM_TYPES.SYSTEM,
        alarm_level=ALARM_LEVEL.ERROR,
        alarm_cause=RootCause.get('WORKING_IO_CONTENTION')
    )
    history_alarm.set_timestamp(100, 101)
    dai.save_history_alarms([history_alarm, history_alarm, history_alarm])

    slow_query = SlowQuery(
        db_host='127.0.0.1',
        db_port=1234,
        schema_name='test_schema',
        db_name='test_db',
        query='select sleep(100);',
        start_timestamp=1000,
        duration_time=2,
        hit_rate=0.90,
        fetch_rate=1000,
        cpu_time=100,
        data_io_time=100
    )
    slow_query.add_cause(RootCause.get('LOCK_CONTENTION'))
    dai.save_slow_queries([slow_query, slow_query, slow_query])


def test_estimate_appropriate_step():
    dai.estimate_appropriate_step()
