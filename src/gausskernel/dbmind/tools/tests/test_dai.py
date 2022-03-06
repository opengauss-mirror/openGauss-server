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
import configparser

from dbmind import global_vars
from dbmind.common.tsdb import TsdbClientFactory
from dbmind.common.types import RootCause
from dbmind.common.types import Sequence
from dbmind.common.types import SlowQuery
from dbmind.metadatabase import create_metadatabase_schema
from dbmind.service import dai
from dbmind.service.dai import SequenceUtils, DISTINGUISHING_INSTANCE_LABEL

configs = configparser.ConfigParser()
configs.add_section('TSDB')
configs.set('TSDB', 'name', 'prometheus')
configs.set('TSDB', 'host', '10.90.56.172')  # TODO: CHANGE or IGNORE
configs.set('TSDB', 'port', '9090')
configs.add_section('METADATABASE')
configs.set('METADATABASE', 'dbtype', 'sqlite')
configs.set('METADATABASE', 'host', '')
configs.set('METADATABASE', 'port', '')
configs.set('METADATABASE', 'username', '')
configs.set('METADATABASE', 'password', '')
configs.set('METADATABASE', 'database', 'test_metadatabase.db')
global_vars.configs = configs
global_vars.must_filter_labels = {}
golden_kpi = ('os_cpu_usage', 'os_mem_usage',
              'gaussdb_qps_by_instance', 'gaussdb_dynamic_used_memory')

create_metadatabase_schema()


def test_range_metrics():
    minutes = 10
    dai.get_latest_metric_sequence('pg_boot_time', minutes).fetchall()
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

    slow_query = SlowQuery(
        db_host='10.90.5.172',
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

