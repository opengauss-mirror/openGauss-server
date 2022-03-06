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
import random
import os
from datetime import datetime, timedelta
from unittest.mock import MagicMock

from dbmind import global_vars
from dbmind.service.dai import _AbstractLazyFetcher
from dbmind.cmd.edbmind import get_worker_instance
from dbmind.cmd.config_utils import DynamicConfig


prom_addr = os.environ.get('PROMETHEUS_ADDR', 'hostname:9090')
prom_host, prom_port = prom_addr.split(':')
configs = configparser.ConfigParser()
configs.add_section('TSDB')
configs.set('TSDB', 'name', 'prometheus')
configs.set('TSDB', 'host', prom_host)
configs.set('TSDB', 'port', prom_port)
configs.add_section('SELF-MONITORING')
configs.set(
    'SELF-MONITORING', 'detection_interval', '600'
)
configs.set(
    'SELF-MONITORING', 'last_detection_time', '600'
)
configs.set(
    'SELF-MONITORING', 'forecasting_future_time', '86400'
)
configs.set(
    'SELF-MONITORING', 'golden_kpi', 'os_cpu_usage, os_mem_usage'
)
configs.set(
    'SELF-MONITORING', 'result_storage_retention', '600'
)
global_vars.configs = configs
global_vars.dynamic_configs = DynamicConfig
global_vars.worker = get_worker_instance('local', 2)

from dbmind.common.types import Sequence
from dbmind.service import dai
from dbmind.app.timed_app import self_monitoring, forecast_kpi


def faked_get_metric_sequence(metric_name, start_time, end_time):
    step = 5
    random_scope = 0.5
    timestamps = list(range(int(start_time.timestamp()), int(end_time.timestamp()) + step, step))
    values = timestamps.copy()
    # set random values
    for _ in range(0, min(len(timestamps) // 5, 5)):
        idx = random.randint(0, len(timestamps) - 1)
        current_value = values[idx]
        values[idx] = random.randint(
            int(current_value * (1 - random_scope)),
            int(current_value * (1 + random_scope))
        )

    class MockFetcher(_AbstractLazyFetcher):
        def _real_fetching_action(self):
            hosts = ('192.168.1.100:1234', '192.168.1.101:5678', '192.168.1.102:1111')
            rv = list()
            for host in hosts:
                rv.append(
                    Sequence(timestamps=timestamps, values=values, name=metric_name, labels={'from_instance': host})
                )
            return rv

    return MockFetcher()


def faked_get_latest_metric_sequence(metric_name, minutes):
    end_time = datetime.now()
    start_time = end_time - timedelta(minutes=minutes)
    return faked_get_metric_sequence(metric_name, start_time, end_time)


def test_self_monitoring():
    if prom_host == 'hostname':
        # Use faked data source since not found PROMETHEUS_ADDR environment variable.
        dai.get_metric_sequence = faked_get_metric_sequence
        dai.get_latest_metric_sequence = faked_get_latest_metric_sequence
        dai.get_all_slow_queries = MagicMock(
            return_value=()
        )

    dai.get_all_last_monitoring_alarm_logs = MagicMock(
        return_value=()
    )

    dai.save_history_alarm = print
    dai.save_slow_queries = print
    self_monitoring()


def test_forecast_kpi():
    dai.get_metric_sequence = faked_get_metric_sequence
    dai.save_forecast_sequence = print
    dai.save_future_alarm = print
    forecast_kpi()


if __name__ == '__main__':
    test_self_monitoring()
    test_forecast_kpi()

