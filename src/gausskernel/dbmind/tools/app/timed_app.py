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
from datetime import timedelta, datetime

from dbmind import constants
from dbmind import global_vars
from dbmind.app.diagnosis import diagnose_query
from dbmind.common.algorithm.forecasting import quickly_forecast
from dbmind.common.dispatcher import timer
from dbmind.service import dai
from dbmind.common import utils

metric_value_range_map = utils.read_simple_config_file(constants.METRIC_VALUE_RANGE_CONFIG)

detection_interval = global_vars.configs.getint(
    'SELF-MONITORING', 'detection_interval'
)

last_detection_minutes = global_vars.configs.getint(
    'SELF-MONITORING', 'last_detection_time'
) / 60

how_long_to_forecast_minutes = global_vars.configs.getint(
    'SELF-MONITORING', 'forecasting_future_time'
) / 60

"""The Four Golden Signals:
https://sre.google/sre-book/monitoring-distributed-systems/#xref_monitoring_golden-signals
"""
golden_kpi = list(map(
    str.strip,
    global_vars.configs.get(
        'SELF-MONITORING', 'golden_kpi'
    ).split(',')
))

@timer(detection_interval)
def self_monitoring():
    # diagnose for slow queries
    if constants.SLOW_QUERY_DIAGNOSIS_NAME in global_vars.backend_timed_task:
        slow_query_collection = dai.get_all_slow_queries(last_detection_minutes)
        logging.debug('The length of slow_query_collection is %d.', len(slow_query_collection))
        dai.save_slow_queries(
            global_vars.worker.parallel_execute(
                diagnose_query, ((slow_query,) for slow_query in slow_query_collection)
            )
        )


@timer(how_long_to_forecast_minutes * 60)
def forecast_kpi():
    if constants.FORECAST_NAME not in global_vars.backend_timed_task:
        return

    # The general training length is at least three times the forecasting length.
    expansion_factor = 5
    enough_history_minutes = how_long_to_forecast_minutes * expansion_factor
    if enough_history_minutes <= 0:
        logging.error(
            'The value of enough_history_minutes less than or equal to 0 '
            'and DBMind has ignored it.'
        )
        return

    start = datetime.now() - timedelta(minutes=enough_history_minutes)
    end = datetime.now()
    for metric in golden_kpi:
        last_sequences = dai.get_metric_sequence(metric, start, end).fetchall()

        try:
            metric_value_range = global_vars.metric_value_range_map.get(metric)
            lower, upper = map(float, metric_value_range.split(','))
        except Exception:
            lower, upper = 0, float("inf")

        future_sequences = global_vars.worker.parallel_execute(
            quickly_forecast, ((sequence, how_long_to_forecast_minutes, lower, upper)
                                       for sequence in last_sequences)
        )
        detect_materials = list()
        for last_sequence, future_sequence in zip(last_sequences, future_sequences):
            host = dai.SequenceUtils.from_server(last_sequence)
            detect_materials.append((host, metric, future_sequence))
            # Save the forecast future KPIs for users browsing.
            dai.save_forecast_sequence(metric, host, future_sequence)

