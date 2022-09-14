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
from itertools import product

from dbmind import global_vars
from dbmind.service.utils import SequenceUtils
from dbmind.common.utils import cast_to_int_or_float
from .generic_detection import detect as generic_detect
from .specific_detection import _rules_for_history, _rule_mapper
from .specific_detection import detect as specific_detect


class MUST_BE_DETECTED_METRICS:

    @staticmethod
    def future():
        return _rule_mapper.keys() - _rules_for_history

    HISTORY = _rules_for_history
    BUILTIN_GOLDEN_KPI = {'os_cpu_usage', 'os_disk_iops', 'os_mem_usage',
                          'pg_connections_used_conn', 'statement_responsetime_percentile_p80',
                          'node_network_transmit_error'}


def detect_future(host, metrics, latest_sequences, future_sequences):
    return specific_detect(
        host, metrics, latest_sequences, future_sequences
    )


def detect_history(sequences):
    metrics = tuple(sequence.name for sequence in sequences)
    host = SequenceUtils.from_server(sequences[0])

    alarms = []
    if metrics in MUST_BE_DETECTED_METRICS.HISTORY:
        alarms.extend(
            specific_detect(host, metrics, sequences)
        )

    # prevent cross-reference
    from ..diagnosis import diagnose_for_sequences
    for sequence in sequences:
        if sequence in MUST_BE_DETECTED_METRICS.BUILTIN_GOLDEN_KPI:
            anomalies = generic_detect(sequence.name, sequence)
            alarms.extend(diagnose_for_sequences(host, sequence.name, anomalies))
    return alarms


def group_sequences_together(sequences_list, metrics):
    n = len(sequences_list)
    if n == 1:
        return [(sequence,) for sequence in sequences_list[0]]

    mode = 'full'
    for i, sequences in enumerate(sequences_list):
        if not sequences:
            logging.warning('At least one list of sequences of %s is empty while grouping.', metrics[i])
            return []

        if ':' not in SequenceUtils.from_server(sequences[0]):
            mode = 'host'
            break

    rules = set()
    for sequences in sequences_list:
        for sequence in sequences:
            if mode == 'full':
                rules.add(SequenceUtils.from_server(sequence))
            elif mode == 'host':
                rules.add(SequenceUtils.from_server(sequence).split(':')[0])

    res = []
    for rule in rules:
        cache = [[] for _ in range(n)]
        for i, sequences in enumerate(sequences_list):
            for sequence in sequences:
                if rule in sequence.labels['from_instance']:
                    cache[i].append(sequence)
        res.extend(product(*cache))

    if not res:
        logging.warning('No sequences matched by distance when grouping metrics %s.', str(metrics))

    return res


def get_param(name: str):
    value = global_vars.dynamic_configs.get('detection_params', name)
    if value is None:
        from dbmind.metadatabase.schema import config_detection_params
        value = config_detection_params.DetectionParams.__default__.get(name)
        logging.warning(
            'Cannot get the detection parameter %s. '
            'DBMind used a default value %s as an alternative. '
            'Please check and update the dynamic configuration.',
            name, value
        )
    return cast_to_int_or_float(value)


def get_threshold(name: str) -> [float, int]:
    value = global_vars.dynamic_configs.get('slow_sql_threshold', name)
    if value is None:
        from dbmind.metadatabase.schema import config_slow_sql_threshold
        value = config_slow_sql_threshold.SlowSQLThreshold.__default__.get(name)
        logging.warning(
            'Cannot get the slow query parameter %s. '
            'DBMind used a default value %s as an alternative. '
            'Please check and update the dynamic configuration.',
            name, value
        )
    return cast_to_int_or_float(value)
