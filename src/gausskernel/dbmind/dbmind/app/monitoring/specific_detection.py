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
from typing import List

from dbmind.common.algorithm import anomaly_detection
from dbmind.common.types import Alarm, ALARM_LEVEL, ALARM_TYPES
from dbmind.common.types import RootCause
from dbmind.common.types.sequence import Sequence, EMPTY_SEQUENCE
from dbmind.common.utils import dbmind_assert
import dbmind.app.monitoring
from .generic_detection import AnomalyDetections


_rule_mapper = {}
_rules_for_history = set()


def _check_for_metric(metrics, only_history=True):
    def decorator(func):
        if only_history:
            _rules_for_history.add(metrics)
        _rule_mapper[metrics] = func

    return decorator


def _dummy(*args, **kwargs):
    return []


def detect(
        host, metrics,
        latest_sequences, future_sequences=tuple()
) -> List[Alarm]:
    func = _rule_mapper.get(metrics, _dummy)

    if not future_sequences:
        for latest_sequence in latest_sequences:
            future_sequences += (EMPTY_SEQUENCE,)
            metric = latest_sequence.name
            logging.warning('Forecast future sequences %s at %s is None.', metric, host)

    alarms = func(latest_sequences, future_sequences)
    for alarm in alarms:
        alarm.host = host
    return alarms


"""Add checking rules below."""


@_check_for_metric(('os_disk_usage',), only_history=True)
def will_disk_spill(latest_sequences, future_sequences):
    latest_sequence, future_sequence = latest_sequences[0], future_sequences[0]
    full_sequence = latest_sequence + future_sequence
    disk_usage_threshold = dbmind.app.monitoring.get_param('disk_usage_threshold')
    disk_usage_max_coef = dbmind.app.monitoring.get_param('disk_usage_max_coef')
    disk_device = latest_sequence.labels.get('device', 'unknown')
    disk_mountpoint = latest_sequence.labels.get('mountpoint', 'unknown')

    over_threshold_anomalies = AnomalyDetections.do_threshold_detect(
        full_sequence,
        high=disk_usage_threshold
    )

    rapid_increase_anomalies = AnomalyDetections.do_gradient_detect(
        full_sequence,
        side='positive',
        max_coef=disk_usage_max_coef,
        timed_window=300000  # 300000 ms
    )

    alarms = []
    if True in over_threshold_anomalies.values:
        alarms.append(
            Alarm(
                alarm_content='The disk usage has exceeded the warning level: %s%%(device: %s, mountpoint: %s).' % (
                    disk_usage_threshold * 100,
                    disk_device,
                    disk_mountpoint
                ),
                alarm_type=ALARM_TYPES.ALARM,
                metric_name='os_disk_usage',
                alarm_level=ALARM_LEVEL.WARNING,
                alarm_cause=RootCause.get('DISK_WILL_SPILL')
            )
        )
        # If above alarm raises, we can return directly
        # because the level of subsequent alarm is lower.
        return alarms

    if True in rapid_increase_anomalies.values:
        alarm = Alarm(
            alarm_content="The disk usage increased too fast, and exceeded the warning level: %s"
                          "(device: %s, mountpoint: %s)." % (disk_usage_max_coef,
                                                             disk_device,
                                                             disk_mountpoint,
                                                             ),
            alarm_type=ALARM_TYPES.ALARM,
            metric_name='os_disk_usage',
            alarm_level=ALARM_LEVEL.WARNING,
            alarm_cause=RootCause.get('DISK_BURST_INCREASE')
        )
        alarm.set_timestamp(start=full_sequence.timestamps[0], end=full_sequence.timestamps[-1])
        alarms.append(alarm)
    return alarms


def _add_anomalies_values_2_msg(anomalies):
    anomaly_values = []
    for anomaly in anomalies:
        anomaly_values += list(anomaly.values)
    return " Abnormal value(s) are " + ",".join(str(item) for item in anomaly_values)


@_check_for_metric(('gaussdb_invalid_logins_rate',), only_history=True)
def has_login_brute_force_attack(latest_sequences, future_sequences):
    latest_sequence, future_sequence = latest_sequences[0], future_sequences[0]
    dbmind_assert(future_sequence or future_sequence == EMPTY_SEQUENCE)
    logging.debug("starting has_login_brute_force_attack")
    detector = anomaly_detection.SpikeDetector(side='positive')
    anomalies_found = detector.detect(latest_sequence)

    alarms = []
    if len(anomalies_found) > 0:
        alarm_content = "Find suspicious abnormal brute force logins(%s times). " % (len(anomalies_found))
        alarm_content += _add_anomalies_values_2_msg(anomalies_found)
        logging.info(alarm_content)
        alarm = \
            Alarm(
                alarm_content=alarm_content,
                alarm_type=ALARM_TYPES.SECURITY,
                metric_name='gaussdb_invalid_logins_rate',
                alarm_level=ALARM_LEVEL.WARNING,
                alarm_cause=RootCause.get('TOO_MANY_INVALID_LOGINS')
            )
        alarm.set_timestamp(start=latest_sequence.timestamps[0], end=latest_sequence.timestamps[-1])
        alarms.append(alarm)
    return alarms


@_check_for_metric(('gaussdb_errors_rate',), only_history=True)
def has_scanning_attack(latest_sequences, future_sequences):
    latest_sequence, future_sequence = latest_sequences[0], future_sequences[0]
    dbmind_assert(future_sequence or future_sequence == EMPTY_SEQUENCE)
    logging.debug("starting has_scanning_attack")
    detector = anomaly_detection.SpikeDetector(side='positive')
    anomalies_found = detector.detect(latest_sequence)

    alarms = []
    if len(anomalies_found) > 0:
        alarm_content = "Found anomalies for gaussdb_errors_rate(%s times)." \
                        " Operational issue may also be the cause of this alarm." % (len(anomalies_found))
        logging.info(alarm_content)
        alarm_content += _add_anomalies_values_2_msg(anomalies_found)
        alarm = Alarm(
            alarm_content=alarm_content,
            alarm_type=ALARM_TYPES.SECURITY,
            metric_name='gaussdb_errors_rate',
            alarm_level=ALARM_LEVEL.WARNING,
            alarm_cause=RootCause.get('TOO_MANY_ERRORS')
        )
        alarm.set_timestamp(start=latest_sequence.timestamps[0], end=latest_sequence.timestamps[-1])
        alarms.append(alarm)
    return alarms


def has_mem_leak(latest_sequences, future_sequences, metric_name=''):
    latest_sequence, future_sequence = latest_sequences[0], future_sequences[0]
    full_sequence = latest_sequence + future_sequence
    mem_usage_threshold = dbmind.app.monitoring.get_param('mem_usage_threshold')
    mem_usage_max_coef = dbmind.app.monitoring.get_param('mem_usage_max_coef')

    over_threshold_anomalies = AnomalyDetections.do_threshold_detect(
        full_sequence,
        high=mem_usage_threshold
    )

    rapid_increase_anomalies = AnomalyDetections.do_gradient_detect(
        full_sequence,
        side='positive',
        max_coef=mem_usage_max_coef,
        timed_window=300000  # 300000 ms
    )

    alarms = []
    if True in over_threshold_anomalies.values:
        alarm = Alarm(
            alarm_content="The memory usage has exceeded the warning level: %s%%." % (mem_usage_threshold * 100),
            alarm_type=ALARM_TYPES.ALARM,
            metric_name=metric_name,
            alarm_level=ALARM_LEVEL.WARNING,
            alarm_cause=RootCause.get('HIGH_MEMORY_USAGE')
        )
        alarm.set_timestamp(start=full_sequence.timestamps[0], end=full_sequence.timestamps[-1])
        alarms.append(alarm)
    if True in rapid_increase_anomalies.values:
        alarm = Alarm(
            alarm_content="The memory usage increased too fast, and exceeded the warning level: %s" % (
                mem_usage_max_coef),
            alarm_type=ALARM_TYPES.ALARM,
            metric_name=metric_name,
            alarm_level=ALARM_LEVEL.WARNING,
            alarm_cause=RootCause.get('MEMORY_USAGE_BURST_INCREASE')
        )
        alarm.set_timestamp(start=full_sequence.timestamps[0], end=full_sequence.timestamps[-1])
        alarms.append(alarm)
    return alarms


@_check_for_metric(('os_mem_usage',), only_history=True)
def os_has_mem_leak(latest_sequences, future_sequences):
    return has_mem_leak(latest_sequences, future_sequences, metric_name='os_mem_usage')


@_check_for_metric(('gaussdb_state_memory',), only_history=True)
def db_has_mem_leak(latest_sequences, future_sequences):
    return has_mem_leak(latest_sequences, future_sequences, metric_name='gaussdb_state_memory')


@_check_for_metric(('os_cpu_usage',), only_history=True)
def has_cpu_high_usage(latest_sequences, future_sequences):
    latest_sequence, future_sequence = latest_sequences[0], future_sequences[0]
    # Worry about unaligned risks.
    full_sequence = latest_sequence + future_sequence
    cpu_usage_threshold = dbmind.app.monitoring.get_param('cpu_usage_threshold')
    cpu_high_usage_percent = dbmind.app.monitoring.get_param('cpu_high_usage_percent')

    over_threshold_anomalies = AnomalyDetections.do_threshold_detect(
        full_sequence,
        high=cpu_usage_threshold
    )

    alarms = []
    if over_threshold_anomalies.values.count(True) > cpu_high_usage_percent * len(full_sequence):
        alarm = Alarm(
            alarm_content='The cpu usage has exceeded the warning level '
                          '%s%% of total for over %s%% of last detection period.' % (
                              cpu_usage_threshold * 100, cpu_high_usage_percent * 100),
            alarm_type=ALARM_TYPES.ALARM,
            metric_name='os_cpu_usage',
            alarm_level=ALARM_LEVEL.ERROR,
            alarm_cause=RootCause.get('HIGH_CPU_USAGE')
        )
        alarm.set_timestamp(start=full_sequence.timestamps[0], end=full_sequence.timestamps[-1])
        alarms.append(alarm)
    return alarms


@_check_for_metric(('gaussdb_qps_by_instance',), only_history=True)
def has_qps_rapid_change(latest_sequences, future_sequences):
    latest_sequence, future_sequence = latest_sequences[0], future_sequences[0]
    full_sequence = latest_sequence + future_sequence
    qps_max_coef = dbmind.app.monitoring.get_param('qps_max_coef')

    rapid_increase_anomalies = AnomalyDetections.do_gradient_detect(
        full_sequence,
        side='positive',
        max_coef=qps_max_coef,
        timed_window=300000  # 300000 ms
    )

    alarms = []
    if True in rapid_increase_anomalies:
        alarm = Alarm(
            alarm_content="The qps increased too fast, and exceeded the warning level: %s." % (qps_max_coef),
            alarm_type=ALARM_TYPES.ALARM,
            metric_name='gaussdb_qps_by_instance',
            alarm_level=ALARM_LEVEL.WARNING,
            alarm_cause=RootCause.get('QPS_VIOLENT_INCREASE')
        )
        alarm.set_timestamp(start=full_sequence.timestamps[0], end=full_sequence.timestamps[-1])
        alarms.append(alarm)
    return alarms


@_check_for_metric(('gaussdb_connections_used_ratio',), only_history=True)
def has_connections_high_occupation(latest_sequences, future_sequences):
    latest_sequence, future_sequence = latest_sequences[0], future_sequences[0]
    full_sequence = latest_sequence + future_sequence
    connection_usage_threshold = dbmind.app.monitoring.get_param('connection_usage_threshold')
    over_threshold_anomalies = AnomalyDetections.do_threshold_detect(
        full_sequence,
        high=connection_usage_threshold
    )

    alarms = []
    if True in over_threshold_anomalies.values:
        alarms.append(
            Alarm(
                alarm_content='The connection usage has exceeded the warning level: %s%%.' % (
                    connection_usage_threshold * 100
                ),
                alarm_type=ALARM_TYPES.ALARM,
                metric_name='used connection',
                alarm_level=ALARM_LEVEL.ERROR,
                alarm_cause=RootCause.get('FAST_CONNECTIONS_INCREASE')
            )
        )
    return alarms


@_check_for_metric(('statement_responsetime_percentile_p80',), only_history=True)
def has_high_p80(latest_sequences, future_sequences):
    latest_sequence, future_sequence = latest_sequences[0], future_sequences[0]
    full_sequence = latest_sequence + future_sequence
    p80_threshold = dbmind.app.monitoring.get_param('p80_threshold')

    over_threshold_anomalies = AnomalyDetections.do_threshold_detect(
        full_sequence,
        high=p80_threshold
    )

    alarms = []
    if True in over_threshold_anomalies.values:
        alarms.append(
            Alarm(
                alarm_content='The 80%% SQL response time of openGauss has exceeded the warning level: %sms.' % (
                    p80_threshold
                ),
                alarm_type=ALARM_TYPES.ALARM,
                metric_name='P80',
                alarm_level=ALARM_LEVEL.WARNING,
                alarm_cause=RootCause.get('POOR_SQL_PERFORMANCE')
            )
        )
    return alarms


@_check_for_metric(('pg_replication_replay_diff',), only_history=True)
def has_high_replication_delay(latest_sequences, future_sequences):
    latest_sequence, future_sequence = latest_sequences[0], future_sequences[0]
    full_sequence = latest_sequence + future_sequence
    replication_replay_diff_threshold = dbmind.app.monitoring.get_param('replication_replay_diff_threshold')

    over_threshold_anomalies = AnomalyDetections.do_threshold_detect(
        full_sequence,
        high=replication_replay_diff_threshold
    )

    alarms = []
    if True in over_threshold_anomalies.values:
        alarms.append(
            Alarm(
                alarm_content='The primary-standby synchronization delay has exceeded the warning level: %s.' % (
                    replication_replay_diff_threshold
                ),
                alarm_type=ALARM_TYPES.ALARM,
                metric_name='pg_replication_write_diff',
                alarm_level=ALARM_LEVEL.WARNING,
                alarm_cause=RootCause.get('REPLICATION_SYNC')
            )
        )
    return alarms


@_check_for_metric(('os_disk_iocapacity',), only_history=True)
def has_high_io_capacity(latest_sequences, future_sequences):
    latest_sequence, future_sequence = latest_sequences[0], future_sequences[0]
    full_sequence = latest_sequence + future_sequence
    io_capacity_threshold = dbmind.app.monitoring.get_param('io_capacity_threshold')

    over_threshold_anomalies = AnomalyDetections.do_threshold_detect(
        full_sequence,
        high=io_capacity_threshold
    )

    alarms = []
    if True in over_threshold_anomalies.values:
        alarms.append(
            Alarm(
                alarm_content='The IO_CAPACITY has exceeded the warning level: %s(MB).' % (
                    io_capacity_threshold
                ),
                alarm_type=ALARM_TYPES.ALARM,
                metric_name='io capacity',
                alarm_level=ALARM_LEVEL.WARNING,
                alarm_cause=RootCause.get('LARGE_IO_CAPACITY')
            )
        )
    return alarms


@_check_for_metric(('node_process_fds_rate',), only_history=True)
def has_high_handle_occupation(latest_sequences, future_sequences):
    latest_sequence, future_sequence = latest_sequences[0], future_sequences[0]
    full_sequence = latest_sequence + future_sequence
    handler_occupation_threshold = dbmind.app.monitoring.get_param('handler_occupation_threshold')

    over_threshold_anomalies = AnomalyDetections.do_threshold_detect(
        full_sequence,
        high=handler_occupation_threshold
    )

    alarms = []
    if True in over_threshold_anomalies.values:
        alarms.append(
            Alarm(
                alarm_content='The usage of handle has exceeded the warning level: %s%%.' % (
                    handler_occupation_threshold
                ),
                alarm_type=ALARM_TYPES.ALARM,
                metric_name='node_process_fds_rate',
                alarm_level=ALARM_LEVEL.WARNING,
            )
        )
    return alarms


@_check_for_metric(('os_disk_ioutils',), only_history=True)
def has_high_disk_ioutils(latest_sequences, future_sequences):
    latest_sequence, future_sequence = latest_sequences[0], future_sequences[0]
    full_sequence = latest_sequence + future_sequence
    disk_ioutils_threshold = dbmind.app.monitoring.get_param('disk_ioutils_threshold')

    over_threshold_anomalies = AnomalyDetections.do_threshold_detect(
        full_sequence,
        high=disk_ioutils_threshold
    )

    alarms = []
    if True in over_threshold_anomalies.values:
        alarms.append(
            Alarm(
                alarm_content='The DISK_IO_UTILS has exceeded the warning level: %s%%.' % (
                    disk_ioutils_threshold
                ),
                alarm_type=ALARM_TYPES.ALARM,
                metric_name='os_disk_ioutils',
                alarm_level=ALARM_LEVEL.WARNING,
            )
        )
    return alarms


@_check_for_metric(('pg_connections_used_conn', 'pg_connections_max_conn',), only_history=True)
def has_too_many_connections(latest_sequences, future_sequences):
    pg_connections_idle_session = latest_sequences[0] + future_sequences[0]
    max_connection = latest_sequences[1] + future_sequences[1]
    timestamps, values = [], []
    for i in range(min(len(pg_connections_idle_session), len(max_connection))):
        timestamps.append(pg_connections_idle_session.timestamps[i])
        values.append(pg_connections_idle_session.values[i] / max_connection.values[i])
    connection_rate = Sequence(timestamps=timestamps, values=values)
    connection_rate_threshold = dbmind.app.monitoring.get_param('connection_rate_threshold')

    over_threshold_anomalies = AnomalyDetections.do_threshold_detect(
        connection_rate,
        high=connection_rate_threshold
    )

    alarms = []
    if True in over_threshold_anomalies.values:
        alarms.append(
            Alarm(
                alarm_content='The CONNECTION_RATE is over {}%.'.format(
                    connection_rate_threshold
                ),
                alarm_type=ALARM_TYPES.ALARM,
                metric_name='pg_connections_idle_session',
                alarm_level=ALARM_LEVEL.WARNING,
            )
        )
    return alarms


"""Add checking rules above."""
