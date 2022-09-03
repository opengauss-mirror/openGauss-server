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

from dbmind.common.types.alarm import Alarm
from dbmind.common.types import ALARM_LEVEL, ALARM_TYPES
from dbmind.common.types.root_cause import RootCause


def _find_highest_level(root_causes):
    highest = ALARM_LEVEL.NOTSET
    for root_cause in root_causes:
        if root_cause.level > highest:
            highest = root_cause.level

    return highest


def diagnose_for_sequences(host, metric, sequences):
    from .system import diagnose_system

    rca_system = list()
    # input parameter format:
    # ('xx.xx.xx.xx', 'os_cpu_usage', [Sequence, Sequence, Sequence])
    for seq in sequences:
        root_causes = diagnose_system(host, metric, seq)
        if len(root_causes) == 0:
            continue
        for cause, probability in root_causes:
            root_cause = RootCause.get(cause)
            root_cause.set_probability(probability)
            alarm = Alarm(
                host=host,
                metric_name=metric,
                alarm_content='Found anomaly on %s.' % metric,
                alarm_type=ALARM_TYPES.SYSTEM,
                alarm_level=ALARM_LEVEL.NOTICE,
                alarm_cause=root_cause
            )
            alarm.set_timestamp(start=seq.timestamps[0],
                                end=seq.timestamps[-1])
            rca_system.append(alarm)

    return rca_system


def diagnose_for_alarm_logs(host, alarm_logs):
    from .cluster import diagnose_cluster

    rca_cluster = list()
    # input parameter format:
    # ('xx.xx.xx.xx', [Log, Log, Log])
    for alarm_log in alarm_logs:
        rca_cluster.extend(
            diagnose_cluster(host, alarm_log)
        )
    return rca_cluster
