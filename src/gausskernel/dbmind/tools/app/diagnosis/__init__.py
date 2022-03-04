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

from dbmind.common.types.alarm import Alarm, ALARM_TYPES, ALARM_LEVEL
from .cluster import diagnose_cluster
from .query import diagnose_query
from .system import diagnose_system


def _find_highest_level(root_causes):
    highest = ALARM_LEVEL.NOTSET
    for root_cause in root_causes:
        if root_cause.level > highest:
            highest = root_cause.level

    return highest


def diagnose_for_sequences(host, metric, sequences):
    rca_system = list()
    # input parameter format:
    # ('10.90.56.172', 'os_cpu_usage_rate', [Sequence, Sequence, Sequence])
    for seq in sequences:
        root_causes = diagnose_system(host, metric, seq)
        if len(root_causes) == 0:
            continue
        alarm = Alarm(
            host=host,
            metric_name=metric,
            alarm_content='found anomaly on %s' % metric,
            alarm_type=ALARM_TYPES.SYSTEM,
            alarm_subtype=None,
            alarm_level=_find_highest_level(root_causes),
            alarm_cause=root_causes
        )
        alarm.set_timestamp(start=seq.timestamps[0],
                            end=seq.timestamps[-1])
        rca_system.append(alarm)

    return rca_system


def diagnose_for_alarm_logs(host, alarm_logs):
    rca_cluster = list()
    # input parameter format:
    # ('10.90.56.172', [Log, Log, Log])
    for alarm_log in alarm_logs:
        rca_cluster.extend(
            diagnose_cluster(host, alarm_log)
        )
    return rca_cluster
