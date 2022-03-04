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
from typing import Optional, Iterable, Union

from .root_cause import RootCause
from .enumerations import ALARM_TYPES, ALARM_LEVEL


class Alarm:
    def __init__(self,
                 host: Union[str],
                 alarm_content: str,
                 alarm_type: ALARM_TYPES,
                 alarm_subtype=None,
                 metric_name: str = None,
                 alarm_level: ALARM_LEVEL = ALARM_LEVEL.ERROR,
                 alarm_cause: Optional[Union[RootCause, Iterable[RootCause]]] = None,
                 extra=None):
        self.host = host
        self.alarm_content = alarm_content
        self.alarm_type = alarm_type
        self.alarm_subtype = alarm_subtype
        self.metric_name = metric_name
        self.alarm_level = alarm_level
        self.start_timestamp = self.end_timestamp = None

        self.extra = extra

        if isinstance(alarm_cause, Iterable):
            self.alarm_cause = list(alarm_cause)
        elif isinstance(alarm_cause, RootCause):
            self.alarm_cause = [alarm_cause]
        else:
            self.alarm_cause = list()

    def add_reason(self, root_cause):
        self.alarm_cause.append(root_cause)
        return self

    def set_timestamp(self, start, end=None):
        self.start_timestamp = start
        self.end_timestamp = end
        return self

    def __repr__(self):
        return '[%s](%s)' % (
            self.alarm_content, self.alarm_cause
        )

    @property
    def root_causes(self):
        lines = list()
        index = 1
        for c in self.alarm_cause:
            lines.append(
                '%d. %s: (%.2f) %s' % (index, c.title, c.probability, c.detail)
            )
            index += 1
        return '\n'.join(lines)

    @property
    def suggestions(self):
        lines = list()
        index = 1
        for c in self.alarm_cause:
            lines.append(
                '%d. %s' % (index, c.suggestion if c.suggestion else 'No suggestions.')
            )
            index += 1
        return '\n'.join(lines)
