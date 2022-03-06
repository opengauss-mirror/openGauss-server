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
from enum import IntEnum


class ALARM_LEVEL(IntEnum):
    CRITICAL = 50
    FATAL = CRITICAL
    ERROR = 40
    WARNING = 30
    WARN = WARNING
    INFO = 20
    NOTICE = INFO
    DEBUG = 10
    NOTSET = 0

    def __str__(self):
        return self._name_


class ALARM_STATUS:
    RESOLVED = 'resolved'
    UNRESOLVED = 'unresolved'


class ALARM_TYPES:
    SYSTEM = 'SYSTEM'
    SLOW_QUERY = 'SLOW_QUERY'
    ALARM_LOG = 'ALARM_LOG'
