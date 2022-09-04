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
import time

from dbmind.common.utils import dbmind_assert


class HealingAction:
    def __init__(self, action_name, callback, callback_params=None):
        self.action_name = action_name
        self._callback = callback
        self._callback_params = callback_params or {}
        self.performed = False
        self._performed_at = None
        self._alarms = []
        self._success = False
        self._detail = None
        self._cost_seconds = 0

    def attach_alarm(self, alarm):
        self._alarms.append(alarm)

    def perform(self):
        success, detail = False, None
        try:
            self._performed_at = int(time.time() * 1000)
            start_time = time.monotonic()
            result = self._callback(**self._callback_params)
            self._cost_seconds = time.monotonic() - start_time
            if result is None:
                success, detail = False, 'No specific error cause returned.'
            else:
                success, detail = result
            if success and not detail:
                detail = 'Success to perform repair action.'
            detail = detail + ' Cost %.2f seconds for repairing.' % self._cost_seconds
        except Exception as e:
            detail = str(e)
            logging.warning('[HealingAction] An exception occurred while calling %s with %s.',
                            self._callback, self._callback_params, exc_info=e)

        self.performed = True
        self._success = success
        self._detail = detail

    @property
    def result(self):
        dbmind_assert(self.performed)

        result = HealingResult(
            success=self._success,
            detail=self._detail,
            action=self.action_name
        )
        hosts = set()
        events = set()
        root_causes = set()
        for alarm in self._alarms:
            hosts.add(alarm.host)
            events.add(alarm.alarm_content)
            for cause in alarm.alarm_cause:
                root_causes.add(cause.title)
        result.host = ' '.join(hosts)
        result.trigger_events = '\n'.join(events)
        result.trigger_root_causes = '\n'.join(root_causes)
        result.called_method = self._callback.__qualname__
        result.occurrence_at = self._performed_at
        return result


class HealingResult:
    def __init__(self, success, detail, action):
        self.host = ''
        self.trigger_events = ''
        self.trigger_root_causes = ''
        self.action = action
        self.called_method = ''
        self.success = success
        self.detail = detail
        self.occurrence_at = -1
