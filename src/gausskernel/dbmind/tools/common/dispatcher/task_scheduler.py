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
from threading import Thread, Event

ONE_DAY = 86400  # 24 * 60 * 60 seconds


class RepeatedTimer(Thread):
    """RepeatedTimer class.
    This class inherits from `threading.Thread`,
     which triggers a periodic func at a fixed interval.
    """

    def __init__(self, interval, function, *args, **kwargs):
        self._interval = interval
        self._function = function
        self._args = args
        self._kwargs = kwargs
        self._finished = Event()
        Thread.__init__(self)

    def run(self):
        while not self._finished.is_set():
            try:
                self._function(*self._args, **self._kwargs)
            except Exception as e:
                logging.error('RepeatedTimer<%s(%s), %d> occurred an error because %s.'
                              % (self._function, self._args, self._interval, e))
                logging.exception(e)
            self._finished.wait(self._interval)
        self._finished.set()

    def cancel(self):
        self._finished.set()

    def __hash__(self):
        return hash((self._interval, self._function))

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._interval == other._interval and self._function == other._function
        else:
            return False

    def __str__(self):
        return '%s(%s, %s)' % (self.__class__.__name__, self._function.__name__, self._interval)

    def __repr__(self):
        return self.__str__()


class _TimedTaskManager:
    def __init__(self):
        self.task_table = dict()
        self.timers = set()

    def apply(self, func, seconds):
        self.task_table[func] = seconds
        self.timers.add(RepeatedTimer(seconds, func))

    def start(self):
        for t in self.timers:
            t.start()

    def stop(self):
        for t in self.timers:
            t.cancel()


TimedTaskManager = _TimedTaskManager()


# TODO: make it dummy and implement with reflection.
def timer(seconds):
    def inner(func):
        TimedTaskManager.apply(func, seconds)
        return func

    return inner
