"""
Copyright (c) 2020 Huawei Technologies Co.,Ltd.

openGauss is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:

         http://license.coscl.org.cn/MulanPSL2

THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
"""
import threading
import time

from .agent_logger import logger
from .source import Source


class TaskHandler(threading.Thread):
    """
    This class inherits the threading.Thread, it is used for managing single task.
    """

    def __init__(self, interval, function, *args, **kwargs):
        """
        :param interval: int, execute interval for task, unit is 'second'.
        :param function: function object for task
        :param args: list parameters
        :param kwargs: dict parameters
        """
        threading.Thread.__init__(self)
        self._function = function
        self._interval = interval
        self._args = args
        self._kwargs = kwargs
        self._finished = threading.Event()
        self._res = None
        self._channel = None

    def set_channel(self, channel):
        self._channel = channel

    def run(self):
        t = threading.currentThread()
        logger.info('current threading id is {id_}'.format(id_=t.ident))
        while not self._finished.is_set():
            try:
                self._res = self._function(*self._args, **self._kwargs)
                self._channel.put({'timestamp': int(time.time()), 'value': self._res})
            except Exception as e:
                logger.exception(e)
            self._finished.wait(self._interval)

    def cancel(self):
        self._finished.set()


class DBSource(Source):
    """
    This class inhert from Source and is used for acquiring mutiple metric data
    from database at a specified time interval.
    """

    def __init__(self):
        Source.__init__(self)
        self.running = False
        self._tasks = {}

    def add_task(self, name, interval, task, maxsize, *args, **kwargs):
        """
        Add task in DBSource object.
        :param name: string, task name
        :param interval: int, execute interval for task, unit is 'second'.
        :param task: function object of task
        :param maxsize: int, maxsize of channel in task.
        :param args: list parameters
        :param kwargs: dict parameters
        :return: NA
        """
        if name not in self._tasks:
            self._tasks[name] = TaskHandler(interval, task, *args, **kwargs)
            self._channel_manager.add_channel(name, maxsize)
            self._tasks[name].setDaemon(True)
            self._tasks[name].set_channel(self._channel_manager.get_channel(name))

    def start(self):
        for _, task in self._tasks.items():
            task.start()

    def stop(self):
        for _, task in self._tasks:
            task.cancel()
