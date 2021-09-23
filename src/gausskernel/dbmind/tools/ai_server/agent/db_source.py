#!/usr/bin/python3
# -*- coding: utf-8 -*-
#############################################################################
# Copyright (c): 2021, Huawei Tech. Co., Ltd.
# FileName     : db_source.py
# Version      :
# Date         : 2021-4-7
# Description  : Collection Task Management
#############################################################################

try:
    import sys
    import os
    import threading

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../"))
    from agent.source import Source
except ImportError as err:
    sys.exit("db_source.py: Failed to import module: %s." % str(err))


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
        self._logger = kwargs["logger"]

    def set_channel(self, channel):
        self._channel = channel

    def run(self):

        while not self._finished.is_set():
            try:
                metrics = self._function(*self._args, **self._kwargs)
                self._channel.put(metrics)
            except Exception as e:
                self._logger.exception(e)
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
