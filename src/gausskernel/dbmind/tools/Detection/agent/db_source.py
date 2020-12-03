import threading
import time

from .agent_logger import logger
from .source import Source


class TaskHandler(threading.Thread):
    def __init__(self, interval, function, *args, **kwargs):
        threading.Thread.__init__(self)
        self._func = function
        self._interval = interval
        self._args = args
        self._kwargs = kwargs
        self._finished = threading.Event()
        self._res = None
        self._channel = None

    def set_channel(self, channel):
        self._channel = channel

    def run(self):
        while not self._finished.is_set():
            try:
                self._res = self._func(*self._args, **self._kwargs)
                self._channel.put({'timestamp': int(time.time()), 'value': self._res})
            except Exception as e:
                logger.exception(e)
            self._finished.wait(self._interval)

    def cancel(self):
        self._finished.set()


class DBSource(Source):
    def __init__(self):
        Source.__init__(self)
        self.running = False
        self._tasks = {}

    def add_task(self, name, interval, task, maxsize, *args, **kwargs):
        if name not in self._tasks:
            self._tasks[name] = TaskHandler(interval, task, *args, **kwargs)
            self._channel_manager.add_channel(name, maxsize)
            self._tasks[name].set_channel(self._channel_manager.get_channel(name))

    def start(self):
        for _, task in self._tasks.items():
            task.start()
