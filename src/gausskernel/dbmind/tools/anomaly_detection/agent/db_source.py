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
import sys
import os
import logging
import threading
import time
import signal

from .source import Source

agent_logger = logging.getLogger('agent')


class DBSource(Source, threading.Thread):
    """
    This class inherits the threading.Thread, it is used for managing single task.
    """

    def __init__(self, interval):
        Source.__init__(self)
        threading.Thread.__init__(self)
        self._finished = threading.Event()
        self._tasks = {}
        self._interval = interval

    def add_task(self, name, task):
        if name not in self._tasks:
            self._tasks[name] = task

    def run(self):
        while not self._finished.is_set():
            try:
                content = {'timestamp': int(time.time())}
                # All tasks are executed serially.
                for task_name, task_handler in self._tasks.items():
                    value = task_handler.output()
                    content.update(**{task_name: value})
                self._channel.put(content)
            except Exception as e:
                agent_logger.error(e)
                process_id = os.getpid()
                os.kill(process_id, signal.SIGTERM)
            self._finished.wait(self._interval)

    def cancel(self):
        self._finished.set()
