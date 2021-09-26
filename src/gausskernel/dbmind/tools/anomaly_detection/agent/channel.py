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
import logging
from queue import Queue, Empty, Full

agent_logger = logging.getLogger('agent')


class Channel:
    """
    This is parent class of buffer channel, which acts as a buffer
    middleware between Source and Sink.
    """

    def __init__(self):
        pass

    def put(self, event):
        pass

    def take(self):
        pass

    def size(self):
        pass


class MemoryChannel(Channel):
    """
    This class inherits from Channel, which is used to buffer data in memory.
    """

    def __init__(self, maxsize=None):
        """
        :param maxsize: int, maxsize of channel
        """
        Channel.__init__(self)
        self.maxsize = maxsize
        self.memory = Queue(maxsize)

    def put(self, event):
        if self.maxsize and self.size() > self.maxsize:
            agent_logger.warning("Channel has reached max size.")
        try:
            self.memory.put(event, block=True, timeout=0.2)
        except Full:
            agent_logger.warning("Throw away new data due to the channel reaching max size.")

    def take(self):
        try:
            return self.memory.get_nowait()
        except Empty:
            return None

    def size(self):
        rv = self.memory.qsize()
        return 0 if rv is None else rv
