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
from queue import Queue, Empty, Full

from .agent_logger import logger


class Channel:
    """
    This is father class of buffer channel, it acts as a buffer 
    medium between Source and Sink. 
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
    This class inherit from Channel, use to buffer data in memory.
    """

    def __init__(self, name, maxsize=None):
        """
        :param name: string, channel name
        :param maxsize: int, maxsize of channel
        """
        Channel.__init__(self)
        self.name = name
        self.maxsize = maxsize
        self.memory = Queue(maxsize)

    def put(self, event):
        if self.maxsize and self.size() > self.maxsize:
            logger.warn("channel {name} has reach queue maxsize".format(name=self.name))
        try:
            self.memory.put(event, block=True, timeout=0.2)
        except Full:
            logger.warn("throw away {name} data when reach maxsize".format(name=self.name))

    def take(self):
        try:
            return self.memory.get_nowait()
        except Empty:
            logger.warn('channel {name} is empty.'.format(name=self.name))
            return None

    def size(self):
        rv = self.memory.qsize()
        return 0 if rv is None else rv


class ChannelManager:
    """
    This class is used for managing multiple MemoryChannel object.
    """

    def __init__(self):
        self._channels = {}

    def add_channel(self, name, maxsize):
        """
        Add MemoryChannel object.
        :params name: string, name of Memorychannel object
        :params maxsize: int, maxsize of Memorychannel object
        """
        self._channels[name] = MemoryChannel(name=name, maxsize=maxsize)
        logger.info('channel {name} is created.'.format(name=name))

    def get_channel(self, name):
        return self._channels[name]

    def check(self, name):
        if name not in self._channels:
            return False
        return True

    def get_channel_content(self):
        contents = {}
        for name, queue in self._channels.items():
            event = queue.take()
            if event is not None:
                contents[name] = event
        return contents

    def size(self):
        return len(self._channels)
