from queue import Queue, Empty, Full

from .agent_logger import logger


class Channel:
    def __init__(self):
        pass

    def put(self, event):
        pass

    def take(self):
        pass

    def size(self):
        pass


class MemoryChannel(Channel):
    def __init__(self, name, maxsize=None):
        Channel.__init__(self)
        self.name = name
        self.maxsize = maxsize
        self.memory = Queue(maxsize)

    def put(self, event):
        if self.maxsize and self.size() > self.maxsize:
            logger.warn("Channel {name} has reach queue maxsize".format(name=self.name))
        try:
            self.memory.put(event, block=True, timeout=0.2)
        except Full:
            logger.warn("throw away {name} data when reach maxsize".format(name=self.name))

    def take(self):
        try:
            return self.memory.get_nowait()
        except Empty:
            logger.warn('Channel {name} is empty.'.format(name=self.name))
            return None

    def size(self):
        rv = self.memory.qsize()
        return 0 if rv is None else rv


class ChannelManager:

    def __init__(self):
        self._channels = {}

    def add_channel(self, name, maxsize):
        self._channels[name] = MemoryChannel(name=name, maxsize=maxsize)
        logger.info('Channel {name} is created.'.format(name=name))

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
