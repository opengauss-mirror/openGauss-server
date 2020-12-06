class Source:
    def __init__(self):
        self._channel_manager = None

    def start(self):
        pass

    def stop(self):
        pass

    @property
    def channel_manager(self):
        return self._channel_manager

    @channel_manager.setter
    def channel_manager(self, channel_manager):
        self._channel_manager = channel_manager
