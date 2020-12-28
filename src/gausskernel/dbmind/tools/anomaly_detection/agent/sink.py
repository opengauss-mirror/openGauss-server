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
import json
import time
from urllib import request

from .agent_logger import logger

header = {'Content-Type': 'application/json'}


class Sink:
    """
    This is father class which is used for getting data from ChannelManager object and 
    sending data at a specified time interval.
    """

    def __init__(self):
        self._channel_manager = None
        self.running = False

    @property
    def channel_manager(self):
        return self._channel_manager

    @channel_manager.setter
    def channel_manager(self, channel_manager):
        self._channel_manager = channel_manager

    def process(self):
        pass

    def start(self):
        self.running = True
        self.process()

    def stop(self):
        self.running = False


class HttpSink(Sink):
    """
    This class inherit from Sink and use to send data to server based on http/https 
    method at a specified time interval.
    """

    def __init__(self, interval, url, context):
        """
        :param interval: int, time interval when send data.
        :param url: string, http/https url.
        :param context: certificate context for https method.
        """
        Sink.__init__(self)
        self._interval = interval
        self.running = False
        self._url = url
        self.context = context

    def process(self):
        logger.info('begin send data to {url}'.format(url=self._url))
        while self.running:
            time.sleep(self._interval)
            contents = self._channel_manager.get_channel_content()
            if contents:
                while True:
                    try:
                        req = request.Request(self._url, headers=header, data=json.dumps(contents).encode('utf-8'),
                                              method='POST')
                        request.urlopen(req, context=self.context)
                        break
                    except Exception as e:
                        logger.warn(e, exc_info=True)
                    time.sleep(0.5)
            else:
                logger.warn('Not found data in each channel.')
