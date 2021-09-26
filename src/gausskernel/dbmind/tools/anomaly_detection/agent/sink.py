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
import logging
import time
from urllib import request

_JSON_HEADER = {'Content-Type': 'application/json'}
agent_logger = logging.getLogger('agent')


class Sink:
    """
    This is parent class which is used for getting data from ChannelManager object and
    sending data at a specified time interval.
    """

    def __init__(self):
        self._channel = None
        self.running = False

    @property
    def channel(self):
        return self._channel

    @channel.setter
    def channel(self, channel):
        self._channel = channel

    def process(self):
        pass

    def start(self):
        self.running = True
        self.process()

    def stop(self):
        self.running = False


class HttpSink(Sink):
    """
    This class inherits from Sink and use to send data to server based on http/https
    method at a specified time interval.
    """

    def __init__(self, interval, url, context, db_host, db_port, db_type):
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
        self.db_host = db_host
        self.db_port = db_port
        self.db_type = db_type

    def process(self):
        agent_logger.info('Begin send data to {url}.'.format(url=self._url))
        while self.running:
            contents = self._channel.take()
            if not contents:
                time.sleep(0.5)
                continue

            contents.update(**{'flag': {'host': self.db_host, 'port': self.db_port, 'type': self.db_type}})
            retry_times = 5
            while retry_times:
                try:
                    req = request.Request(self._url, headers=_JSON_HEADER,
                                          data=json.dumps(contents).encode('utf-8'),
                                          method='POST')
                    request.urlopen(req, context=self.context)
                    break
                except Exception as e:
                    agent_logger.error("{error}, retry...".format(error=str(e)))
                    retry_times -= 1
                    if not retry_times:
                        raise
                time.sleep(1.0)
            time.sleep(self._interval)
