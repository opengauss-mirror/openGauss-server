#!/usr/bin/python3
# -*- coding: utf-8 -*-
#############################################################################
# Copyright (c): 2021, Huawei Tech. Co., Ltd.
# FileName     : http_sink.py
# Version      :
# Date         : 2021-4-7
# Description  : Sends data to server.
#############################################################################

try:
    import os
    import sys
    import time
    import json
    from urllib import request

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../"))
    from common.utils import Common
    from agent.sink import Sink
except ImportError as err:
    sys.exit("http_sink.py: Failed to import module: %s." % str(err))

header = {'Content-Type': 'application/json'}


class HttpSink(Sink):
    """
    This class inherit from Sink and use to send data to server based on http/https
    method at a specified time interval.
    """

    def __init__(self, interval, url, context, logger):
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
        self.logger = logger
        try:
            self.host = Common.acquire_collection_info()["ip"]
            self.port = Common.acquire_collection_info()["port"]
            self.data_type = Common.acquire_collection_info()["data_type"]
            self.cluster_name = Common.parser_config_file("agent", "cluster_name")
        except Exception as err_msg:
            logger.error(str(err_msg))
            raise Exception(str(err_msg))

    def process(self):
        self.logger.info('Begin send data to %s' % self._url)
        while self.running:
            time.sleep(self._interval)
            # {'OSExporter': {metric1: value, ...}, 'DatabaseExporter': {metric1: value, ...}}
            contents = self._channel_manager.get_channel_content()
            retry_times = 0
            if contents:
                # {'database': ip_port,
                # 'OSExporter': {metric1: value, ...},
                # 'DatabaseExporter': {metric1: value, ...}}
                contents.update(
                    {"database": "%s:%s:%s:%s" % (self.cluster_name, self.host.replace(".", "_"),
                                                  str(self.port), self.data_type)})
                while True:
                    try:
                        req = request.Request(self._url, headers=header,
                                              data=json.dumps(contents).encode('utf-8'),
                                              method='POST')
                        request.urlopen(req, context=self.context)
                        break
                    except Exception as e:
                        retry_times += 1
                        self.logger.warn(str(e) + " Retry times: %d." % retry_times, exc_info=True)
                        if retry_times >= 10:
                            raise ConnectionError("Failed to send data, \nError: %s" % str(e))
                    time.sleep(1)
            else:
                self.logger.warn('Not found data in each channel.')
