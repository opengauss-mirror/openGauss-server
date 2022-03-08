# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

import threading
import time

from dbmind.common.exceptions import ApiClientException
from dbmind.common.tsdb.tsdb_client import TsdbClient
from dbmind.common.tsdb.prometheus_client import PrometheusClient
from dbmind import global_vars


class TsdbClientFactory(object):
    tsdb_client = None
    shared_lock = threading.Lock()

    @classmethod
    def get_tsdb_client(cls) -> TsdbClient:
        if cls.tsdb_client is not None:
            return cls.tsdb_client
        with cls.shared_lock:
            if cls.tsdb_client is None:
                cls._init_client()
        return cls.tsdb_client

    @classmethod
    def _init_client(cls):
        configs = global_vars.configs
        tsdb_name = configs.get('TSDB', 'name')
        host = configs.get('TSDB', 'host')
        port = configs.get('TSDB', 'port')
        url = 'http://' + host + ':' + port
        if tsdb_name == 'prometheus':
            client = PrometheusClient(url=url)
            if not client.check_connection():
                raise ApiClientException("Failed to connect TSDB url.")
            cls.tsdb_client = client
        if cls.tsdb_client is None:
            raise ApiClientException("Failed to init TSDB client, please check config file")

        if abs(cls.tsdb_client.timestamp() - time.time() * 1000) > 60 * 1000:  # threshold is 1 minute.
            raise ApiClientException('Found clock drift between TSDB client and server, '
                                     'please check and synchronize system clocks.')


