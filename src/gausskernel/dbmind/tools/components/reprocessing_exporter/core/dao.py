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
from collections import defaultdict
from dbmind.common.tsdb.prometheus_client import PrometheusClient

_prometheus_client: 'PrometheusClient' = None


class PrometheusMetricConfig:
    def __init__(self, name, promql, desc):
        self.name = name
        self.desc = desc
        self.promql = promql
        self.labels = []
        self.label_map = defaultdict(str)

    def __repr__(self):
        return repr((self.name, self.promql, self.labels))


def set_prometheus_client(host, port):
    global _prometheus_client

    url = 'http://' + host + ':' + port
    client = PrometheusClient(url)
    if not client.check_connection():
        raise ConnectionRefusedError("failed to connect TSDB url: %s" % url)

    _prometheus_client = client


def query(promql):
    return _prometheus_client.custom_query(
        promql
    )
