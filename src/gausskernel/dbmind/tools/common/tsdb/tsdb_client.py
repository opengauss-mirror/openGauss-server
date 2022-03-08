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

import time
from datetime import datetime, timedelta


class TsdbClient(object):
    """The common baseclass of various time series database
    implementation classes, which is actually an interface,
    and other subclasses are implemented based on this
    interface in order to keep consistent format of
    return value to the upper layer's calling.

      ..Attention::

         The format of return value should be a list of Sequence.

    """

    def check_connection(self, params: dict = None) -> bool:
        """check to connect tsdb client"""
        pass

    def get_current_metric_value(self,
                                 metric_name: str,
                                 label_config: dict = None,
                                 params: dict = None):
        """get metric target from tsdb"""
        pass

    def get_metric_range_data(self,
                              metric_name: str,
                              label_config: dict = None,
                              start_time: datetime = (datetime.now() - timedelta(minutes=10)),
                              end_time: datetime = datetime.now(),
                              chunk_size: timedelta = None,
                              step: str = None,
                              params: dict = None):
        """get metric target from tsdb"""
        pass

    def timestamp(self):
        """get the current unix-timestamp from the time-series database."""
        return int(time.time() * 1000)

