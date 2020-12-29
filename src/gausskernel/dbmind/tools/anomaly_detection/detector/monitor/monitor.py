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
from utils import RepeatTimer, transform_time_string
from .monitor_logger import logger


class Monitor:
    """
    This class is used for monitoring mutiple metric.
    """

    def __init__(self):
        self._tasks = dict()

    def apply(self, instance, *args, **kwargs):
        if instance in self._tasks:
            return False
        logger.info('add [{task}] in Monitor task......'.format(task=getattr(instance, 'metric_name')))
        interval = getattr(instance, 'forecast_interval')
        try:
            interval = transform_time_string(interval, mode='to_second')
        except ValueError as e:
            logger.error(e, exc_info=True)
            return
        timer = RepeatTimer(interval, instance.run, *args, **kwargs)
        self._tasks[instance] = timer
        return True

    def start(self):
        for instance, timer in self._tasks.items():
            timer.start()
            logger.info('begin to monitor [{task}]'.format(task=getattr(instance, 'metric_name')))
