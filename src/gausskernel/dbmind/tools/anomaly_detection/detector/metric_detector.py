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
import logging
import os
import re
import sys
from configparser import ConfigParser

import config
import global_vars
from detector.algorithm import get_fcst_alg
from detector.service.storage.sqlite_storage import SQLiteStorage
from detector.tools.slow_sql import SQL_RCA
from detector.tools.trend.forecast import Forecaster
from utils import TimeString, RepeatTimer

m_logger = logging.getLogger('detector')


class Detector:
    """
    This class is used for monitoring mutiple metric.
    """

    def __init__(self):
        self._tasks = dict()

    def apply(self, instance, *args, **kwargs):
        if instance in self._tasks:
            return False
        interval = getattr(instance, 'interval')
        try:
            interval = TimeString(interval).to_second()
        except ValueError as e:
            m_logger.error(e, exc_info=True)
            return False
        timer = RepeatTimer(interval, instance.run, *args, **kwargs)
        self._tasks[instance] = timer
        return True

    def start(self):
        for instance, timer in self._tasks.items():
            timer.start()


def _extract_trend_params():
    """
    Extract required parameters for tools task like forecast algorithm and
    database path.
    :return: parameter dict.
    """
    return {'forecast_alg': config.get('forecast', 'forecast_alg'),
            'database_dir': os.path.realpath(config.get('database', 'database_dir'))}


def _extract_sql_rca_params(metric_config):
    try:
        slow_sql_service_list =  metric_config.get('detector_method', 'slow_sql')
        data_handler = SQLiteStorage
        database_dir = config.get('database', 'database_dir')
        if metric_config.has_option('common_parameter', 'interval') and metric_config.get('common_parameter', 'interval'):
            interval = metric_config.get('common_parameter', 'interval')
            value_prefix, value_suffix = re.match(r'^(\d+)([WDHMS])$', interval).groups()
        else:
            interval = '300S'
    except Exception as e:
        m_logger.error("parameter interval is error, use default instead({interval}): {err}".format(interval='300S', err=str(e)))
        interval = '300S'

    return {'slow_sql_service_list': slow_sql_service_list,
            'data_handler': data_handler,
            'database_dir': database_dir,
            'interval': interval}


def _extract_trend_optional_params(metric_config):
    params = {'period': 1, 'interval': '300S', 'data_period': '300S', 'freq': 'S'}
    if not metric_config.has_section('common_parameter'):
        m_logger.warning("Not found 'common_parameter' section in {metric_config}".format(
            metric_config=global_vars.METRIC_CONFIG_PATH))
        return params
    if metric_config.has_option('common_parameter', 'data_period') and metric_config.get('common_parameter', 'data_period'):
        params['data_period'] = metric_config.get('common_parameter', 'data_period')
    if metric_config.has_option('common_parameter', 'period') and metric_config.get('common_parameter', 'period'):
        try:
            params['period'] = metric_config.getint('common_parameter', 'period')
        except Exception as e:
            m_logger.error("parameter period is error, use default instead({default})".format(default='1'))
            params['period'] = 1
    if metric_config.has_option('common_parameter', 'interval') and metric_config.get('common_parameter', 'interval'):
        params['interval'] = metric_config.get('common_parameter', 'interval')
    if metric_config.has_option('common_parameter', 'freq') and metric_config.get('common_parameter', 'freq'):
        params['freq'] = metric_config.get('common_parameter', 'freq')

    for key in ('data_period', 'freq', 'interval'):
        try:
            value_prefix, value_suffix = re.match(r'^(\d+)?([WDHMS])$', params[key]).groups()
        except Exception as e:
            if key == 'interval':
                default = '300S'
            if key == 'data_period':
                default = '300S'
            if key == 'freq':
                default = 'S'
            m_logger.error("parameter {key} is error, use default instead({default}).".format(key=key, default=default))
            params[key] = default
    return params


def _trend_service(metric_config):
    kwargs = {}
    trend_service_list = metric_config.get('detector_method', 'trend').split()
    trend_required_params = _extract_trend_params()
    trend_optional_params = _extract_trend_optional_params(metric_config)
    kwargs.update(**trend_required_params)
    kwargs.update(**trend_optional_params)
    kwargs.update(**{'trend_service_list': trend_service_list})
    kwargs.update(**{'metric_config': metric_config})
    kwargs['data_handler'] = SQLiteStorage
    kwargs['forecast_handler'] = get_fcst_alg(kwargs['forecast_alg'])()
    trend_service = Forecaster(service_list=trend_service_list, **kwargs)
    return trend_service


def _sql_rca_service(metric_config):
    kwargs = _extract_sql_rca_params(metric_config)
    sql_rca_service = SQL_RCA(**kwargs)
    return sql_rca_service


def detector_main():
    if not os.path.exists(global_vars.METRIC_CONFIG_PATH):
        m_logger.error('The {metric_config_path} is not exist.'.format(
            metric_config_path=global_vars.METRIC_CONFIG_PATH))
        return
    metric_config = ConfigParser()
    metric_config.read(global_vars.METRIC_CONFIG_PATH)
    if set(['detector_method', 'os_exporter', 'common_parameter']) != set(metric_config.sections()):
        different_secs = set(metric_config.sections()).symmetric_difference(set(['detector_method', 'os_exporter', 'common_parameter']))
        m_logger.error('error_section: {error_section}'.format(error_section=str(different_secs)))
        return
    trend_service = _trend_service(metric_config)
    slow_service = _sql_rca_service(metric_config)
    detector_service = Detector()
    try:
        detector_service.apply(trend_service)
        detector_service.apply(slow_service)
    except Exception as e:
        m_logger.error(e, exc_info=True)
    detector_service.start()

