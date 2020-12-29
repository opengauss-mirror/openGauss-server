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
import os
import re
from configparser import ConfigParser, NoOptionError, NoSectionError

from detector.timeseries_algorithms import forecast_algorithm
from task import metric_task
from utils import get_funcs
from .data_handler import DataHandler
from .forecast import Forecastor
from .monitor import Monitor
from .monitor_logger import logger


def check_required_parameter(config):
    """
    Check required parameters for monitor task like forecast algorithm and
    database path.
    :param config: config handler for config file.
    :return: check_status and parameter dict.
    """
    check_status = 'fail'
    required_parameters = {}
    try:
        forecast_alg = config.get('forecast', 'forecast_alg')
        database_path = config.get('database', 'database_path')
        database_path = os.path.realpath(database_path)
    except (NoOptionError, NoSectionError) as e:
        logger.error(e)
    else:
        required_parameters['forecast_alg'] = forecast_alg
        required_parameters['database_path'] = database_path
        check_status = 'success'

    return check_status, required_parameters


def check_detect_basis(metric_config, metric_name):
    """
    Check detect basis of parameter like 'minimum' and 'maximum'.
    :param metric_config: Config handler for metric config file.
    :param metric_name: string, metric name to monitor.
    :return: check_status and parameter dict.
    """
    check_status = 'fail'
    detect_basis_parameter = {}
    if not metric_config.has_option(metric_name, 'maximum') and not metric_config.has_option(metric_name, 'minimum'):
        logger.error("{metric_name} do not provide any range parameter ('minimum' or 'maximum'), skip monitor."
                     .format(metric_name=metric_name))
    else:
        try:
            if metric_config.has_option(metric_name, 'maximum'):
                detect_basis_parameter['maximum'] = metric_config.getfloat(metric_name, 'maximum')
            if metric_config.has_option(metric_name, 'minimum'):
                detect_basis_parameter['minimum'] = metric_config.getfloat(metric_name, 'minimum')
            check_status = 'success'
            if detect_basis_parameter.get('maximum', float('inf')) <= detect_basis_parameter.get('minimum',
                                                                                                 float('-inf')):
                logger.error(
                    "{metric_name}: maximum({maximum}) is smaller than minimum({minimum}) or equal, skip monitor"
                        .format(metric_name=metric_name,
                                maximum=detect_basis_parameter.get('maximum'),
                                minimum=detect_basis_parameter.get('minimum')))
                check_status = 'fail'
        except Exception as e:
            logger.error("{metric_name}: error when parse mamimum or minimum: {error}, skip monitor."
                         .format(metric_name=metric_name,
                                 error=str(e)))
            check_status = 'fail'

    return check_status, detect_basis_parameter


def check_optional_parameter(metric_config, metric_name):
    """
    Check optional parameter.
    :param metric_config: config handler for metric config file.
    :param metric_name: string, metric name to monitor.
    :return: parameter dict.
    """
    optional_parameter = {}
    default_metric_parameter_dicts = {'forecast_interval': '60S',
                                      'forecast_period': '60S',
                                      'data_period': '120S'}

    for parameter, default_value in default_metric_parameter_dicts.items():
        if not metric_config.has_option(metric_name, parameter):
            logger.warn("{metric_name} do not provide {parameter}, use default value: {default_value}."
                        .format(parameter=parameter,
                                metric_name=metric_name,
                                default_value=default_value))
            parameter_value = default_value
        else:
            config_parameter_value = metric_config.get(metric_name, parameter)
            if parameter == 'data_period' and config_parameter_value.isdigit():
                parameter_value = int(config_parameter_value)
            else:
                try:
                    value_prefix, value_suffix = re.match(r'^(\d+)([WDHMS])$', config_parameter_value).groups()
                    parameter_value = config_parameter_value
                except Exception:
                    logger.error(
                        "{metric_name} - {parameter}: wrong format '{config_parameter_value}', support combination "
                        "'S(second)' 'M(minute)' 'H(hour)' 'D(day)' 'W(week)' with pure integer or pure integer,"
                        " use default value: {default_value}."
                            .format(metric_name=metric_name,
                                    parameter=parameter,
                                    config_parameter_value=config_parameter_value,
                                    default_value=default_value))
                    parameter_value = default_value
        optional_parameter[parameter] = parameter_value

    return optional_parameter


def start_monitor(config_path, metric_config_path):
    """
    Monitor database metrics based on parameter from config.
    :param config_path: string, config path. 
    :param metric_config_path: string, metric config path.
    :return: NA
    """
    if not os.path.exists(config_path):
        logger.error('{config_path} is not exist.'.format(config_path=config_path))
        return
    if not os.path.exists(metric_config_path):
        logger.error('{metric_config_path} is not exist.'.format(metric_config_path=metric_config_path))
        return

    config = ConfigParser()
    config.read(config_path)
    check_status, required_parameters = check_required_parameter(config)
    if check_status == 'fail':
        return

    monitor_service = Monitor()
    config.clear()
    config.read(metric_config_path)
    metric_task_from_py = get_funcs(metric_task)
    metric_name_from_py = [item[0] for item in metric_task_from_py]
    metric_name_from_config = config.sections()

    for metric_name in set(metric_name_from_config).union(set(metric_name_from_py)):
        if metric_name in set(metric_name_from_config).difference(set(metric_name_from_py)):
            logger.error("'{metric_name}' is not defined in 'task/metric_task.py', skip monitor."
                         .format(metric_name=metric_name))
            continue
        if metric_name in set(metric_name_from_py).difference(set(metric_name_from_config)):
            logger.error("'{metric_name}' has no config information in 'task/metric_config.conf', skip monitor."
                         .format(metric_name=metric_name))
            continue
        if metric_name in set(metric_name_from_py).intersection(set(metric_name_from_config)):
            kwargs = {}
            optional_parameters = check_optional_parameter(config, metric_name)
            check_status, detect_basis_parameter = check_detect_basis(config, metric_name)
            if check_status == 'fail':
                continue
            kwargs.update(**optional_parameters)
            kwargs.update(**required_parameters)
            kwargs.update(**detect_basis_parameter)
            kwargs['data_handler'] = DataHandler
            kwargs['forecast_alg'] = forecast_algorithm(kwargs['forecast_alg'])()
            kwargs['metric_name'] = metric_name
            monitor_service.apply(Forecastor(**kwargs))
    monitor_service.start()
