import os
import re
from configparser import ConfigParser

from detector.algorithms import forecast_algorithm
from task import metric_task
from utils import get_funcs
from .data_handler import DataHandler
from .forecast import Forecastor
from .monitor import Monitor
from .monitor_logger import logger


def start_monitor(config_path, metric_config_path):
    if not os.path.exists(config_path):
        logger.error('{config_path} is not exist.'.format(config_path=config_path))
        return
    if not os.path.exists(metric_config_path):
        logger.error('{metric_config_path} is not exist.'.format(metric_config_path=metric_config_path))
        return

    config = ConfigParser()
    config.read(config_path)

    if not config.has_section('forecast') or not config.has_section('database'):
        logger.error("do not has 'forecast' or 'database' section in config file.")
        return

    if not config.has_option('forecast', 'forecast_alg'):
        logger.warn("do not find 'forecast_alg' in forecast section, use default 'fbprophet'.")
        forecast_alg = forecast_algorithm('fbprophet')
    else:
        try:
            forecast_alg = forecast_algorithm(config.get('forecast', 'forecast_alg'))
        except Exception as e:
            logger.warn("{error}, use default method: 'fbprophet'.".format(error=str(e)))
            forecast_alg = forecast_algorithm('fbprophet')

    if not config.has_option('database', 'database_path'):
        logger.error("do not find 'database_path' in database section...")
        return
    else:
        database_path = config.get('database', 'database_path')
        database_path = os.path.realpath(database_path)

    monitor_service = Monitor()
    config.clear()
    config.read(metric_config_path)

    metric_task_from_py = get_funcs(metric_task)
    metric_name_from_py = [item[0] for item in metric_task_from_py]
    metric_name_from_config = config.sections()

    default_metric_parameter_values = {'forecast_interval': '120S',
                                       'forecast_period': '60S',
                                       'data_period': '60S'}

    for metric_name in set(metric_name_from_config).union(set(metric_name_from_py)):
        if metric_name in set(metric_name_from_config).difference(set(metric_name_from_py)):
            logger.error("{metric_name} is not defined in 'task/metric_task.py', abandon monitoring."
                         .format(metric_name=metric_name))
            continue

        if metric_name in set(metric_name_from_py).difference(set(metric_name_from_config)):
            logger.error("{metric_name} has no config information in 'task/metric_config.conf', abandon monitoring."
                         .format(metric_name=metric_name))
            continue

        if metric_name in set(metric_name_from_py).intersection(set(metric_name_from_config)):
            kwargs = {}
            if not config.has_option(metric_name, 'maximum') and not config.has_option(metric_name, 'minimum'):
                logger.error("{metric_name} do not provide any range parameter ('minimum' or 'maximum'), skip monitor."
                             .format(metric_name=metric_name))
                continue
            else:
                if config.has_option(metric_name, 'maximum'):
                    kwargs['maximum'] = config.getfloat(metric_name, 'maximum')
                if config.has_option(metric_name, 'minimum'):
                    kwargs['minimum'] = config.getfloat(metric_name, 'minimum')

            for parameter, default_value in default_metric_parameter_values.items():
                if not config.has_option(metric_name, parameter):
                    logger.warn("{metric_name} do not provide {parameter}, use default value: {default_value}."
                                .format(parameter=parameter,
                                        metric_name=metric_name,
                                        default_value=default_value))
                    value = default_value
                else:
                    temp_value = config.get(metric_name, parameter)
                    if parameter == 'data_period' and temp_value.isdigit():
                        value = int(temp_value)
                    else:
                        try:
                            value_number, value_unit = re.match(r'(\d+)?([WDHMS])', temp_value).groups()
                            if value_number is None or value_unit is None or value_unit not in ('S', 'M', 'H', 'D', 'W'):
                                logger.error("wrong value: {metric_name} - {parameter}, only support 'S(second)' 'M(minute)'"
                                             "'H(hour)' 'D(day)' 'W(week)', not support '{unit}', use default value: {default_value}"
                                             .format(metric_name=metric_name,
                                                     unit=value_unit,
                                                     parameter=parameter,
                                                     default_value=default_value))
                                value = default_value
                            else:
                                value = temp_value 
                        except Exception as e:
                            logger.error("{metric_name} - {parameter} error: {error}, use default value: {default_value}.")
                            value = default_value

                kwargs[parameter] = value

            kwargs['forecast_alg'] = forecast_alg()
            kwargs['database_path'] = database_path
            kwargs['data_handler'] = DataHandler
            kwargs['metric_name'] = metric_name

            monitor_service.apply(Forecastor(**kwargs))

    monitor_service.start()
