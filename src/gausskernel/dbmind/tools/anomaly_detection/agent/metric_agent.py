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
import ssl
import sys
from configparser import ConfigParser, NoOptionError, NoSectionError

from task import metric_task
from utils import get_funcs, transform_time_string, check_certificate
from .agent_logger import logger
from .channel import ChannelManager
from .db_source import DBSource
from .sink import HttpSink


def check_agent_parameter(config):
    """
    Check if the agent parameter is valid, if the parameter is valid, 
    then return parameters dict, otherwise exit process.
    :param config: config handler for config file.
    :return: agent parameters dict.
    """
    agent_parameters = {}
    url = ""
    try:
        host = config.get('server', 'host')
        listen_port = config.get('server', 'listen_port')
    except (NoOptionError, NoSectionError) as e:
        logger.error(e)
        sys.exit(0)
    else:
        agent_parameters['host'] = host
        agent_parameters['listen_port'] = listen_port

    default_agent_parameter_dicts = {'sink_timer_interval': '10S',
                                     'source_timer_interval': '10S',
                                     'channel_capacity': 1000}
    for parameter, default_value in default_agent_parameter_dicts.items():
        try:
            if parameter == 'channel_capacity':
                agent_parameter_value = config.getint('agent', parameter)
                agent_parameters[parameter] = agent_parameter_value
            else:
                agent_parameter_value = config.get('agent', parameter)
                agent_parameters[parameter] = transform_time_string(agent_parameter_value, mode='to_second')
        except Exception as e:
            logger.error("error occur when acquire {parameter}: {error}, use default_value: {default_value}"
                         .format(parameter=parameter,
                                 error=str(e),
                                 default_value=default_value))
            agent_parameters[parameter] = default_agent_parameter_dicts[parameter]

    return agent_parameters


def check_certificate_setting(config, config_path):
    """
    If use https method, it is used for checking whether CA and Agent certificate is valid, 
    if certificate is not valid, then exit process, otherwise return right context; 
    if use http method, it skip checking and just return None.
    :param config: config handler for config file.
    :param config_path: string, config path.
    :return: if 'https', return certificate context, else return None.
    """
    context = None
    if config.has_option('security', 'tls') and config.getboolean('security', 'tls'):
        try:
            agent_cert = os.path.realpath(config.get('security', 'agent_cert'))
            agent_key = os.path.realpath(config.get('security', 'agent_key'))
            ca = os.path.realpath(config.get('security', 'ca'))
        except (NoOptionError, NoSectionError) as e:
            logger.error(e)
            sys.exit(0)
        else:
            ssl_certificate_status = check_certificate(agent_cert)
            ca_certificate_status = check_certificate(ca)
            if ssl_certificate_status['status'] == 'fail' or ca_certificate_status['status'] == 'fail':
                logger.warn("error occur when check 'certificate'.")
            else:
                if ssl_certificate_status['level'] == 'error' or ca_certificate_status['level'] == 'error':
                    logger.error("{ssl_certificate_info}; {ca_certificate_info}"
                                 .format(ssl_certificate_info=ssl_certificate_status['info'],
                                         ca_certificate_info=ca_certificate_status['info']))
                    sys.exit(0)
                else:
                    logger.warn("{ssl_certificate_info}; {ca_certificate_info}"
                                .format(ssl_certificate_info=ssl_certificate_status['info'],
                                        ca_certificate_info=ca_certificate_status['info']))

                    pw_file = os.path.join(os.path.dirname(config_path), 'certificate/pwf')
                    with open(pw_file, mode='r') as f:
                        pw = f.read().strip()

                    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca)
                    context.check_hostname = False
                    context.load_cert_chain(certfile=agent_cert, keyfile=agent_key, password=pw)

    return context


def start_agent(config_path):
    """
    Start agent service.
    :param config_path: string, config path.
    :return: NA
    """
    if not os.path.exists(config_path):
        logger.error('{config_path} is not exist..'.format(config_path=config_path))
        return -1

    config = ConfigParser()
    config.read(config_path)

    agent_parameters = check_agent_parameter(config)
    context = check_certificate_setting(config, config_path)

    if context is not None:
        url = 'https://' + agent_parameters['host'] + ':' + agent_parameters['listen_port'] + '/sink'
    else:
        url = 'http://' + agent_parameters['host'] + ':' + agent_parameters['listen_port'] + '/sink'

    chan = ChannelManager()
    source = DBSource()
    http_sink = HttpSink(interval=agent_parameters['sink_timer_interval'], url=url, context=context)
    source.channel_manager = chan
    http_sink.channel_manager = chan

    for task_name, task_func in get_funcs(metric_task):
        source.add_task(name=task_name,
                        interval=agent_parameters['source_timer_interval'],
                        task=task_func,
                        maxsize=agent_parameters['channel_capacity'])
    source.start()
    http_sink.start()
