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
import signal

import config
import global_vars
from cert import get_agent_ssl_context
from task import database_exporter, os_exporter, wdr
from utils import TimeString
from utils import getpasswd
from deamon import handle_sigterm
from .channel import MemoryChannel
from .db_source import DBSource
from .sink import HttpSink

agent_logger = logging.getLogger('agent')


def _extract_params():
    """
    Check if the agent parameter is valid, if the parameter is valid,
    then return parameters dict, otherwise exit process.
    :return: agent parameters dict.
    """
    params = {}

    host = config.get('server', 'host')
    listen_port = config.get('server', 'listen_port')
    params['host'] = host
    params['listen_port'] = listen_port

    default_params = {'sink_timer_interval': '10S',
                      'source_timer_interval': '10S',
                      'channel_capacity': 1000}
    for parameter, default in default_params.items():
        try:
            if parameter == 'channel_capacity':
                agent_parameter_value = config.getint('agent', parameter)
                params[parameter] = agent_parameter_value
            else:
                agent_parameter_value = config.get('agent', parameter)
                params[parameter] = TimeString(agent_parameter_value).to_second()
        except Exception as e:
            agent_logger.error("An error ({error}) occurs when acquiring {parameter},"
                               " using default value: {default_value}."
                               .format(parameter=parameter,
                                       error=e,
                                       default_value=default))
            params[parameter] = default_params[parameter]
    params['db_host'] = config.get('agent', 'db_host')
    params['db_port'] = config.get('agent', 'db_port')
    params['db_type'] = config.get('agent', 'db_type')

    # Https configures.
    if config.has_option('security', 'tls') and config.getboolean('security', 'tls'):
        params['agent_cert'] = os.path.realpath(config.get('security', 'agent_cert'))
        params['agent_key'] = os.path.realpath(config.get('security', 'agent_key'))
        params['ca'] = os.path.realpath(config.get('security', 'ca'))
        pwd_path = os.path.dirname(params['agent_cert'])
        params['cert_pwd'] = getpasswd(pwd_path)

    return params


def agent_main():
    """
    The main entrance of the agent service.
    """
    signal.signal(signal.SIGTERM, handle_sigterm) 
    try:
        params = _extract_params()
        context = get_agent_ssl_context(params)
        if context:
            protocol = 'https'
        else:
            protocol = 'http'

        url = '%s://%s:%s/sink' % (protocol, params['host'], params['listen_port'])
        chan = MemoryChannel(maxsize=params['channel_capacity'])
        source = DBSource(interval=params['source_timer_interval'])
        source.setDaemon(True)
        http_sink = HttpSink(interval=params['sink_timer_interval'],
                             url=url,
                             context=context,
                             db_host=params['db_host'],
                             db_port=params['db_port'],
                             db_type=params['db_type'])
        source.channel = chan
        http_sink.channel = chan

        source.add_task(name=database_exporter.DatabaseExporter.__tablename__,
                        task=database_exporter.DatabaseExporter(db_port=params['db_port']))
        source.add_task(name=os_exporter.OSExporter.__tablename__, task=os_exporter.OSExporter(db_port=params['db_port']))
        source.add_task(name=wdr.WDR.__tablename__, task=wdr.WDR(db_port=params['db_port'], db_type=params['db_type']))

        source.start()
        http_sink.start()
    except Exception as e:
        agent_logger.error(e)
        
