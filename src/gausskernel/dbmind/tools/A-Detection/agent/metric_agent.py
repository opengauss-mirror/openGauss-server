import os
import ssl
from configparser import ConfigParser, NoOptionError

from task import metric_task
from utils import get_funcs, transform_time_string, check_certificate
from .agent_logger import logger
from .channel import ChannelManager
from .db_source import DBSource
from .sink import HttpSink


def start_agent(config_path):
    if not os.path.exists(config_path):
        logger.error('{config_path} is not exist..'.format(config_path=config_path))
        return

    config = ConfigParser()
    config.read(config_path)

    if not config.has_section('agent') or not config.has_section('server'):
        logger.error("do not has 'agent' or 'server' section in config file...")
        return

    if not config.has_option('server', 'host') or not config.has_option('server', 'listen_port'):
        logger.error("do not has 'host' or 'listen_port' in 'server' section...")
        return
    else:
        context = None
        if config.has_option('security', 'tls') and config.getboolean('security', 'tls'):
            url = 'https://' + config.get('server', 'host') + ':' + config.get('server', 'listen_port') + '/sink'
            try:
                agent_cert = os.path.realpath(config.get('security', 'agent_cert'))
                agent_key = os.path.realpath(config.get('security', 'agent_key'))
                ca = os.path.realpath(config.get('security', 'ca'))
            except NoOptionError as e:
                logger.error(e)
                return
            else:
                logger.info(agent_cert)
                logger.info(agent_key)
                logger.info(ca)
                ssl_certificate_status = check_certificate(agent_cert)
                ca_certificate_status = check_certificate(ca)
                if ssl_certificate_status['status'] == 'fail':
                    logger.error("error occur when check '{certificate}'.".format(certificate=agent_cert))
                else:
                    if ssl_certificate_status['level'] == 'info':
                        logger.info(ssl_certificate_status['info'])
                    elif ssl_certificate_status['level'] == 'warn':
                        logger.warn(ssl_certificate_status['info'])
                    else:
                        logger.error(ssl_certificate_status['info'])
                        return

                if ca_certificate_status['status'] == 'fail':
                    logger.error("error occur when check '{certificate}'.".format(certificate=ca))
                else:
                    if ca_certificate_status['level'] == 'info':
                        logger.info(ca_certificate_status['info'])
                    elif ca_certificate_status['level'] == 'warn':
                        logger.warn(ca_certificate_status['info'])
                    else:
                        logger.error(ca_certificate_status['info'])
                        return
                pw_file = os.path.join(os.path.dirname(config_path), 'certificate/pwf')
                with open(pw_file, mode='r') as f:
                    pw = f.read().strip()
                
                context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca)
                context.check_hostname = False
                context.load_cert_chain(certfile=agent_cert, keyfile=agent_key, password=pw)
        else:
            logger.warn("detect not config 'ssl certificate', use 'http' instead.[advise use 'https']")
            url = 'http://' + config.get('server', 'host') + ':' + config.get('server', 'listen_port') + '/sink'

    default_agent_parameter_dicts = {'sink_timer_interval': '10S',
                                     'source_timer_interval': '10S',
                                     'channel_capacity': 1000}

    for parameter, default_value in default_agent_parameter_dicts.items():
        if not config.has_option('agent', parameter):
            logger.warn("do not provide '{parameter}' in 'agent' section, use default '{default_value}'."
                        .format(parameter=parameter,
                                default_value=default_agent_parameter_dicts['sink_timer_interval']))
            value = default_value
        else:
            value = config.get('agent', parameter)
        try:
            if parameter in ('sink_timer_interval', 'source_timer_interval'):
                globals()[parameter] = transform_time_string(value, mode='to_second')
            if parameter == 'channel_capacity':
                globals()[parameter] = int(value)
        except Exception as e:
            logger.error(e)
            return

    chan = ChannelManager()
    source = DBSource()
    http_sink = HttpSink(interval=globals()['sink_timer_interval'], url=url, context=context)
    source.channel_manager = chan
    http_sink.channel_manager = chan

    for task_name, task_func in get_funcs(metric_task):
        source.add_task(name=task_name,
                        interval=globals()['source_timer_interval'],
                        task=task_func,
                        maxsize=globals()['channel_capacity'])
    source.start()
    http_sink.start()
