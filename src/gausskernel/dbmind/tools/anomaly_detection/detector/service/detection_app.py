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

from flask import Flask
from flask_restful import Api

import config
import global_vars
from cert import get_server_ssl_context
from utils import getpasswd
from .app import App
from .resource import receiver

service_logger = logging.getLogger('service')


def check_params():
    if not config.has_section('database'):
        service_logger.error("Not found 'database' section in config file.")
    else:
        if not config.has_option('database', 'database_path'):
            service_logger.error("Not found 'database_path' in database section.")

    if not config.has_section('server'):
        service_logger.error("Not found 'database' section in config file.")
    else:
        if not config.has_option('server', 'listen_host') or not config.has_option('server', 'listen_port'):
            service_logger.error("Not found 'listen_host' or 'listen_port' in server section.")


def _extract_params():
    params = {'storage_duration': '12H'}
    for name, default_value in params.items():
        if not config.has_option('database', name):
            service_logger.warning("Not found '{name}' in database section, using default value: '{default_value}'."
                                   .format(name=name, default_value=default_value))
            value = default_value
        else:
            value = config.get('database', name)
        params[name] = value

    params['database_dir'] = config.get('database', 'database_dir')
    params['listen_host'] = config.get('server', 'listen_host')
    params['listen_port'] = config.getint('server', 'listen_port')
    params['white_host'] = config.get('server', 'white_host')
    params['white_port'] = config.get('server', 'white_port')

    # Https configures.
    if config.has_option('security', 'tls') and config.getboolean('security', 'tls'):
        params['server_cert'] = config.get('security', 'server_cert')
        params['server_key'] = config.get('security', 'server_key')
        params['ca'] = config.get('security', 'ca')
        pwd_path = os.path.dirname(params['server_cert'])
        params['cert_pwd'] = getpasswd(pwd_path)
    return params


class DetectionApp(App):
    """
    This class is used for starting detector service.
    """

    def __init__(self):
        App.__init__(self)
        self.params = _extract_params()
        self.app = Flask(__name__)
        self.app.config['debug'] = False
        self.api = Api(self.app)

    def check_valid_address(self):
        valid_dbname = []
        # Consider the case if user do not provide any host and port information.
        if not self.params['white_host']:
            white_host = ''
        else:
            white_host = self.params['white_host']
        if not self.params['white_port']:
            white_port = ''
        else:
            white_port = self.params['white_port']
        white_host = re.findall(r'\d{1,4}\.\d{1,4}\.\d{1,4}\.\d{1,4}', white_host)
        white_port = re.findall(r'\d{1,5}', white_port)
        for i, ip in enumerate(white_host):
            for j, port in enumerate(white_port):
                dbname = ip + ':' + port
                valid_dbname.append(dbname)
        return valid_dbname

    def add_resources(self):
        database_dir = os.path.realpath(self.params['database_dir'])
        os.makedirs(database_dir, exist_ok=True)
        valid_dbname = self.check_valid_address()
        self.api.add_resource(receiver.Source, '/sink', resource_class_kwargs={'database_dir': database_dir,
                                                                               'valid_dbname': valid_dbname})

    def start_service(self):
        context = get_server_ssl_context(self.params)
        try:
            self.app.run(host=self.params['listen_host'],
                         port=self.params['listen_port'],
                         ssl_context=context)
        except Exception as e:
            service_logger.fatal(e)
