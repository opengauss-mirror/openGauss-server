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
from configparser import ConfigParser, NoOptionError

from flask import Flask
from flask_restful import Api
from flask_sqlalchemy import event

from task import metric_task
from utils import get_funcs, check_certificate
from .app import App
from .database import Base
from .resource import receiver
from .server_logger import logger


class MyApp(App):
    """
    This class is used for starting detector server
    """

    def __init__(self):
        App.__init__(self)
        self.app = None
        self.api = None
        self.config = None
        self.db = None
        self.dirname_path = None
        self.table_class_relation = {}

    def initialize_config(self, config_path):
        logger.info('initialize config...')
        if not os.path.exists(config_path):
            logger.error('{config_path} is not exist.'.format(config_path=config_path))
        self.dirname_path = os.path.dirname(config_path)
        self.config = ConfigParser()
        self.config.read(config_path)
        if not self.config.has_section('database'):
            logger.error("do not find 'database' section in config file.")
        else:
            if not self.config.has_option('database', 'database_path'):
                logger.error("do not find 'database_path' in database section.")

        if not self.config.has_section('server'):
            logger.error("do not find 'database' section in config file.")
        else:
            if not self.config.has_option('server', 'listen_host') or not self.config.has_option('server',
                                                                                                 'listen_port'):
                logger.error("do not find 'listen_host' or 'listen_port' in server section.")

    def initialize_app(self):
        logger.info('initialize app...')
        self.app = Flask(__name__)
        self.app.config['debug'] = False

        database_path = os.path.realpath(self.config.get('database', 'database_path'))
        database_path_dir = os.path.dirname(database_path)
        if not os.path.exists(database_path_dir):
            os.makedirs(database_path_dir)
        if os.name == 'nt':
            self.app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + database_path
        elif os.name == 'posix':
            self.app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:////' + database_path
        else:
            logger.error("do not support this {system}".format(system=os.name))
        self.app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
        self.api = Api(self.app)

    def initialize_database(self, db):
        logger.info('initialize database...')
        self.db = db
        self.db.init_app(self.app)
        default_database_parameter_values = {'max_rows': 100000, 'max_flush_cache': 1000}
        for parameter, default_value in default_database_parameter_values.items():
            if not self.config.has_option('database', parameter):
                logger.warn("do not find '{parameter}' in database section, use default value: '{default_value}'"
                            .format(parameter=parameter, default_value=default_value))
                value = default_value
            else:
                value = self.config.getint('database', parameter)
            globals()[parameter] = value

        Base.max_rows = globals()['max_rows']
        Base.max_flush_cache = globals()['max_flush_cache']
        metric_names = [func_name for func_name, _, in get_funcs(metric_task)]
        for metric_name in metric_names:
            table = type(metric_name.upper(), (Base, self.db.Model), {'__tablename__': metric_name, 'rows': 0})
            event.listen(table, 'after_insert', table.on_insert)
            self.table_class_relation[metric_name] = table
        with self.app.app_context():
            self.db.create_all()

    def add_resources(self):
        self.api.add_resource(receiver.Source, '/sink',
                              resource_class_kwargs={'db': self.db, 'table_class_relation': self.table_class_relation})

    def start_service(self):
        context = None
        listen_host = self.config.get('server', 'listen_host')
        listen_port = self.config.getint('server', 'listen_port')
        if self.config.has_option('security', 'tls') and self.config.getboolean('security', 'tls'):
            try:
                server_cert = self.config.get('security', 'server_cert')
                server_key = self.config.get('security', 'server_key')
                ca = self.config.get('security', 'ca')
            except NoOptionError as e:
                logger.error(e)
                return
            else:
                ssl_certificate_status = check_certificate(server_cert)
                ca_certificate_status = check_certificate(ca)
                if ssl_certificate_status['status'] == 'fail' or ca_certificate_status['status'] == 'fail':
                    logger.warn("error occur when check 'certificate'.")
                else:
                    if ssl_certificate_status['level'] == 'error' or ca_certificate_status['level'] == 'error':
                        logger.error("{ssl_certificate_info}; {ca_certificate_info}"
                                     .format(ssl_certificate_info=ssl_certificate_status['info'],
                                             ca_certificate_info=ca_certificate_status['info']))
                        return
                    else:
                        logger.warn("{ssl_certificate_info}; {ca_certificate_info}"
                                    .format(ssl_certificate_info=ssl_certificate_status['info'],
                                            ca_certificate_info=ca_certificate_status['info']))

                        pw_file = os.path.join(self.dirname_path, 'certificate/pwf')
                        with open(pw_file, mode='r') as f:
                            pw = f.read().strip()
                        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH, cafile=ca)
                        context.verify_mode = ssl.CERT_REQUIRED
                        context.load_cert_chain(certfile=server_cert, keyfile=server_key, password=pw)
        logger.info('Start service...')
        self.app.run(host=listen_host, port=listen_port, ssl_context=context)
