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
import argparse
import getpass
import os
import tempfile
import logging
from logging.handlers import TimedRotatingFileHandler

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from dbmind.common.daemon import Daemon
from dbmind.common.security import check_ip_valid, check_port_valid
from dbmind.common.utils import write_to_terminal
from dbmind.common.utils import check_ssl_certificate_remaining_days, check_ssl_file_permission
from . import controller
from . import dao
from . import service
from .. import __version__

CURR_DIR = os.path.realpath(
    os.path.join(os.path.dirname(__file__), '..')
)
DEFAULT_YAML = 'reprocessing_exporter.yml'
DEFAULT_LOGFILE = 'reprocessing_exporter.log'
with tempfile.NamedTemporaryFile(suffix='.pid') as fp:
    EXPORTER_PIDFILE_NAME = fp.name

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


def path_type(path):
    if os.path.exists(path):
        return os.path.realpath(path)
    else:
        raise argparse.ArgumentTypeError('%s is not a valid path.' % path)


class CheckPort(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        if not check_port_valid(values):
            parser.error('Illegal port value(1024~65535): %s.' % values)
        setattr(args, self.dest, values)


class CheckIP(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        if not check_ip_valid(values):
            parser.error('Illegal IP: %s.' % values)
        setattr(args, self.dest, values)


def is_exporter_alive(host, port, disable_https):
    scheme = 'http' if disable_https else 'https'
    host = '127.0.0.1' if host == '0.0.0.0' else host
    url = "{}://{}:{}/metrics".format(scheme, host, port)
    try:
        response = requests.get(
            url,
            headers={"Content-Type": "application/json"},
            verify=False,
        )
        if response.status_code == 200:
            return True
        return False
    except Exception as e:
        return False


def is_prometheus_alive(host, port):
    scheme = 'http'
    url = "{}://{}:{}/api/v1/query?query=up".format(scheme, host, port)
    try:
        response = requests.get(
            url,
            headers={"Content-Type": "application/json"},
        )
        if response.status_code == 200:
            return True
        return False
    except Exception as e:
        return False


def parse_argv(argv):
    parser = argparse.ArgumentParser(
        description='Reprocessing Exporter: A re-processing module for metrics stored in the Prometheus server.'
    )
    parser.add_argument('prometheus_host', help='from which host to pull data', action=CheckIP)
    parser.add_argument('prometheus_port', help='the port to connect to the Prometheus host', action=CheckPort)
    parser.add_argument('--disable-https', action='store_true',
                        help='disable Https schema')
    parser.add_argument('--ssl-keyfile', type=path_type, help='set the path of ssl key file')
    parser.add_argument('--ssl-certfile', type=path_type, help='set the path of ssl certificate file')
    parser.add_argument('--web.listen-address', default='127.0.0.1', action=CheckIP,
                        help='address on which to expose metrics and web interface')
    parser.add_argument('--web.listen-port', type=int, default=8181, action=CheckPort,
                        help='listen port to expose metrics and web interface')
    parser.add_argument('--collector.config', type=path_type, default=os.path.join(CURR_DIR, DEFAULT_YAML),
                        help='according to the content of the yaml file for metric collection')
    parser.add_argument('--log.filepath', type=os.path.realpath,
                        default=os.path.join(os.getcwd(), DEFAULT_LOGFILE),
                        help='the path to log')
    parser.add_argument('--log.level', default='info', choices=('debug', 'info', 'warn', 'error', 'fatal'),
                        help='only log messages with the given severity or above.'
                             ' Valid levels: [debug, info, warn, error, fatal]')
    parser.add_argument('--version', action='version', version=__version__)

    args = parser.parse_args(argv)
    ssl_keyfile_pwd = None
    if args.disable_https:
        # Clear up redundant arguments.
        args.ssl_keyfile = None
        args.ssl_certfile = None
    else:
        if not (args.ssl_keyfile and args.ssl_certfile):
            parser.error('If you use the Https protocol (default), you need to give the argument values '
                         'of --ssl-keyfile and --ssl-certfile. '
                         'Otherwise, use the --disable-https argument to disable the Https protocol.')
        else:
            # Need to check whether the key file has been encrypted.
            with open(args.ssl_keyfile) as fp:
                for line in fp.readlines():
                    if line.startswith('Proc-Type') and 'ENCRYPTED' in line.upper():
                        ssl_keyfile_pwd = ''
                        while not ssl_keyfile_pwd:
                            ssl_keyfile_pwd = getpass.getpass('Enter PEM pass phrase:')
    setattr(args, 'keyfile_password', ssl_keyfile_pwd)
    return args


def set_logger(filepath, level):
    level = level.upper()
    log_path = os.path.dirname(filepath)
    if not os.path.isdir(log_path):
        os.makedirs(log_path, 500)

    formatter = logging.Formatter(
        '[%(asctime)s]'
        '[%(filename)s:%(lineno)d]'
        '[%(funcName)s][%(levelname)s][%(threadName)s] '
        '- %(message)s'
    )
    handler = TimedRotatingFileHandler(
        filename=filepath,
        when='D',
        interval=1,
        backupCount=15,
        encoding='UTF-8',
        delay=False,
        utc=True
    )
    handler.setFormatter(formatter)
    handler.setLevel(level)
    default_logger = logging.getLogger()
    default_logger.setLevel(level)
    default_logger.addHandler(handler)


class ExporterMain(Daemon):
    def clean(self):
        if os.path.exists(self.pid_file):
            os.unlink(self.pid_file)

    def __init__(self, args):
        self.args = args
        self.pid_file = EXPORTER_PIDFILE_NAME
        super().__init__(self.pid_file)

    def run(self):
        set_logger(self.args.__dict__['log.filepath'],
                   self.args.__dict__['log.level'])
        dao.set_prometheus_client(
            host=self.args.__dict__['prometheus_host'],
            port=self.args.__dict__['prometheus_port']
        )
        service.register_prometheus_metrics(
            rule_filepath=self.args.__dict__['collector.config']
        )

        check_ssl_file_permission(self.args.ssl_keyfile, self.args.ssl_certfile)
        check_ssl_certificate_remaining_days(self.args.ssl_certfile)

        controller.run(
            host=self.args.__dict__['web.listen_address'],
            port=self.args.__dict__['web.listen_port'],
            ssl_keyfile=self.args.ssl_keyfile,
            ssl_certfile=self.args.ssl_certfile,
            ssl_keyfile_password=self.args.keyfile_password
        )


def main(argv):
    args = parse_argv(argv)
    if is_exporter_alive(args.__dict__['web.listen_address'],
                         args.__dict__['web.listen_port'],
                         args.disable_https):
        write_to_terminal('Service has been started, exiting...', color='red')
    elif not is_prometheus_alive(args.prometheus_host, args.prometheus_port):
        write_to_terminal('Prometheus service is abnormal, please check for it, exiting...', color='red')
    else:
        ExporterMain(args).start()
