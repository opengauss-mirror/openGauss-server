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
import os
import tempfile

from dbmind.common.daemon import Daemon
from dbmind.common.utils import exporter
from dbmind.common.utils import write_to_terminal
from dbmind.common.utils.checking import (
    warn_ssl_certificate, CheckPort, CheckIP, path_type,
    positive_int_type
)
from dbmind.common.utils.exporter import (
    is_exporter_alive, set_logger, parse_and_adjust_args
)
from dbmind.constants import __version__
from . import controller
from . import dao
from . import service

CURR_DIR = os.path.realpath(
    os.path.join(os.path.dirname(__file__), '..')
)
DEFAULT_YAML = 'reprocessing_exporter.yml'
DEFAULT_LOGFILE = 'reprocessing_exporter.log'
with tempfile.NamedTemporaryFile(suffix='.pid') as fp_:
    EXPORTER_PIDFILE_NAME = fp_.name


def parse_argv(argv):
    parser = argparse.ArgumentParser(
        description='Reprocessing Exporter: A re-processing module for metrics stored in the Prometheus server.'
    )
    parser.add_argument('prometheus_host', help='from which host to pull data')
    parser.add_argument('prometheus_port', type=positive_int_type,
                        help='the port to connect to the Prometheus host')
    parser.add_argument('--disable-https', action='store_true',
                        help='disable Https scheme')
    parser.add_argument('--ssl-keyfile', type=path_type, help='set the path of ssl key file')
    parser.add_argument('--ssl-certfile', type=path_type, help='set the path of ssl certificate file')
    parser.add_argument('--ssl-ca-file', type=path_type, help='set the path of ssl ca file')
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
    parser.add_argument('-v', '--version', action='version', version=__version__)

    args = parse_and_adjust_args(parser, argv)
    alive, scheme = exporter.is_prometheus_alive(args.prometheus_host, args.prometheus_port)
    if not alive:
        parser.error(
            'Prometheus service is abnormal, please check for it, exiting...'
        )
        return

    setattr(args, 'prometheus_url', '{}://{}:{}'.format(
        scheme, args.prometheus_host, args.prometheus_port
    ))

    return args


class ExporterMain(Daemon):
    def clean(self):
        pass

    def __init__(self, args):
        self.args = args
        self.pid_file = EXPORTER_PIDFILE_NAME
        super().__init__(self.pid_file)

    def change_file_permissions(self):
        if (
                self.args.ssl_keyfile
                and os.path.exists(self.args.ssl_keyfile)
                and os.path.isfile(self.args.ssl_keyfile)
        ):
            os.chmod(self.args.ssl_keyfile, 0o400)
        if (
                self.args.ssl_certfile
                and os.path.exists(self.args.ssl_certfile)
                and os.path.isfile(self.args.ssl_certfile)
        ):
            os.chmod(self.args.ssl_certfile, 0o400)
        if (
                self.args.ssl_ca_file
                and os.path.exists(self.args.ssl_ca_file)
                and os.path.isfile(self.args.ssl_ca_file)
        ):
            os.chmod(self.args.ssl_ca_file, 0o400)
        if self.args.__dict__['log.filepath'] \
                and os.path.exists(self.args.__dict__['log.filepath']) \
                and os.path.isfile(
                self.args.__dict__['log.filepath']):
            os.chmod(self.args.__dict__['log.filepath'], 0o600)
        if os.path.exists(CURR_DIR):
            os.chmod(CURR_DIR, 0o700)
        if os.path.exists(os.path.join(CURR_DIR, DEFAULT_YAML)):
            os.chmod(os.path.join(CURR_DIR, DEFAULT_YAML), 0o600)
        if os.path.exists(self.args.__dict__['collector.config']) and os.path.isfile(
                self.args.__dict__['collector.config']):
            os.chmod(self.args.__dict__['collector.config'], 0o600)
        if os.path.exists(self.pid_file):
            os.chmod(self.pid_file, 0o600)

    def run(self):
        set_logger(self.args.__dict__['log.filepath'],
                   self.args.__dict__['log.level'])
        self.change_file_permissions()
        dao.set_prometheus_client(
            url=self.args.__dict__['prometheus_url']
        )
        service.register_prometheus_metrics(
            rule_filepath=self.args.__dict__['collector.config']
        )

        warn_ssl_certificate(self.args.ssl_certfile, self.args.ssl_keyfile)

        controller.run(
            host=self.args.__dict__['web.listen_address'],
            port=self.args.__dict__['web.listen_port'],
            ssl_keyfile=self.args.ssl_keyfile,
            ssl_certfile=self.args.ssl_certfile,
            ssl_keyfile_password=self.args.keyfile_password,
            ssl_ca_file=self.args.ssl_ca_file
        )


def main(argv):
    args = parse_argv(argv)
    if is_exporter_alive(args.__dict__['web.listen_address'],
                         args.__dict__['web.listen_port'],
                         ):
        write_to_terminal('Service has been started or the address already in use, exiting...', color='red')
        return
    ExporterMain(args).start()
