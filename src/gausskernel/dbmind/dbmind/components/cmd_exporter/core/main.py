# Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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

import yaml

from dbmind.common.daemon import Daemon
from dbmind.common.utils import write_to_terminal
from dbmind.common.utils.checking import path_type, CheckPort, CheckIP, positive_int_type
from dbmind.common.utils.checking import warn_ssl_certificate
from dbmind.common.utils.exporter import (
    is_exporter_alive, KVPairAction, set_logger, parse_and_adjust_args
)
from dbmind.constants import __version__
from . import controller
from . import service

ROOT_DIR_PATH = os.path.realpath(
    os.path.join(os.path.dirname(__file__), '..')
)
YAML_DIR_PATH = os.path.join(ROOT_DIR_PATH, 'yamls')
DEFAULT_YAML = 'default.yml'

DEFAULT_LOGFILE = 'dbmind_cmd_exporter.log'
with tempfile.NamedTemporaryFile(suffix='.pid') as fp_:
    EXPORTER_PIDFILE_NAME = fp_.name


def parse_argv(argv):
    parser = argparse.ArgumentParser(
        description='Command Exporter (DBMind): scrape metrics by performing shell commands.'
    )
    parser.add_argument('--constant-labels', default='', action=KVPairAction,
                        help='a list of label=value separated by comma(,).')
    parser.add_argument('--web.listen-address', default='127.0.0.1', action=CheckIP,
                        help='address on which to expose metrics and web interface')
    parser.add_argument('--web.listen-port', type=int, default=9180, action=CheckPort,
                        help='listen port to expose metrics and web interface')
    parser.add_argument('--disable-https', action='store_true',
                        help='disable Https scheme')
    parser.add_argument('--config', type=path_type, default=os.path.join(YAML_DIR_PATH, DEFAULT_YAML),
                        help='path to config dir or file.')
    parser.add_argument('--ssl-keyfile', type=path_type, help='set the path of ssl key file')
    parser.add_argument('--ssl-certfile', type=path_type, help='set the path of ssl certificate file')
    parser.add_argument('--ssl-ca-file', type=path_type, help='set the path of ssl ca file')
    parser.add_argument('--parallel', default=5, type=positive_int_type,
                        help='performing shell command in parallel.')
    parser.add_argument('--log.filepath', type=os.path.realpath,
                        default=os.path.join(os.getcwd(), DEFAULT_LOGFILE),
                        help='the path to log')
    parser.add_argument('--log.level', default='info', choices=('debug', 'info', 'warn', 'error', 'fatal'),
                        help='only log messages with the given severity or above.'
                             ' Valid levels: [debug, info, warn, error, fatal]')
    parser.add_argument('-v', '--version', action='version', version=__version__)

    return parse_and_adjust_args(parser, argv)


class ExporterMain(Daemon):
    def clean(self):
        pass

    def __init__(self, args):
        self.args = args
        self.pid_file = EXPORTER_PIDFILE_NAME
        super().__init__(self.pid_file)

    def handle_file_permission(self):
        if self.args.ssl_keyfile and os.path.exists(self.args.ssl_keyfile) and os.path.isfile(self.args.ssl_keyfile):
            os.chmod(self.args.ssl_keyfile, 0o400)
        if self.args.ssl_certfile and os.path.exists(self.args.ssl_certfile) and os.path.isfile(self.args.ssl_certfile):
            os.chmod(self.args.ssl_certfile, 0o400)
        if self.args.ssl_ca_file and os.path.exists(self.args.ssl_ca_file) and os.path.isfile(self.args.ssl_ca_file):
            os.chmod(self.args.ssl_ca_file, 0o400)
        if self.args.__dict__['log.filepath'] and os.path.exists(self.args.__dict__['log.filepath']) and os.path.isfile(
                self.args.__dict__['log.filepath']):
            os.chmod(self.args.__dict__['log.filepath'], 0o600)
        if os.path.exists(YAML_DIR_PATH):
            os.chmod(YAML_DIR_PATH, 0o700)
        if self.args.config and os.path.exists(self.args.config) and os.path.isfile(self.args.config):
            os.chmod(self.args.config, 0o600)
        if os.path.exists(os.path.join(YAML_DIR_PATH, DEFAULT_YAML)):
            os.chmod(os.path.join(YAML_DIR_PATH, DEFAULT_YAML), 0o600)
        if os.path.exists(self.pid_file):
            os.chmod(self.pid_file, 0o600)

    def run(self):
        set_logger(self.args.__dict__['log.filepath'],
                   self.args.__dict__['log.level'])
        self.handle_file_permission()
        service.config_collecting_params(
            parallel=self.args.parallel,
            constant_labels=self.args.constant_labels,
        )
        with open(self.args.config, errors='ignore') as fp:
            service.register_metrics(yaml.load(fp, Loader=yaml.FullLoader))

        warn_ssl_certificate(self.args.ssl_certfile, self.args.ssl_keyfile)
        controller.run(
            host=self.args.__dict__['web.listen_address'],
            port=self.args.__dict__['web.listen_port'],
            telemetry_path='/metrics',
            ssl_keyfile=self.args.ssl_keyfile,
            ssl_certfile=self.args.ssl_certfile,
            ssl_keyfile_password=self.args.keyfile_password,
            ssl_ca_file=self.args.ssl_ca_file
        )


def main(argv):
    args = parse_argv(argv)
    if is_exporter_alive(args.__dict__['web.listen_address'],
                         args.__dict__['web.listen_port']):
        write_to_terminal('Service has been started or the address already in use, exiting...', color='red')
    else:
        ExporterMain(args).start()
