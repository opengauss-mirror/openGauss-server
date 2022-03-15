# Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
import logging
import os
import sys
import re
import tempfile
from logging.handlers import TimedRotatingFileHandler

import yaml

from dbmind.common.daemon import Daemon
from dbmind.common.utils import set_proc_title, check_ssl_certificate_remaining_days,\
    check_ssl_file_permission
from dbmind.common.utils import write_to_terminal
from . import controller
from . import service
from .. import __version__

ROOT_DIR_PATH = os.path.realpath(
    os.path.join(os.path.dirname(__file__), '..')
)

YAML_DIR_PATH = os.path.join(ROOT_DIR_PATH, 'yamls')
DEFAULT_YAML = 'default.yml'
PG_SETTINGS_YAML = 'pg_settings.yml'
STATEMENTS_YAML = 'statements.yml'

DEFAULT_LOGFILE = 'dbmind_opengauss_exporter.log'
with tempfile.NamedTemporaryFile(suffix='.pid') as fp:
    EXPORTER_PIDFILE_NAME = fp.name


class PairAction(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        d = dict()
        try:
            for pair in values.split(','):
                name, value = pair.split('=')
                d[name.strip()] = value.strip()
            setattr(args, self.dest, d)
        except ValueError:
            parser.error('Illegal constant labels: %s.' % values)


def wipe_off_password(dsn):
    result = re.findall(r'[^:]*://[^:]*:(.+)@[^@]*:[^:]*/[^:]*', dsn)
    if len(result) == 0:
        result = re.findall(r'password=(.*)\s', dsn)
        if len(result) == 0:
            return "*********"

    password = result[0]
    if len(password) == 0:
        return "*********"
    return dsn.replace(password, '******')


def path_type(path):
    if os.path.exists(path):
        return os.path.realpath(path)
    else:
        raise argparse.ArgumentTypeError('%s is not a valid path.' % path)


def parse_argv(argv):
    parser = argparse.ArgumentParser(
        description='openGauss Exporter (DBMind): Monitoring for openGauss.'
    )
    parser.add_argument('--url', required=True, help='openGauss database target url.')
    parser.add_argument('--config', type=path_type, default=os.path.join(YAML_DIR_PATH, DEFAULT_YAML),
                        help='path to config file.')
    parser.add_argument('--constant-labels', default='', action=PairAction,
                        help='a list of label=value separated by comma(,).')
    parser.add_argument('--web.listen-address', default='127.0.0.1',
                        help='address on which to expose metrics and web interface')
    parser.add_argument('--web.listen-port', type=int, default=9187,
                        help='listen port to expose metrics and web interface')
    parser.add_argument('--web.telemetry-path', default='/metrics',
                        help='path under which to expose metrics.')
    parser.add_argument('--disable-cache', action='store_true',
                        help='force not using cache.')
    parser.add_argument('--disable-settings-metrics', action='store_true',
                        help='not collect pg_settings.yml metrics.')
    parser.add_argument('--disable-statement-history-metrics', action='store_true',
                        help='not collect statement-history metrics (including slow queries).')
    parser.add_argument('--disable-https', action='store_true',
                        help='disable Https schema')
    parser.add_argument('--ssl-keyfile', type=path_type, help='set the path of ssl key file')
    parser.add_argument('--ssl-certfile', type=path_type, help='set the path of ssl certificate file')
    parser.add_argument('--parallel', default=5, type=int,
                        help='not collect pg_settings.yml metrics.')
    parser.add_argument('--log.filepath', type=os.path.realpath, default=os.path.join(os.getcwd(), DEFAULT_LOGFILE),
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

    def __init__(self, argv):
        self.args = parse_argv(argv)
        self.pid_file = EXPORTER_PIDFILE_NAME
        super().__init__(self.pid_file)

    def run(self):
        # Wipe off password of url for the process title.
        try:
            url = self.args.url
            wiped_url = wipe_off_password(url)
            with open('/proc/self/cmdline') as fp:
                cmdline = fp.readline().replace('\x00', ' ')
            wiped_cmdline = cmdline.replace(url, wiped_url)
            set_proc_title(wiped_cmdline)
        except FileNotFoundError:
            # ignore
            pass

        set_logger(self.args.__dict__['log.filepath'],
                   self.args.__dict__['log.level'])
        try:
            service.config_collecting_params(
                url=self.args.url,
                parallel=self.args.parallel,
                disable_cache=self.args.disable_cache,
                constant_labels=self.args.constant_labels,
            )
        except ConnectionError:
            write_to_terminal('Failed to connect to the url, exiting...', color='red')
            sys.exit(1)
        if not self.args.disable_settings_metrics:
            with open(os.path.join(YAML_DIR_PATH, PG_SETTINGS_YAML), errors='ignore') as fp:
                service.register_metrics(yaml.load(fp, Loader=yaml.FullLoader))
        if not self.args.disable_statement_history_metrics:
            with open(os.path.join(YAML_DIR_PATH, STATEMENTS_YAML), errors='ignore') as fp:
                service.register_metrics(
                    yaml.load(fp, Loader=yaml.FullLoader),
                    force_connection_db='postgres'
                )
        with open(self.args.config, errors='ignore') as fp:
            service.register_metrics(yaml.load(fp, Loader=yaml.FullLoader))

        check_ssl_file_permission(self.args.ssl_keyfile, self.args.ssl_certfile)
        check_ssl_certificate_remaining_days(self.args.ssl_certfile)

        controller.run(
            host=self.args.__dict__['web.listen_address'],
            port=self.args.__dict__['web.listen_port'],
            telemetry_path=self.args.__dict__['web.telemetry_path'],
            ssl_keyfile=self.args.ssl_keyfile,
            ssl_certfile=self.args.ssl_certfile,
            ssl_keyfile_password=self.args.keyfile_password
        )


def main(argv):
    ExporterMain(argv).start()
