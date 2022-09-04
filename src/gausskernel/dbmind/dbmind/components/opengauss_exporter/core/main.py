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
import os
import sys
import tempfile

import yaml

from dbmind.common.daemon import Daemon
from dbmind.common.utils import write_to_terminal
from dbmind.common.utils.checking import (
    warn_ssl_certificate, CheckPort, CheckIP, CheckDSN, path_type, positive_int_type
)
from dbmind.common.utils.cli import wipe_off_password_from_proc_title
from dbmind.common.utils.exporter import KVPairAction, ListPairAction
from dbmind.common.utils.exporter import is_exporter_alive, set_logger, parse_and_adjust_args
from dbmind.constants import __version__
from . import controller
from . import service
from .agent import create_agent_rpc_service

ROOT_DIR_PATH = os.path.realpath(
    os.path.join(os.path.dirname(__file__), '..')
)
YAML_DIR_PATH = os.path.join(ROOT_DIR_PATH, 'yamls')
COMING_FROM_EACH_DATABASE = 'coming_from_each_database.yml'
DEFAULT_YAML = 'default.yml'
PG_SETTINGS_YAML = 'pg_settings.yml'
STATEMENTS_YAML = 'statements.yml'

DEFAULT_LOGFILE = 'dbmind_opengauss_exporter.log'
with tempfile.NamedTemporaryFile(suffix='.pid') as fp_:
    EXPORTER_PIDFILE_NAME = fp_.name


def parse_argv(argv):
    parser = argparse.ArgumentParser(
        description='openGauss Exporter (DBMind): Monitoring or controlling for openGauss.'
    )
    parser.add_argument('--url', required=True,
                        help='openGauss database target url. '
                             'It is recommended to connect to the postgres '
                             'database through this URL, so that the exporter '
                             'can actively discover and monitor other databases.',
                        action=CheckDSN)
    parser.add_argument('--config-file', '--config', type=path_type, default=os.path.join(YAML_DIR_PATH, DEFAULT_YAML),
                        help='path to config file.')
    parser.add_argument('--include-databases', action=ListPairAction, default='',
                        help='only scrape metrics from the given database list.'
                             ' a list of database name (format is label=dbname or dbname) separated by comma(,).')
    parser.add_argument('--exclude-databases', action=ListPairAction, default='',
                        help='scrape metrics from the all auto-discovered databases'
                             ' excluding the list of database.'
                             ' a list of database name (format is label=dbname or dbname) separated by comma(,).')
    parser.add_argument('--constant-labels', default='', action=KVPairAction,
                        help='a list of label=value separated by comma(,).')
    parser.add_argument('--web.listen-address', default='127.0.0.1', action=CheckIP,
                        help='address on which to expose metrics and web interface')
    parser.add_argument('--web.listen-port', type=int, default=9187, action=CheckPort,
                        help='listen port to expose metrics and web interface')
    parser.add_argument('--disable-cache', action='store_true',
                        help='force not using cache.')
    parser.add_argument('--disable-settings-metrics', action='store_true',
                        help='not collect pg_settings.yml metrics.')
    parser.add_argument('--disable-statement-history-metrics', action='store_true',
                        help='not collect statement-history metrics (including slow queries).')
    parser.add_argument('--disable-https', action='store_true',
                        help='disable Https scheme')
    parser.add_argument('--disable-agent', action='store_true',
                        help='by default, this exporter also assumes the role of DBMind-Agent, that is, executing '
                             'database operation and maintenance actions issued by the DBMind service. With this '
                             'argument, users can disable the agent functionality, thereby prohibiting the DBMind '
                             'service from making changes to the database.')
    parser.add_argument('--ssl-keyfile', type=path_type, help='set the path of ssl key file')
    parser.add_argument('--ssl-certfile', type=path_type, help='set the path of ssl certificate file')
    parser.add_argument('--ssl-ca-file', type=path_type, help='set the path of ssl ca file')
    parser.add_argument('--parallel', default=5, type=positive_int_type,
                        help='number of parallels for metrics scrape.')
    parser.add_argument('--log.filepath', type=os.path.realpath,
                        default=os.path.join(os.getcwd(), DEFAULT_LOGFILE),
                        help='the path to log')
    parser.add_argument('--log.level', default='info', choices=('debug', 'info', 'warn', 'error', 'fatal'),
                        help='only log messages with the given severity or above.'
                             ' Valid levels: [debug, info, warn, error, fatal]')
    parser.add_argument('-v', '--version', action='version', version=__version__)

    args = parser.parse_args(argv)

    both_set_database = set(args.include_databases).intersection(args.exclude_databases)
    if both_set_database:
        parser.error('Not allowed to set the same database %s '
                     'in the argument --include-databases and '
                     '--exclude-database at the same time.' % both_set_database)

    return parse_and_adjust_args(parser, argv)


class ExporterMain(Daemon):
    def clean(self):
        pass

    def __init__(self, args):
        self.args = args
        self.pid_file = EXPORTER_PIDFILE_NAME
        super().__init__(self.pid_file)

    def change_file_permissions(self):
        if self.args.ssl_keyfile and os.path.exists(self.args.ssl_keyfile) and os.path.isfile(self.args.ssl_keyfile):
            os.chmod(self.args.ssl_keyfile, 0o400)
        if self.args.ssl_certfile and os.path.exists(self.args.ssl_certfile) and os.path.isfile(self.args.ssl_certfile):
            os.chmod(self.args.ssl_certfile, 0o400)
        if self.args.ssl_ca_file and os.path.exists(self.args.ssl_ca_file) and os.path.isfile(self.args.ssl_ca_file):
            os.chmod(self.args.ssl_ca_file, 0o400)
        if self.args.__dict__['log.filepath'] and os.path.exists(self.args.__dict__['log.filepath']) and os.path.isfile(
                self.args.__dict__['log.filepath']):
            os.chmod(self.args.__dict__['log.filepath'], 0o600)
        if self.args.config_file and os.path.exists(self.args.config_file) and os.path.isfile(self.args.config_file):
            os.chmod(self.args.config_file, 0o600)
        if os.path.exists(self.pid_file):
            os.chmod(self.pid_file, 0o600)
        if os.path.exists(YAML_DIR_PATH):
            os.chmod(YAML_DIR_PATH, 0o700)
        if os.path.exists(os.path.join(YAML_DIR_PATH, PG_SETTINGS_YAML)):
            os.chmod(os.path.join(YAML_DIR_PATH, PG_SETTINGS_YAML), 0o600)
        if os.path.exists(os.path.join(YAML_DIR_PATH, DEFAULT_YAML)):
            os.chmod(os.path.join(YAML_DIR_PATH, DEFAULT_YAML), 0o600)
        if os.path.exists(os.path.join(YAML_DIR_PATH, COMING_FROM_EACH_DATABASE)):
            os.chmod(os.path.join(YAML_DIR_PATH, COMING_FROM_EACH_DATABASE), 0o600)
        if os.path.exists(os.path.join(YAML_DIR_PATH, STATEMENTS_YAML)):
            os.chmod(os.path.join(YAML_DIR_PATH, STATEMENTS_YAML), 0o600)

    def run(self):
        # Wipe off password of url for the process title.
        try:
            url = self.args.url
            wipe_off_password_from_proc_title(url)
        except FileNotFoundError:
            # ignore
            pass
        set_logger(self.args.__dict__['log.filepath'],
                   self.args.__dict__['log.level'])
        self.change_file_permissions()
        try:
            service.config_collecting_params(
                url=self.args.url,
                include_databases=self.args.include_databases,
                exclude_databases=self.args.exclude_databases,
                parallel=self.args.parallel,
                disable_cache=self.args.disable_cache,
                constant_labels=self.args.constant_labels,
            )
        except ConnectionError:
            write_to_terminal('Failed to connect to the url, exiting...', color='red')
            sys.exit(1)
        except Exception as e:
            write_to_terminal('Failed to connect to the url due to exception, exiting...', color='red')
            raise e
        if not self.args.disable_settings_metrics:
            with open(os.path.join(YAML_DIR_PATH, PG_SETTINGS_YAML), errors='ignore') as fp:
                service.register_metrics(
                    yaml.load(fp, Loader=yaml.FullLoader),
                    force_connection_db='postgres'
                )
        if not self.args.disable_statement_history_metrics:
            with open(os.path.join(YAML_DIR_PATH, STATEMENTS_YAML), errors='ignore') as fp:
                service.register_metrics(
                    yaml.load(fp, Loader=yaml.FullLoader),
                    force_connection_db='postgres'
                )
        # Metrics in the following config file need to
        # be scraped from each discovered database.
        with open(os.path.join(YAML_DIR_PATH, COMING_FROM_EACH_DATABASE), errors='ignore') as fp:
            service.register_metrics(yaml.load(fp, Loader=yaml.FullLoader))
        # It is enough to connect to the given database and scrape metrics for the following config.
        with open(self.args.config_file, errors='ignore') as fp:
            service.register_metrics(
                yaml.load(fp, Loader=yaml.FullLoader),
                force_connection_db=service.driver.main_dbname
            )

        # Reuse Http service to serve RPC.
        if not self.args.disable_agent:
            rpc = create_agent_rpc_service()
            controller.bind_rpc_service(rpc)

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
