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
"""DBMind common functionality interface"""

import logging
import os
import signal
import sys
import threading
import time
import traceback

from dbmind import app
from dbmind import constants
from dbmind import global_vars
from dbmind.cmd.config_utils import (
    load_sys_configs,
    DynamicConfig
)
from dbmind.common import platform
from dbmind.common import utils
from dbmind.common.daemon import Daemon, read_dbmind_pid_file
from dbmind.common.dispatcher import TimedTaskManager
from dbmind.common.dispatcher import get_worker_instance
from dbmind.common.exceptions import SetupError
from dbmind.common.rpc import RPCClient
from dbmind.common.tsdb import TsdbClientFactory

# Support input() function in using backspace.
try:
    import readline
except ImportError:
    pass

SKIP_LIST = ('COMMENT', 'LOG')

dbmind_master_should_exit = False


def _check_confpath(confpath):
    confile_path = os.path.join(confpath, constants.CONFILE_NAME)
    if os.path.exists(confile_path):
        return True
    return False


def _process_clean(force=False):
    # Wait for workers starting.
    while global_vars.worker is None:
        time.sleep(.1)
    global_vars.worker.terminate(cancel_futures=force)
    TimedTaskManager.stop()


def signal_handler(signum, frame):
    global dbmind_master_should_exit

    if signum == signal.SIGINT or signum == signal.SIGHUP:
        utils.cli.write_to_terminal('Reloading parameters.', color='green')
        global_vars.configs = load_sys_configs(constants.CONFILE_NAME)
    elif signum == signal.SIGUSR2:
        # used for debugging
        with open('traceback.stack', 'w+') as f:
            for th in threading.enumerate():
                f.write(str(th) + '\n')
                current_frames = getattr(sys, '_current_frames')()
                traceback.print_stack(current_frames[th.ident], file=f)
                f.write('\n')
    elif signum == signal.SIGTERM:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        logging.info('DBMind received exit signal.')
        utils.cli.write_to_terminal('Cleaning resources...')
        dbmind_master_should_exit = True
        _process_clean()
    elif signum == signal.SIGQUIT:
        # Force to exit and cancel future tasks.
        signal.signal(signal.SIGQUIT, signal.SIG_IGN)
        logging.info('DBMind received the signal: exit immediately.')
        utils.cli.write_to_terminal('Cleaning resources...')
        dbmind_master_should_exit = True
        _process_clean(force=True)


class DBMindMain(Daemon):
    def __init__(self, confpath):
        if not _check_confpath(confpath):
            raise SetupError("Invalid directory '%s', please set up first." % confpath)

        self.confpath = os.path.realpath(confpath)
        self.worker = None

        pid_file = os.path.join(confpath, constants.PIDFILE_NAME)
        super().__init__(pid_file)

    def run(self):
        os.chdir(self.confpath)
        os.umask(0o0077)

        mismatching_message = (
            "The version of the configuration file directory "
            "does not match the DBMind, "
            "please use the '... setup --initialize' command to reinitialize."
        )
        if not os.path.exists(constants.VERFILE_NAME):
            utils.raise_fatal_and_exit(
                mismatching_message, use_logging=False
            )
        with open(constants.VERFILE_NAME) as fp:
            if fp.readline().strip() != constants.__version__:
                utils.raise_fatal_and_exit(
                    mismatching_message, use_logging=False
                )

        utils.cli.set_proc_title('DBMind [Master Process]')
        # Set global variables.
        global_vars.confpath = self.confpath
        global_vars.configs = load_sys_configs(constants.CONFILE_NAME)
        global_vars.dynamic_configs = DynamicConfig
        global_vars.metric_map = utils.read_simple_config_file(
            constants.METRIC_MAP_CONFIG
        )
        global_vars.metric_value_range_map = utils.read_simple_config_file(constants.METRIC_VALUE_RANGE_CONFIG)
        global_vars.must_filter_labels = utils.read_simple_config_file(
            constants.MUST_FILTER_LABEL_CONFIG
        )

        # Set logger.
        os.makedirs('logs', exist_ok=True)
        max_bytes = global_vars.configs.getint('LOG', 'maxbytes')
        backup_count = global_vars.configs.getint('LOG', 'backupcount')
        logging_handler = utils.MultiProcessingRFHandler(filename=os.path.join('logs', constants.LOGFILE_NAME),
                                                         maxBytes=max_bytes,
                                                         backupCount=backup_count)
        logging_handler.setFormatter(
            logging.Formatter("[%(asctime)s %(levelname)s][%(process)d-%(thread)d][%(name)s]: %(message)s")
        )
        logger = logging.getLogger()
        logger.name = 'DBMind'
        logger.addHandler(logging_handler)
        logger.setLevel(global_vars.configs.get('LOG', 'level').upper())

        logging.info('DBMind is starting.')

        # Warn user of proxies if user set.
        if os.getenv('http_proxy') or os.getenv('https_proxy'):
            logging.warning('You set the proxy environment variable (e.g., http_proxy, https_proxy),'
                            ' which may cause network requests to be sent '
                            'through the proxy server, which may cause some network connectivity issues too.'
                            ' You need to make sure that the action is what you expect.')

        # Set the information for TSDB.
        TsdbClientFactory.set_client_info(
            global_vars.configs.get('TSDB', 'name'),
            global_vars.configs.get('TSDB', 'host'),
            global_vars.configs.get('TSDB', 'port'),
            global_vars.configs.get('TSDB', 'username'),
            global_vars.configs.get('TSDB', 'password'),
            global_vars.configs.get('TSDB', 'ssl_certfile'),
            global_vars.configs.get('TSDB', 'ssl_keyfile'),
            global_vars.configs.get('TSDB', 'ssl_keyfile_password'),
            global_vars.configs.get('TSDB', 'ssl_ca_file')
        )

        # Register signal handler.
        if not platform.WIN32:
            signal.signal(signal.SIGHUP, signal_handler)
            signal.signal(signal.SIGUSR2, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            signal.signal(signal.SIGQUIT, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

        # Initialize RPC components.
        master_url = global_vars.configs.get('AGENT', 'master_url')
        ssl_certfile = global_vars.configs.get('AGENT', 'ssl_certfile')
        ssl_keyfile = global_vars.configs.get('AGENT', 'ssl_keyfile')
        ssl_keyfile_password = global_vars.configs.get('AGENT', 'ssl_keyfile_password')
        ssl_ca_file = global_vars.configs.get('AGENT', 'ssl_ca_file')
        agent_username = global_vars.configs.get('AGENT', 'username')
        agent_pwd = global_vars.configs.get('AGENT', 'password')

        if agent_pwd.strip() != '':
            logging_handler.add_sensitive_word(agent_pwd)

        global_vars.agent_rpc_client = RPCClient(
            master_url,
            username=agent_username,
            pwd=agent_pwd,
            ssl_cert=ssl_certfile,
            ssl_key=ssl_keyfile,
            ssl_key_password=ssl_keyfile_password,
            ca_file=ssl_ca_file
        )
        # Create executor pool.
        local_workers = global_vars.configs.getint('WORKER', 'process_num', fallback=-1)
        global_vars.worker = self.worker = get_worker_instance('local', local_workers)

        # Start timed tasks.
        global_vars.self_driving_records = utils.NaiveQueue(20)
        app.register_timed_app()
        if global_vars.is_dry_run_mode:
            TimedTaskManager.run_once()
            return

        TimedTaskManager.start()

        # Main thread will block here under Python 3.7.
        while not dbmind_master_should_exit:
            time.sleep(1)
        logging.info('DBMind will close.')

    def clean(self):
        pass

    def reload(self):
        pid = read_dbmind_pid_file(self.pid_file)
        if pid > 0:
            if platform.WIN32:
                os.kill(pid, signal.SIGINT)
            else:
                os.kill(pid, signal.SIGHUP)
        else:
            utils.cli.write_to_terminal(
                'Invalid DBMind process.',
                level='error',
                color='red'
            )
            os.remove(self.pid_file)
