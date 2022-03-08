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
import threading
import signal
import sys
import traceback
from logging import handlers
import time

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
    global_vars.worker.terminate(cancel_futures=force)
    TimedTaskManager.stop()


def signal_handler(signum, frame):
    global dbmind_master_should_exit

    if signum == signal.SIGINT or signum == signal.SIGHUP:
        utils.write_to_terminal('Reloading parameters.', color='green')
        global_vars.configs = load_sys_configs(constants.CONFILE_NAME)
    elif signum == signal.SIGUSR2:
        # used for debugging
        utils.write_to_terminal('Stack frames:', color='green')
        for th in threading.enumerate():
            print(th)
            traceback.print_stack(sys._current_frames()[th.ident])
            print()
    elif signum == signal.SIGTERM:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        logging.info('DBMind received exit signal.')
        utils.write_to_terminal('Cleaning opened resources...')
        _process_clean()
        dbmind_master_should_exit = True
    elif signum == signal.SIGQUIT:
        signal.signal(signal.SIGQUIT, signal.SIG_IGN)
        logging.info('DBMind received the signal: exit immediately.')
        utils.write_to_terminal('Cleaning opened resources...')
        _process_clean(True)
        dbmind_master_should_exit = True


class DBMindMain(Daemon):
    def __init__(self, confpath):
        if not _check_confpath(confpath):
            raise SetupError("Invalid directory '%s', please set up first." % confpath)

        self.confpath = os.path.abspath(confpath)
        self.worker = None

        pid_file = os.path.join(confpath, constants.PIDFILE_NAME)
        super().__init__(pid_file)

    def run(self):
        os.chdir(self.confpath)
        os.umask(0o0077)

        utils.set_proc_title('DBMind [Master Process]')
        # Set global variables.
        global_vars.confpath = self.confpath
        global_vars.configs = load_sys_configs(constants.CONFILE_NAME)
        global_vars.dynamic_configs = DynamicConfig
        global_vars.metric_map = utils.read_simple_config_file(
            constants.METRIC_MAP_CONFIG
        )
        global_vars.must_filter_labels = utils.read_simple_config_file(
            constants.MUST_FILTER_LABEL_CONFIG
        )

        # Set logger.
        os.makedirs('logs', exist_ok=True)
        max_bytes = global_vars.configs.getint('LOG', 'maxbytes')
        backup_count = global_vars.configs.getint('LOG', 'backupcount')
        disk_handler = utils.MultiProcessingRFHandler(filename=os.path.join('logs', constants.LOGFILE_NAME),
                                                      maxBytes=max_bytes,
                                                      backupCount=backup_count)

        disk_handler.setFormatter(
            logging.Formatter("[%(asctime)s %(levelname)s][%(process)d-%(thread)d][%(name)s]: %(message)s")
        )
        logger = logging.getLogger()
        logger.name = 'DBMind'
        logger.addHandler(disk_handler)
        logger.setLevel(global_vars.configs.get('LOG', 'level').upper())

        logging.info('DBMind is starting.')
        # Register signal handler.
        if not platform.WIN32:
            signal.signal(signal.SIGHUP, signal_handler)
            signal.signal(signal.SIGUSR2, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            signal.signal(signal.SIGQUIT, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

        # Create execution pool.
        global_vars.worker = self.worker = get_worker_instance('local', 0)
        # Start timed tasks.
        app.register_timed_app()
        TimedTaskManager.start()

        # Pending until all threads exist.
        while not dbmind_master_should_exit:
            time.sleep(1)
        logging.info('DBMind will close.')

    def clean(self):
        if os.path.exists(self.pid_file):
            os.unlink(self.pid_file)

    def reload(self):
        pid = read_dbmind_pid_file(self.pid_file)
        if pid > 0:
            if platform.WIN32:
                os.kill(pid, signal.SIGINT)
            else:
                os.kill(pid, signal.SIGHUP)
        else:
            utils.write_to_terminal(
                'Invalid DBMind process.',
                level='error',
                color='red'
            )
            os.remove(self.pid_file)
