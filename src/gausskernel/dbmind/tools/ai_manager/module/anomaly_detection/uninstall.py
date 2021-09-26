#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c): 2012-2021, Huawei Tech. Co., Ltd.
# FileName     : uninstall.py
# Version      : V1.0.0
# Date         : 2021-03-01
# Description  : uninstall
#############################################################################

import optparse
import os
import sys
import subprocess

sys.path.append(sys.path[0] + "/../../")
from config_cabin.config import PROJECT_PATH
from config_cabin.config import VERSION_RECORD_FILE_ANOMALY_DETECTION
from tools.common_tools import CommonTools
from definitions.errors import Errors
from tools.global_box import g
from definitions.constants import Constant


class Uninstaller(object):
    def __init__(self, **param_dict):
        self.param_dict = param_dict
        self.project_path = PROJECT_PATH
        self.module_name = self.param_dict.get(Constant.MODULE)
        self.install_path = None
        self.install_version = None
        self.install_time = None
        self.stopping_list = None
        self.module_path = None
        self.service_list = None

    def init_globals(self):
        self.install_time, self.install_version, self.install_path = self.get_install_info()
        self.stopping_list = self.param_dict.get('stopping_list')
        self.service_list = self.param_dict.get('service_list')
        self.module_name = Constant.AI_SERVER if \
            self.module_name == Constant.MODULE_ANOMALY_DETECTION else self.module_name
        self.module_path = os.path.realpath(os.path.join(self.install_path, self.module_name))

    def del_agent_server_monitor_cron(self):
        """
        Delete cron service of agent, server and monitor.
        """
        g.logger.info('Start uninstall package[version:%s][installed time:%s][installed path:%s]'
                      % (self.install_version, self.install_time, self.install_path))
        script_path = os.path.realpath(
            os.path.join(self.install_path, Constant.ANORMALY_MAIN_SCRIPT))
        for cmd in self.service_list:
            cron_cmd = cmd % (Constant.CMD_PREFIX, os.path.dirname(script_path), script_path)
            status, output = CommonTools.del_cron(self.install_path, cron_cmd, '1m')
            g.logger.info('Delete crontab CMD[%s]-STATUS[%s]-OUTPUT[%s]' % (
                cron_cmd, status, output))

    def stop_agent_server_monitor(self):
        """
        Stop process of agent, server and monitor, if failed, try to kill it.
        """
        script_path = os.path.realpath(
            os.path.join(self.install_path, Constant.ANORMALY_MAIN_SCRIPT))
        if not os.path.isfile(script_path):
            raise Exception(Errors.FILE_DIR_PATH['gauss_0102'] %
                            script_path + 'Please confirm your files are integrated')
        for cmd in self.stopping_list:
            stop_cmd = cmd % (Constant.CMD_PREFIX, os.path.dirname(script_path), script_path)
            status, output = subprocess.getstatusoutput(stop_cmd)
            if status != 0:
                target = stop_cmd.replace('stop', 'start').split(self.install_version)[-1]
                g.logger.warning('Failed stop process,command[%s],error[%s]' % (stop_cmd, output))
                stat, out = CommonTools.grep_process_and_kill(target)
                if stat != 0:
                    g.logger.error('Failed kill process [%s] '
                                   'with error [%s], please manually stop it.' % (target, out))
                else:
                    g.logger.info('Successfully kill process [%s].' % target)
            else:
                g.logger.info('Successfully stop process with command:[%s]' % stop_cmd)

    def clean_module_path(self):
        """
        Delete files in install path.
        """
        status, output = CommonTools.clean_dir(self.module_path)
        if status != 0:
            g.logger.warning('Failed clean path:[%s]' % self.module_path)
        else:
            g.logger.info('Successfully clean install path:[%s]' % self.module_path)

    @staticmethod
    def get_install_info():
        """
        Get installed information from record file.
        install time | install version | install path
        """
        install_time, install_version, install_path = '', '', ''
        if not os.path.isfile(VERSION_RECORD_FILE_ANOMALY_DETECTION):
            raise Exception(
                Errors.FILE_DIR_PATH['gauss_0102'] % VERSION_RECORD_FILE_ANOMALY_DETECTION)
        install_info = CommonTools.read_last_line_from_file(
            VERSION_RECORD_FILE_ANOMALY_DETECTION).strip()
        if install_info:
            install_time, install_version, install_path = install_info.split('|')
            # check path valid
            CommonTools.check_path_valid(install_path)
        if not os.path.isdir(install_path):
            raise Exception(Errors.FILE_DIR_PATH['gauss_0103'] % install_path)
        else:
            g.logger.info('Successfully got install path[%s].' % install_path)
            return install_time, install_version, install_path

    def run(self, remote=False):
        self.init_globals()
        self.del_agent_server_monitor_cron()
        self.stop_agent_server_monitor()
        if remote:
            self.clean_module_path()


def init_parser():
    """parser command"""
    parser = optparse.OptionParser(conflict_handler='resolve')
    parser.disable_interspersed_args()
    parser.add_option('--param_file', dest='param_file', help='json file path')
    return parser


if __name__ == '__main__':
    try:
        install_parser = init_parser()
        opt, arg = install_parser.parse_args()
        params_file = opt.param_file
        params_dict = CommonTools.json_file_to_dict(params_file)
        uninstaller = Uninstaller(**params_dict)
        uninstaller.run(remote=True)
    except Exception as error:
        g.logger.error(Errors.EXECUTE_RESULT['gauss_0410'] % error)
        raise Exception(Errors.EXECUTE_RESULT['gauss_0410'] % error)


