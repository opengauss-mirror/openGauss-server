#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c): 2012-2021, Huawei Tech. Co., Ltd.
# FileName     : uninstall_remote.py
# Version      : V1.0.0
# Date         : 2021-03-01
# Description  : Remote uninstall
#############################################################################

import os
import re
import sys

sys.path.append(sys.path[0] + "/../../")
from config_cabin.config import PROJECT_PATH
from tools.common_tools import CommonTools
from tools.global_box import g
from definitions.constants import Constant
from module.anomaly_detection.uninstall import Uninstaller
from config_cabin.config import EXTRACT_DIR


class RemoteUninstaller(object):
    def __init__(self, **param_dict):
        self.param_dict = param_dict
        self.project_path = PROJECT_PATH
        self.package_path = self.param_dict.get(Constant.PACKAGE_PATH)
        self.module_name = self.param_dict.get(Constant.MODULE)
        self.agent_nodes = None
        self.module_path = None
        self.install_path = None

    def init_globals(self):
        _, _, self.install_path = Uninstaller.get_install_info()
        self.agent_nodes = self.param_dict.pop(Constant.AGENT_NODES)
        self.module_name = Constant.AI_SERVER if \
            self.module_name == Constant.MODULE_ANOMALY_DETECTION else self.module_name
        self.module_path = os.path.realpath(os.path.join(self.install_path, self.module_name))

    def prepare_param_file(self):
        """
        Record params to file for remote opt.
        """
        param_file_path = os.path.join(EXTRACT_DIR, Constant.TEMP_ANOMALY_PARAM_FILE)
        CommonTools.dict_to_json_file(self.param_dict, param_file_path)
        self._copy_param_file_to_remote_node(param_file_path, param_file_path)

    def _copy_param_file_to_remote_node(self, path_from, path_to):
        """
        Copy params file to remote node.
        """
        for node in self.agent_nodes:
            ip = node.get(Constant.NODE_IP)
            uname = node.get(Constant.NODE_USER)
            pwd = node.get(Constant.NODE_PWD)
            status, output = CommonTools.remote_copy_files(
                ip, uname, pwd, path_from, path_to)
            g.logger.info('Copy uninstall param file to node[%s]-output:%s\n.' % (ip, output))

    @staticmethod
    def remote_uninstall_each_node(params):
        """
        Remote install one agent node
        """
        ip, uname, pwd, cmd, ignore = params
        g.logger.info('Starting uninstall on node:[%s], please wait ...' % ip)
        status, output = CommonTools.remote_execute_cmd(ip, uname, pwd, cmd)
        if status != 0 and ignore:
            return
        g.logger.info('Uninstall result on node:[%s]-output:%s\n' % (ip, output))

    def remote_uninstall(self, ignore=False):
        """
        Remote install agent with install.py
        """
        uninstall_script_path = os.path.join(self.install_path,
                                             Constant.ANOMALY_DETECTION_UNINSTALL_SCRIPT_PATH)
        param_file_path = os.path.join(EXTRACT_DIR, Constant.TEMP_ANOMALY_PARAM_FILE)
        cmd = Constant.EXECUTE_REMOTE_SCRIPT % (
            Constant.CMD_PREFIX, uninstall_script_path, param_file_path)
        params = [(node.get(Constant.NODE_IP), node.get(Constant.NODE_USER),
                   node.get(Constant.NODE_PWD), cmd, ignore) for node in self.agent_nodes]
        CommonTools.parallel_execute(self.remote_uninstall_each_node, params)

    def clean_local_module_path(self):
        """
        Delete files in install path.
        """
        status, output = CommonTools.clean_dir(self.module_path)
        if status != 0:
            g.logger.warning('Failed clean path:[%s]' % self.module_path)
        else:
            g.logger.info('Successfully clean install path:[%s]' % self.module_path)

    def run(self):
        self.init_globals()
        self.prepare_param_file()
        self.remote_uninstall()
        self.clean_local_module_path()


