#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c): 2012-2021, Huawei Tech. Co., Ltd.
# FileName     : uninstall.py
# Version      : V1.0.0
# Date         : 2021-03-01
# Description  :  Module index advisor uninstall script
#############################################################################

import os

from definitions.constants import Constant
from config_cabin.config import PROJECT_PATH
from tools.common_tools import CommonTools
from definitions.errors import Errors
from tools.global_box import g
from config_cabin.config import VERSION_RECORD_FILE_INDEX_ADVISOR


class UnInstaller(object):
    def __init__(self, **param_dict):
        self.param_dict = param_dict
        self.project_path = PROJECT_PATH
        self.install_nodes = self.param_dict.pop(Constant.INSTALL_NODES)
        self.module_name = self.param_dict.get(Constant.MODULE)
        self.install_path = None
        self.module_path = None

    def init_globals(self):
        install_time, install_version, self.install_path = self._get_install_info()
        self.module_path = os.path.realpath(os.path.join(self.install_path, self.module_name))

    @staticmethod
    def _get_install_info():
        """
        Get installed information from record file.
        install time | install version | install path
        """
        install_time, install_version, install_path = '', '', ''
        if not os.path.isfile(VERSION_RECORD_FILE_INDEX_ADVISOR):
            raise Exception(
                Errors.FILE_DIR_PATH['gauss_0102'] % VERSION_RECORD_FILE_INDEX_ADVISOR)
        install_info = CommonTools.read_last_line_from_file(
            VERSION_RECORD_FILE_INDEX_ADVISOR).strip()
        if install_info:
            install_time, install_version, install_path = install_info.split('|')
            # check path valid
            CommonTools.check_path_valid(install_path)
        if not os.path.isdir(install_path):
            raise Exception(Errors.FILE_DIR_PATH['gauss_0103'] % install_path)
        else:
            g.logger.info('Successfully got index advisor install path[%s].' % install_path)
            return install_time, install_version, install_path

    def clean_remote_module_dir(self):
        """
        Clean install path before unpack.
        """
        for node in self.install_nodes:
            ip = node.get(Constant.NODE_IP)
            uname = node.get(Constant.NODE_USER)
            pwd = node.get(Constant.NODE_PWD)
            _, output = CommonTools.retry_remote_clean_dir(self.module_path, ip, uname, pwd)
            g.logger.info('Result of clean module path on node:[%s], output:%s' % (ip, output))

    def run(self):
        self.init_globals()
        self.clean_remote_module_dir()


