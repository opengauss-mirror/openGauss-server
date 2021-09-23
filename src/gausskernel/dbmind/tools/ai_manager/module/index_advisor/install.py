#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c): 2012-2021, Huawei Tech. Co., Ltd.
# FileName     : install.py
# Version      : V1.0.0
# Date         : 2021-03-01
# Description  :  Module index advisor install script
#############################################################################

import os
from datetime import datetime

from definitions.constants import Constant
from config_cabin.config import PROJECT_PATH
from tools.common_tools import CommonTools
from definitions.errors import Errors
from config_cabin.config import EXTRACT_DIR
from tools.global_box import g
from config_cabin.config import VERSION_RECORD_FILE_INDEX_ADVISOR


class Installer(object):
    def __init__(self, **param_dict):
        self.param_dict = param_dict
        self.project_path = PROJECT_PATH
        self.install_nodes = self.param_dict.pop(Constant.INSTALL_NODES)
        self.module_name = self.param_dict.get(Constant.MODULE)
        self.package_path = None
        self.install_path = None
        self.version = None
        self.module_path = None

    def init_globals(self):
        self.package_path = self.param_dict.get('package_path')
        self.install_path = self.param_dict.get('install_path')
        self.version = self.param_dict.get('version')
        self.module_path = os.path.realpath(os.path.join(self.install_path, self.module_name))

    def prepare_remote_package_path(self):
        """
        Prepare install path
        """
        self._mk_remote_module_dir()
        self._clean_remote_module_dir()

    def _mk_remote_module_dir(self):
        """
        Create install path if the path is not exist.
        """
        for node in self.install_nodes:
            ip = node.get(Constant.NODE_IP)
            uname = node.get(Constant.NODE_USER)
            pwd = node.get(Constant.NODE_PWD)
            _, output = CommonTools.remote_mkdir_with_mode(
                os.path.dirname(self.module_path), Constant.AUTH_COMMON_DIR_STR, ip, uname, pwd)
            g.logger.info('Result of create module path dir on node:[%s], output:%s' % (
                ip, output))

    def _clean_remote_module_dir(self):
        """
        Clean install path before unpack.
        """
        for node in self.install_nodes:
            ip = node.get(Constant.NODE_IP)
            uname = node.get(Constant.NODE_USER)
            pwd = node.get(Constant.NODE_PWD)
            _, output = CommonTools.retry_remote_clean_dir(self.module_path, ip, uname, pwd)
            g.logger.info('Result of clean module path on node:[%s], output:%s' % (ip, output))

    def remote_copy_module(self):
        """
        Copy package of index advisor to remote node.
        """
        for node in self.install_nodes:
            ip = node.get(Constant.NODE_IP)
            uname = node.get(Constant.NODE_USER)
            pwd = node.get(Constant.NODE_PWD)
            if not any([ip, uname, pwd]):
                raise Exception(Errors.PARAMETER['gauss_0201'] % 'remote node info')
            local_module_path = os.path.realpath(os.path.join(EXTRACT_DIR, self.module_name))
            if not os.path.exists(local_module_path):
                raise Exception(Errors.FILE_DIR_PATH['gauss_0101'] % 'temp index advisor module')
            _, output = CommonTools.remote_copy_files(
                ip, uname, pwd, local_module_path, self.module_path)
            g.logger.info('Result of copy index advisor package to node[%s], output:%s' % (
                ip, output))

    def remote_copy_version_file(self):
        """
        Copy version record file of index advisor to remote node.
        """
        for node in self.install_nodes:
            ip = node.get(Constant.NODE_IP)
            uname = node.get(Constant.NODE_USER)
            pwd = node.get(Constant.NODE_PWD)
            if not any([ip, uname, pwd]):
                raise Exception(Errors.PARAMETER['gauss_0201'] % 'remote node info')
            if not os.path.exists(VERSION_RECORD_FILE_INDEX_ADVISOR):
                raise Exception(
                    Errors.FILE_DIR_PATH['gauss_0102'] % VERSION_RECORD_FILE_INDEX_ADVISOR)
            _, output = CommonTools.remote_copy_files(
                ip, uname, pwd, VERSION_RECORD_FILE_INDEX_ADVISOR, VERSION_RECORD_FILE_INDEX_ADVISOR)
            g.logger.info('Result of copy version record file to node[%s], output:%s' % (
                ip, output))

    def record_version_info(self):
        """
        Record install time, version, install path in record file.
        """
        time_install = datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
        content = '|'.join([time_install, self.version, self.install_path]) + '\n'
        CommonTools.add_content_to_file(VERSION_RECORD_FILE_INDEX_ADVISOR, content)
        CommonTools.delete_early_record(
            VERSION_RECORD_FILE_INDEX_ADVISOR, Constant.VERSION_FILE_MAX_LINES)
        g.logger.info('Successfully record version information.')

    def run(self):
        self.init_globals()
        self.prepare_remote_package_path()
        self.remote_copy_module()
        self.record_version_info()
        self.remote_copy_version_file()

