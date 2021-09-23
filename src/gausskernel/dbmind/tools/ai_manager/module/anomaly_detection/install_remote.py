#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c): 2012-2021, Huawei Tech. Co., Ltd.
# FileName     : install_remote.py
# Version      : V1.0.0
# Date         : 2021-03-01
# Description  : Remote install
#############################################################################

import os
import re
import sys

sys.path.append(sys.path[0] + "/../../")
from tools.common_tools import CommonTools
from definitions.constants import Constant
from definitions.errors import Errors
from config_cabin.config import PROJECT_PATH
from tools.global_box import g
from config_cabin.config import ENV_FILE
from config_cabin.config import EXTRACT_DIR
from module.anomaly_detection.uninstall_remote import RemoteUninstaller
from copy import deepcopy


class RemoteInstaller(object):
    def __init__(self, **param_dict):
        self.param_dict = param_dict
        self.project_path = PROJECT_PATH
        self.agent_nodes = self.param_dict.pop(Constant.AGENT_NODES)
        self.module_name = self.param_dict.get(Constant.MODULE)
        self.package_path = None
        self.install_path = None
        self.version = None
        self.service_list = None
        self.module_path = None
        self.manager_path = None

    def init_globals(self):
        self.package_path = self.param_dict.get('package_path')
        self.install_path = self.param_dict.get('install_path')
        self.version = self.param_dict.get('version')
        self.service_list = self.param_dict.get('service_list')
        self.module_name = Constant.AI_SERVER if \
            self.module_name == Constant.MODULE_ANOMALY_DETECTION else self.module_name
        self.module_path = os.path.realpath(os.path.join(self.install_path, self.module_name))
        self.manager_path = os.path.join(self.install_path, Constant.AI_MANAGER_PATH)

    def prepare_remote_file_path(self):
        """
        Prepare install path
        """
        self._clean_envs()
        self._mk_remote_module_dir()
        self._clean_remote_module_dir()
        self._mk_remote_manager_dir()
        self._clean_remote_manager_dir()

    def remote_copy_module(self):
        """
        Copy package to remote node.
        """
        for node in self.agent_nodes:
            ip = node.get(Constant.NODE_IP)
            uname = node.get(Constant.NODE_USER)
            pwd = node.get(Constant.NODE_PWD)

            status, output = CommonTools.remote_copy_files(
                ip, uname, pwd, self.module_path, self.module_path)
            if status != 0:
                raise Exception(Errors.EXECUTE_RESULT['gauss_0406'] % (ip, output))
            else:
                g.logger.info('Successfully copy module files to node [%s].' % ip)

    def remote_copy_manager(self):
        """
        Copy package to remote node.
        """
        manager_from = os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.realpath(__file__))))
        for node in self.agent_nodes:
            ip = node.get(Constant.NODE_IP)
            uname = node.get(Constant.NODE_USER)
            pwd = node.get(Constant.NODE_PWD)

            status, output = CommonTools.remote_copy_files(
                ip, uname, pwd, manager_from, self.manager_path)
            if status != 0:
                raise Exception(Errors.EXECUTE_RESULT['gauss_0406'] % (ip, output))
            else:
                g.logger.info('Successfully copy manager to node[%s].' % ip)

    def remote_copy_env_file(self):
        """
        Copy env file to remote node.
        """
        for node in self.agent_nodes:
            ip = node.get(Constant.NODE_IP)
            uname = node.get(Constant.NODE_USER)
            pwd = node.get(Constant.NODE_PWD)

            status, output = CommonTools.remote_copy_files(
                ip, uname, pwd, ENV_FILE, ENV_FILE)
            if status != 0:
                raise Exception(Errors.EXECUTE_RESULT['gauss_0406'] % (ip, output))
            else:
                g.logger.info('Successfully copy env file to node[%s].' % ip)

    @staticmethod
    def remote_install_single_agent(params):
        """
        Remote install single node agent
        """
        ip, uname, pwd, cmd = params
        g.logger.debug('Install node[%s], cmd[%s]' % (ip, cmd))
        g.logger.info('Starting install on node:[%s], please wait ...' % ip)
        status, output = CommonTools.remote_execute_cmd(ip, uname, pwd, cmd)
        if status != 0:
            raise Exception(Errors.EXECUTE_RESULT['gauss_0418'] % (ip, output))
        g.logger.info('Install result on node:[%s]-output:%s\n' % (ip, output))

    def remote_install(self):
        """
        Remote install agent on all nodes
        """
        install_script_path = os.path.join(self.install_path,
                                           Constant.ANOMALY_DETECTION_INSTALL_SCRIPT_PATH)
        param_file_path = os.path.join(EXTRACT_DIR, Constant.TEMP_ANOMALY_PARAM_FILE)
        cmd = Constant.EXECUTE_REMOTE_SCRIPT % (
            Constant.CMD_PREFIX, install_script_path, param_file_path)
        params_list = [(node.get(Constant.NODE_IP), node.get(Constant.NODE_USER),
                        node.get(Constant.NODE_PWD), cmd) for node in self.agent_nodes]
        CommonTools.parallel_execute(self.remote_install_single_agent, params_list)

    def prepare_param_file(self):
        """
        Write params into file
        """
        param_file_path = os.path.join(EXTRACT_DIR, Constant.TEMP_ANOMALY_PARAM_FILE)
        self.param_dict.pop(Constant.CA_INFO)
        CommonTools.mkdir_with_mode(os.path.dirname(param_file_path), Constant.AUTH_COMMON_DIR_STR)
        CommonTools.dict_to_json_file(self.param_dict, param_file_path)
        self._copy_param_file_to_remote_node(param_file_path, param_file_path)

    def _copy_param_file_to_remote_node(self, path_from, path_to):
        """
        Copy params file to remote agent node
        """
        for node in self.agent_nodes:
            ip = node.get(Constant.NODE_IP)
            uname = node.get(Constant.NODE_USER)
            pwd = node.get(Constant.NODE_PWD)
            CommonTools.remote_mkdir_with_mode(
                os.path.dirname(path_to), Constant.AUTH_COMMON_DIR_STR, ip, uname, pwd)
            status, output = CommonTools.remote_copy_files(
                ip, uname, pwd, path_from, path_to)
            if status != 0:
                raise Exception(Errors.EXECUTE_RESULT['gauss_0406'] % (ip, output))
            else:
                g.logger.info('Successfully copy install param file to node[%s].' % ip)

    def _mk_remote_module_dir(self):
        """
        Create install path if the path is not exist.
        """
        for node in self.agent_nodes:
            ip = node.get(Constant.NODE_IP)
            uname = node.get(Constant.NODE_USER)
            pwd = node.get(Constant.NODE_PWD)
            CommonTools.remote_mkdir_with_mode(
                self.module_path, Constant.AUTH_COMMON_DIR_STR, ip, uname, pwd)
            g.logger.info('Successfully create module path dir on node:[%s]' % ip)

    def _clean_envs(self):
        """
        Uninstall remote agent nodes
        :return:
        """
        try:
            params = deepcopy(self.param_dict)
            params[Constant.AGENT_NODES] = self.agent_nodes
            cleaner = RemoteUninstaller(**params)
            cleaner.init_globals()
            cleaner.prepare_param_file()
            cleaner.remote_uninstall(ignore=True)
        except Exception as error:
            g.logger.warning('Failed clean remote agent node with error:%s' % str(error))

    def _clean_remote_module_dir(self):
        """
        Clean install path before unpack.
        """
        for node in self.agent_nodes:
            ip = node.get(Constant.NODE_IP)
            uname = node.get(Constant.NODE_USER)
            pwd = node.get(Constant.NODE_PWD)
            CommonTools.retry_remote_clean_dir(self.module_path, ip, uname, pwd)
            g.logger.info('Successfully clean module path on node:[%s]' % ip)

    def _mk_remote_manager_dir(self):
        """
        Create install path if the path is not exist.
        """
        for node in self.agent_nodes:
            ip = node.get(Constant.NODE_IP)
            uname = node.get(Constant.NODE_USER)
            pwd = node.get(Constant.NODE_PWD)
            CommonTools.remote_mkdir_with_mode(
                self.manager_path, Constant.AUTH_COMMON_DIR_STR, ip, uname, pwd)
            g.logger.info('Successfully create manager path dir on node:[%s]' % ip)

    def _clean_remote_manager_dir(self):
        """
        Clean install path before unpack.
        """
        for node in self.agent_nodes:
            ip = node.get(Constant.NODE_IP)
            uname = node.get(Constant.NODE_USER)
            pwd = node.get(Constant.NODE_PWD)
            CommonTools.retry_remote_clean_dir(self.manager_path, ip, uname, pwd)
            g.logger.info('Successfully clean manager path on node:[%s]' % ip)

    def run(self):
        self.init_globals()
        self.prepare_remote_file_path()
        self.remote_copy_module()
        self.remote_copy_manager()
        self.remote_copy_env_file()
        self.prepare_param_file()
        self.remote_install()





