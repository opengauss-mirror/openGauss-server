#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c): 2012-2021, Huawei Tech. Co., Ltd.
# FileName     : ai_manager.py
# Version      : V1.0.0
# Date         : 2021-03-01
# Description  : Main entrance of ai manager
#############################################################################

import optparse
import os

from definitions.constants import Constant
from tools.global_box import g
from module.anomaly_detection.ad_main import AnomalyDetection
from module.index_advisor.index_main import IndexAdvisor
from definitions.errors import Errors
from tools.common_tools import CommonTools
from tools import params_checkers
from config_cabin.config import EXTRACT_DIR
from config_cabin.config import PROJECT_PATH
from config_cabin.config import PYTHON_PATH
from config_cabin.config import ENV_FILE
from tools.env_handler import EnvHandler
from tools.params_checkers import LostChecker

MODULE_MAPPING = {
    'anomaly_detection': AnomalyDetection,
    'index_advisor': IndexAdvisor
}


class Manager(object):
    def __init__(self, **kwargs):
        self.arg_dict = kwargs
        self.project_path = PROJECT_PATH
        self.module = self.arg_dict.get('module')
        self.action = self.arg_dict.get('action')
        self.package_path = self.arg_dict.get('package_path')
        self.install_path = None
        self.version = None
        self.ai_manager_path = None

    def init_globals(self):
        self.install_path, self.version = self._get_install_path_with_no_package()
        self.arg_dict['install_path'] = self.install_path
        self.arg_dict['version'] = self.version
        self.ai_manager_path = os.path.realpath(os.path.join(
            self.install_path, Constant.AI_MANAGER_PATH))

    def check_params(self):
        lost_checker = LostChecker(self.arg_dict)
        lost_checker.run()
        funcs = CommonTools.get_funcs(params_checkers)
        for param_name, param_value in self.arg_dict.items():
            if param_name not in params_checkers.PARAMS_CHECK_MAPPING:
                raise Exception(Errors.PARAMETER['gauss_0203'] % param_name)
            funcs[params_checkers.PARAMS_CHECK_MAPPING[param_name]](param_value)

    @staticmethod
    def check_process_exist():
        count = CommonTools.check_process('ai_manager')
        if count > 1:
            raise Exception(Errors.PERMISSION['gauss_0702'] % 'ai_manager')
        else:
            g.logger.debug('Check process passed.')

    @staticmethod
    def check_user():
        is_root = CommonTools.check_is_root()
        if is_root:
            raise Exception(Errors.PERMISSION['gauss_0703'])
        else:
            g.logger.debug('Check user passed.')

    def _get_install_path_with_no_package(self):
        """
        Get version info from version file with relative path
        :return:
        """
        version_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.realpath(__file__))))), Constant.VERSION_FILE)
        version = CommonTools.get_version_info_from_file(version_file)
        g.logger.info('Got version info:%s' % version)
        base_dir = Constant.PACK_PATH_PREFIX + version
        install_path = os.path.join(self.project_path, base_dir)
        g.logger.info('Got install path:%s.' % install_path)
        CommonTools.check_path_valid(install_path)
        g.logger.info('Successfully to get install path.')
        return install_path, version

    def _get_install_path(self, pack_path):
        """
        Extract version info and assembling install path.
        """
        g.logger.info('Start getting install path.')
        # mk temp extract dir if not exist
        CommonTools.mkdir_with_mode(EXTRACT_DIR, Constant.AUTH_COMMON_DIR_STR)
        # clean extract dir
        CommonTools.clean_dir(EXTRACT_DIR)
        # extract package file to temp dir
        CommonTools.extract_file_to_dir(pack_path, EXTRACT_DIR)
        g.logger.info('Success extract files to temp dir.')
        # get version info from version file
        version_file = os.path.realpath(os.path.join(EXTRACT_DIR, Constant.VERSION_FILE))
        version = CommonTools.get_version_info_from_file(version_file)
        g.logger.info('Got version info:%s' % version)
        base_dir = Constant.PACK_PATH_PREFIX + version
        install_path = os.path.join(self.project_path, base_dir)
        g.logger.info('Got install path:%s.' % install_path)
        CommonTools.check_path_valid(install_path)
        g.logger.info('Successfully to get install path.')
        return install_path, version

    def _check_project_path_access(self):
        """
        Check project path is full authority.
        """
        CommonTools.check_dir_access(self.project_path, 'full')

    def _mk_manager_dir(self):
        """
        Create install path if the path is not exist.
        """
        if not os.path.isdir(self.ai_manager_path):
            g.logger.info('Install path:%s is not exist, start creating.' % self.ai_manager_path)
            CommonTools.mkdir_with_mode(self.ai_manager_path, Constant.AUTH_COMMON_DIR_STR)
        else:
            g.logger.info('Install path:%s is already exist.' % self.ai_manager_path)

    def _clean_manager_dir(self):
        """
        Clean install path before unpack.
        """
        file_list = os.listdir(self.ai_manager_path)
        if file_list:
            g.logger.info('Start clean install path, file list:[%s]' % file_list)
            CommonTools.clean_dir(self.ai_manager_path)

    def _copy_manager_files(self):
        """
        Copy manager files to manager dir
        """
        from_path = os.path.join(os.path.dirname(os.path.dirname(
            os.path.realpath(__file__))), Constant.AI_MANAGER_PATH)
        to_path = self.ai_manager_path
        CommonTools.copy_file_to_dest_path(from_path, to_path)
        g.logger.info('Successfully to copy files to manager path.')

    @staticmethod
    def _copy_lib():
        """
        Copy lib file to project path
        """
        from_path = os.path.realpath(os.path.join(EXTRACT_DIR, Constant.AI_LIB_PATH))
        to_path = PYTHON_PATH
        CommonTools.copy_file_to_dest_path(from_path, to_path)
        g.logger.info('Successfully to copy lib files.')

    @staticmethod
    def clean_temp_extract_dir():
        """
        Clean temp unpack dir
        """
        CommonTools.clean_dir(EXTRACT_DIR)
        g.logger.info('Successfully clean temp unpack dir.')

    @staticmethod
    def modify_env_file():
        handler = EnvHandler(ENV_FILE)
        handler.run()

    def prepare_manager_tools(self):
        """
        Prepare ai manager, create path, clean path and copy files
        """
        self._check_project_path_access()
        self._mk_manager_dir()
        self._clean_manager_dir()
        self._copy_manager_files()

    def run(self):
        g.logger.info(Constant.LOG_SEP_LINE)
        g.logger.info('Starting Module[%s]-Action[%s]...' % (self.module, self.action))
        g.logger.info(Constant.LOG_SEP_LINE)
        g.logger.info('Get input arguments:%s' % str(self.arg_dict))
        self.check_params()
        self.check_user()
        self.check_process_exist()
        if self.action.lower() != 'uninstall':
            self.init_globals()
            self.modify_env_file()
            self.prepare_manager_tools()
        module_inst = MODULE_MAPPING[self.module](**self.arg_dict)
        module_inst.run()
        self.clean_temp_extract_dir()


def init_parser():
    """parser command"""
    parser = optparse.OptionParser(conflict_handler='resolve')
    parser.disable_interspersed_args()
    parser.add_option('--module', dest='module', help='function module block')
    parser.add_option('--action', dest='action', help='action')
    parser.add_option('--param_file', dest='param_file', help='json file')

    return parser


if __name__ == '__main__':
    ai_parser = init_parser()
    opt, arg = ai_parser.parse_args()
    module = opt.module
    action = opt.action
    param_file = opt.param_file
    if not param_file:
        g.logger.error('Failed to get param file.')
        raise Exception(Errors.FILE_DIR_PATH['gauss_0105'])
    param_dict = CommonTools.json_file_to_dict(param_file)
    if action:
        param_dict['action'] = action
    if module:
        param_dict['module'] = module
    manager = Manager(**param_dict)
    manager.run()

