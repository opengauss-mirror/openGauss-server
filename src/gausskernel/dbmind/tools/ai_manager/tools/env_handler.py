#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c): 2012-2021, Huawei Tech. Co., Ltd.
# FileName     : env_handler.py
# Version      : V1.0.0
# Date         : 2021-03-01
# Description  : env file handler
#############################################################################

import sys

sys.path.append(sys.path[0] + "/../")
from tools.common_tools import CommonTools
from tools.global_box import g

PYTHON_PATH = ''

# Dict of env need defined:
ENV_MENU = {
    "export PYTHONPATH": "export PYTHONPATH=%s" % PYTHON_PATH
}


class EnvHandler(object):
    def __init__(self, env_file_path):
        self.env_file_path = env_file_path
        self.env_mapping = ENV_MENU

    def create_env_file_if_not_exist(self):
        """
        Create env file
        """
        ret = CommonTools.create_file_if_not_exist(self.env_file_path)
        if ret:
            g.logger.info('Successfully create env file.')
        else:
            g.logger.info('Env file already exist.')

    def modify_env_file(self):
        """
        Modify env file
        """
        if not self.env_mapping.keys():
            g.logger.info('No need write env file.')
            return
        with open(self.env_file_path, 'r') as file:
            content_list = file.readlines()
            for key in ENV_MENU.keys():
                content_list = [item.strip() for item in content_list if key not in item]
        content_list += ENV_MENU.values()
        with open(self.env_file_path, 'w') as file2:
            file2.write('\n'.join(content_list))
            g.logger.info('Successfully modify env file.')

    def run(self):
        self.create_env_file_if_not_exist()
        self.modify_env_file()

