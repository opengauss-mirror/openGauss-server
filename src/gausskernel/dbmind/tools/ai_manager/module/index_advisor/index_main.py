#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c): 2012-2021, Huawei Tech. Co., Ltd.
# FileName     : ad_main.py
# Version      : V1.0.0
# Date         : 2021-03-01
# Description  : Main entrance of module anomaly detection
#############################################################################

from module.index_advisor.install import Installer
from module.index_advisor.uninstall import UnInstaller
from config_cabin.config import PROJECT_PATH


TASK_MAPPING = {
    'install': Installer,
    'uninstall': UnInstaller
}


class IndexAdvisor(object):
    def __init__(self, **params_dict):
        self.args_dict = params_dict
        self.action = self.args_dict.get('action')
        self.package_path = self.args_dict.get('package_path')
        self.project_path = PROJECT_PATH
        self.version = self.args_dict.get('version')
        self.install_path = self.args_dict.get('install_path')
        self.task = None

    def init_globals(self):
        self.task = TASK_MAPPING[self.action](**self.args_dict)

    def run(self):
        self.init_globals()
        self.task.run()




