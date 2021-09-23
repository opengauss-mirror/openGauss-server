#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c): 2012-2021, Huawei Tech. Co., Ltd.
# FileName     : ad_main.py
# Version      : V1.0.0
# Date         : 2021-03-01
# Description  : Main entrance of module anomaly detection
#############################################################################
from copy import deepcopy
from module.anomaly_detection.install import Installer
from module.anomaly_detection.uninstall import Uninstaller
from module.anomaly_detection.install_remote import RemoteInstaller
from module.anomaly_detection.uninstall_remote import RemoteUninstaller
from config_cabin.config import PROJECT_PATH
from definitions.constants import Constant


LOCAL_TASK_MAPPING = {
    'install': Installer,
    'uninstall': Uninstaller
}
REMOTE_TASK_MAPPING = {
    'install': RemoteInstaller,
    'uninstall': RemoteUninstaller
}


class AnomalyDetection(object):
    def __init__(self, **params_dict):
        self.args_dict = params_dict
        self.action = self.args_dict.get('action')
        self.package_path = self.args_dict.get('package_path')
        self.project_path = PROJECT_PATH
        self.scene = self.args_dict.get('scene')
        self.version = self.args_dict.get('version')
        self.install_path = self.args_dict.get('install_path')
        self.task = None
        self.remote_task = None

    def init_globals(self):

        if self.scene == Constant.SCENE_HUAWEIYUN:
            self.args_dict['service_list'] = Constant.ANOMALY_INSTALL_CRON_LOCALLY
            self.args_dict['stopping_list'] = Constant.ANOMALY_STOP_CMD_LOCALLY
            self.task = LOCAL_TASK_MAPPING[self.action](**self.args_dict)
        if self.scene == Constant.SCENE_OPENGAUSS:
            self.args_dict['service_list'] = Constant.OPENGAUSS_ANOMALY_INSTALL_CRON_LOCALLY
            self.args_dict['stopping_list'] = Constant.OPENGAUSS_ANOMALY_STOP_CMD_LOCALLY
            self.task = LOCAL_TASK_MAPPING[self.action](**self.args_dict)
            remote_params = deepcopy(self.args_dict)
            remote_params['service_list'] = Constant.OPENGAUSS_ANOMALY_INSTALL_CRON_REMOTE
            remote_params['stopping_list'] = Constant.OPENGAUSS_ANOMALY_STOP_CMD_REMOTE
            self.remote_task = REMOTE_TASK_MAPPING[self.action](**remote_params)

    def run(self):
        self.init_globals()
        self.task.run()
        if self.remote_task:
            self.remote_task.run()



