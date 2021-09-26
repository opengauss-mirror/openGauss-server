#!/usr/bin/python3
# -*- coding: utf-8 -*-
#############################################################################
# Copyright (c): 2021, Huawei Tech. Co., Ltd.
# FileName     : logger.py
# Version      :
# Date         : 2021-4-7
# Description  : Logger for project
#############################################################################

try:
    import os
    import sys
    from configparser import ConfigParser

    sys.path.insert(0, os.path.dirname(__file__))
    from common.utils import Common, CONFIG_PATH
except ImportError as err:
    sys.exit("logger.py: Failed to import module: %s." % str(err))


class CreateLogger:
    def __init__(self, level, log_name):
        self.level = level
        self.log_name = log_name

    def create_log(self):
        config = ConfigParser()
        config.read(CONFIG_PATH)
        log_path = os.path.realpath(config.get("log", "log_path"))
        if not os.path.isdir(log_path):
            os.makedirs(log_path)

        logger = Common.create_logger(level=self.level,
                                      log_name=self.log_name,
                                      log_path=os.path.join(log_path, self.log_name))
        return logger
