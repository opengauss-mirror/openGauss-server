#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c): 2012-2021, Huawei Tech. Co., Ltd.
# FileName     : global_box.py
# Version      : V1.0.0
# Date         : 2021-03-01
# Description  : Globals
#############################################################################

import sys
sys.path.append(sys.path[0] + "/../")
from tools.log import MainLog
from definitions.constants import Constant


class GlobalBox(object):
    def __init__(self):
        self.logger = None

        self.init_globals()

    def init_globals(self):
        self.__get_logger()

    def __get_logger(self):
        self.logger = MainLog(Constant.DEFAULT_LOG_NAME).get_logger()


g = GlobalBox()


