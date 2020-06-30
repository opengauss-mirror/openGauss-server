# -*- coding:utf-8 -*-
# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms
# and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
# WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# ----------------------------------------------------------------------------
import os
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.os.gsfile import g_file


class CheckIntegrity(BaseItem):
    def __init__(self):
        super(CheckIntegrity, self).__init__(self.__class__.__name__)

    def doCheck(self):
        gaussHome = self.cluster.appPath
        gaussdbFile = os.path.join(gaussHome, "bin/gaussdb")
        gaussdbIntegrity = g_file.getFileSHA256(gaussdbFile)
        self.result.raw = gaussdbIntegrity
        if (gaussdbIntegrity != "" and len(gaussdbIntegrity) == 64):
            self.result.rst = ResultStatus.OK
            self.result.val = "gaussdb sha256sum: %s" % gaussdbIntegrity
        else:
            self.result.rst = ResultStatus.NG
            self.result.val = "Failed to obtain gaussdb sha256 value." \
                              " Error:\n%s" % gaussdbIntegrity
