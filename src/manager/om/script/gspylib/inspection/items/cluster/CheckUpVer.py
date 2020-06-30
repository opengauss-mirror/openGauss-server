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
from gspylib.common.ErrorCode import ErrorCode
from gspylib.os.gsfile import g_file


class CheckUpVer(BaseItem):
    def __init__(self):
        super(CheckUpVer, self).__init__(self.__class__.__name__)
        self.upgradepath = None

    def preCheck(self):
        # check the threshold was set correctly
        if (not self.threshold.__contains__("upgradepath")):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50236"]
                            % "The upgrade path")
        self.upgradepath = self.threshold['upgradepath']
        if not os.path.isfile(os.path.join(self.upgradepath, "version.cfg")):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"]
                            % ("new version file[%s]" %
                               os.path.join(self.upgradepath, "version.cfg")))

    def doCheck(self):
        packageFile = os.path.realpath(
            os.path.join(self.upgradepath, "version.cfg"))
        output = g_file.readFile(packageFile)
        self.result.rst = ResultStatus.OK
        self.result.val = "".join(output)
