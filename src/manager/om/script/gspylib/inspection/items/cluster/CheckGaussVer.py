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
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus


class CheckGaussVer(BaseItem):
    def __init__(self):
        super(CheckGaussVer, self).__init__(self.__class__.__name__)

    def doCheck(self):
        gaussdbVersion = ""
        gsqlVersion = ""
        # Get the version
        cmd = "gaussdb -V | awk '{print $4\"_\"$6}'"
        self.result.raw = cmd + "\n"
        gaussdbVersion = SharedFuncs.runShellCmd(cmd, "", self.mpprcFile)
        if (gaussdbVersion[-1] == ")"):
            gaussdbVersion = gaussdbVersion[:-1]
        # Get the version
        cmd = "gsql -V | awk '{print $4\"_\"$6}'"
        self.result.raw += cmd
        gsqlVersion = SharedFuncs.runShellCmd(cmd, "", self.mpprcFile)
        if (gsqlVersion[-1] == ")"):
            gsqlVersion = gsqlVersion[:-1]
        # Compare the two version numbers are the same
        if gaussdbVersion and gaussdbVersion == gsqlVersion:
            self.result.rst = ResultStatus.OK
        else:
            self.result.rst = ResultStatus.NG
        self.result.val = "gaussdb Version: %s \ngsql Version: %s" % (
            gaussdbVersion, gsqlVersion)
