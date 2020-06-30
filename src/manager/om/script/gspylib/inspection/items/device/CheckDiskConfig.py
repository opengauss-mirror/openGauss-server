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


class CheckDiskConfig(BaseItem):
    def __init__(self):
        super(CheckDiskConfig, self).__init__(self.__class__.__name__)

    def doCheck(self):
        DiskInfoDict = {}
        ResultStr = ""
        cmd = "df -h -P | awk '{print $1,$2,$6}'"
        output = SharedFuncs.runShellCmd(cmd)
        diskList = output.split('\n')[1:]
        for disk in diskList:
            diskInfo = disk.split()
            DiskInfoDict[diskInfo[0]] = disk
        keys = DiskInfoDict.keys()
        sorted(keys)
        for diskName in keys:
            ResultStr += "%s\n" % DiskInfoDict[diskName]
        self.result.val = ResultStr
        self.result.rst = ResultStatus.OK
