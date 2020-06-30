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
import subprocess
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus


class CheckProcessStatus(BaseItem):
    def __init__(self):
        super(CheckProcessStatus, self).__init__(self.__class__.__name__)

    def doCheck(self):
        parRes = ""
        flag = 0
        self.result.raw = ""
        processList = ['gaussdb']
        for process in processList:
            # Query process status
            cmd = "ps -u %s -N | grep '\<%s\>'" % (self.user, process)
            self.result.raw += "%s\n" % cmd
            (status, output) = subprocess.getstatusoutput(cmd)
            # Resolve and outputs the execution results
            if (status == 0 and output.find("%s" % process) >= 0):
                parRes += "\n        %s" % (output)
                flag = 1
        if (flag == 1):
            self.result.rst = ResultStatus.NG
            self.result.val = parRes
        else:
            self.result.rst = ResultStatus.OK
            self.result.val = "All process Status is Normal."
