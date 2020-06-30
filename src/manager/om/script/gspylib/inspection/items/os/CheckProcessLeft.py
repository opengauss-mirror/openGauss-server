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
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus


class CheckProcessLeft(BaseItem):
    def __init__(self):
        super(CheckProcessLeft, self).__init__(self.__class__.__name__)

    def doCheck(self):
        parRes = ""
        flag = 0
        processList = ['gaussdb', 'omm']
        for process in processList:
            cmd = "ps -ef | grep '%s ' -m 20 | grep -v 'grep'" % process
            (status, output) = subprocess.getstatusoutput(cmd)
            if (output.find(process) >= 0):
                parRes += "the process is left over: \n%s" % output
                flag = 1

        if (flag == 1):
            self.result.rst = ResultStatus.NG
        else:
            self.result.rst = ResultStatus.OK
        self.result.val = parRes
        self.result.raw = output

    def doSet(self):
        processList = ['gaussdb', 'omm']
        for process in processList:
            cmd = "ps -eo pid,user,comm | grep -E '\<%s\>' " \
                  "| grep -v 'grep' | awk '{print $1}'|xargs kill -9" % process
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                self.result.val = "Failed to kill " \
                                  "process.Error:\n%s\n" % output + \
                                  "The cmd is %s " % cmd
            else:
                self.result.val = "Successfully killed the gauss " \
                                  "and omm user process.\n"
