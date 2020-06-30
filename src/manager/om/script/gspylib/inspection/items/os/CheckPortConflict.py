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


class CheckPortConflict(BaseItem):
    def __init__(self):
        super(CheckPortConflict, self).__init__(self.__class__.__name__)

    def doCheck(self):
        cmd = "netstat -apn | grep 'tcp' " \
              "| grep 'LISTEN'| awk -F ' ' '$4 ~ /25[0-9][0-9][0-9]/'"
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            self.result.rst = ResultStatus.NG
            self.result.val = "Failed to excuted commands: %s\noutput:%s " % (
                cmd, output)
        else:
            if (output.strip() == ""):
                self.result.rst = ResultStatus.OK
                self.result.val = "ports is normal"
            else:
                self.result.rst = ResultStatus.NG
                self.result.val = output
                self.result.raw = "checked ports: (25000-26000)\n" + output

    def doSet(self):
        pidList = []
        cmd = "netstat -apn| grep 'tcp'" \
              "| grep 'LISTEN'| awk -F ' ' '$4 ~ /25[0-9][0-9][0-9]/'" \
              "| awk '{print $NF}'"
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status == 0 and output != ""):
            for line in output.split('\n'):
                if (line.find('/') > 0):
                    pid = line.split('/')[0].strip()
                    if (pid.isdigit()):
                        pidList.append(pid)
        if (pidList):
            cmd = "kill -9"
            for pid in pidList:
                cmd += " %s" % pid
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != ""):
                self.result.val = "Failed to kill process.Error:%s\n" % output
                self.result.val += "The cmd is %s " % cmd
            else:
                self.result.val = \
                    "Successfully killed the process with occupies the port.\n"
