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
from gspylib.common.ErrorCode import ErrorCode

setItem = []


class CheckSshdConfig(BaseItem):
    def __init__(self):
        super(CheckSshdConfig, self).__init__(self.__class__.__name__)
        self.sshdThreshold = {}

    def preCheck(self):
        self.sshdThreshold = {}
        # check the threshold was set correctly
        if (not "PasswordAuthentication" in self.threshold.keys()
                or not "MaxStartups" in self.threshold.keys()
                or not "UseDNS" in self.threshold.keys()
                or not "ClientAliveInterval" in self.threshold.keys()):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"] % 'threshold')
        self.sshdThreshold['PasswordAuthentication'] = self.threshold[
            'PasswordAuthentication']
        self.sshdThreshold['MaxStartups'] = self.threshold['MaxStartups']
        self.sshdThreshold['UseDNS'] = self.threshold['UseDNS']
        self.sshdThreshold['ClientAliveInterval'] = self.threshold[
            'ClientAliveInterval']

    def doCheck(self):
        global setItem
        flag = "Normal"
        resultStr = ""
        self.result.raw = ""
        WarningItem = ['PasswordAuthentication', 'UseDNS']
        for item in self.sshdThreshold.keys():
            cmd = "cat /etc/ssh/sshd_config | grep -E %s | grep -v '^#' | " \
                  "awk '{print $1,$2}'" % item
            output = SharedFuncs.runShellCmd(cmd)
            self.result.raw += "\n%s" % output
            if (item == "ClientAliveInterval"):
                if (output == ""):
                    continue
                else:
                    timeout = int(output.split()[-1])
                    if (timeout != 0 and timeout < int(
                            self.sshdThreshold[item])):
                        flag = "Abnormal"
                        resultStr += "\nAbnormal reason: %s; expected: %s" % (
                            output, self.sshdThreshold[item])
                        setItem.append(output.split()[0])
            else:
                if (output != ""):
                    if (str(output.strip()).lower() != str('%s %s' % (
                            item, self.sshdThreshold[item])).lower()):
                        if (item in WarningItem):
                            flag = "Warning"
                            resultStr += "\nWarning reason: %s; expected: %s" \
                                         % (
                                             output, self.sshdThreshold[item])
                        else:
                            flag = "Abnormal"
                            resultStr += "\nAbnormal reason: %s; expected: " \
                                         "%s" \
                                         % (
                                             output, self.sshdThreshold[item])
                            setItem.append(output.split()[0])
                else:
                    if (item in WarningItem):
                        flag = "Warning"
                        resultStr += "\nWarning reason: " \
                                     "%s parameter is not set; expected: %s" \
                                     % (
                                         item, self.sshdThreshold[item])
                    else:
                        flag = "Abnormal"
                        resultStr += "\nAbnormal reason: " \
                                     "%s parameter is not set; expected: %s" \
                                     % (
                                         item, self.sshdThreshold[item])
                        setItem.append(output.split()[0])
        self.result.val = resultStr
        if (flag == "Normal"):
            self.result.rst = ResultStatus.OK
        elif (flag == "Warning" and len(setItem) == 0):
            self.result.rst = ResultStatus.WARNING
        else:
            self.result.rst = ResultStatus.NG

    def doSet(self):
        cmd = ""
        for item in setItem:
            if (item == "MaxStartups"):
                cmd += "sed -i '/^MaxStartups/d' /etc/ssh/sshd_config;"
                cmd += "echo 'MaxStartups=1000' >> /etc/ssh/sshd_config;"
            else:
                cmd = "sed -i '/^ClientAliveInterval/d' /etc/ssh/sshd_config;"
                cmd += "echo 'ClientAliveInterval 0' >> /etc/ssh/sshd_config;"
        cmd += "service sshd restart"
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            self.result.val = "Failed to set SshdConfig. The cmd is %s" % cmd
