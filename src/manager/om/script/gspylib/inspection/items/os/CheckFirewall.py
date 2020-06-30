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
import platform
import subprocess
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.os.gsservice import g_service
from gspylib.os.gsplatform import g_Platform

EXPECTED_VALUE = "disabled"
SUSE_FLAG = "SuSEfirewall2 not active"
REDHAT6_FLAG = "Firewall is not running"
REDHAT7_FLAG = "Active: inactive (dead)"


class CheckFirewall(BaseItem):
    def __init__(self):
        super(CheckFirewall, self).__init__(self.__class__.__name__)

    def doCheck(self):
        (status, output) = g_service.manageOSService("firewall", "status")
        if (output.find(SUSE_FLAG) > 0 or output.find(
                REDHAT6_FLAG) > 0 or output.find(REDHAT7_FLAG) > 0):
            firewallStatus = "disabled"
        else:
            firewallStatus = "enabled"
        if (firewallStatus == ""):
            self.result.rst = ResultStatus.OK
        elif (firewallStatus != EXPECTED_VALUE):
            self.result.rst = ResultStatus.NG
        else:
            self.result.rst = ResultStatus.OK
        if (not self.result.raw):
            self.result.raw = output
        else:
            self.result.raw = output
        self.result.val = firewallStatus

    def doSet(self):
        if g_Platform.isPlatFormEulerOSOrRHEL7X():
            cmd = "systemctl stop firewalld.service"
        elif SharedFuncs.isSupportSystemOs():
            cmd = "service iptables stop"
        else:
            cmd = "SuSEfirewall2 stop"

        status, output = subprocess.getstatusoutput(cmd)
        if status:
            self.result.val = "Failed to stop firewall service. Error: %s\n" \
                              % output + "The cmd is %s " % cmd
        else:
            self.result.val = "Successfully stopped the firewall service.\n"
