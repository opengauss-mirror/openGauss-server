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
import subprocess
import platform
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.os.gsplatform import g_Platform


class CheckRouting(BaseItem):
    def __init__(self):
        super(CheckRouting, self).__init__(self.__class__.__name__)

    @staticmethod
    def getBinaryAddr(ipAddr):
        binaryStr = ""
        for part in ipAddr.split('.'):
            binaryStr += "%08d" % int(bin(int(part)).replace('0b', ''))
        return binaryStr

    def getBinaryRouting(self, ipAndMask):
        (ip, netMask) = ipAndMask.split(':')
        ipBinary = self.getBinaryAddr(ip)
        maskBinary = self.getBinaryAddr(netMask)
        routingBinary = ""
        if (not len(ipBinary) == len(maskBinary) == 32):
            return ""
        for bit in range(len(ipBinary)):
            routingBinary += str(int(ipBinary[bit]) & int(maskBinary[bit]))
        return routingBinary

    def doCheck(self):
        ipList = []
        routingBinary = self.getBinaryRouting(self.routing)
        if g_Platform.isPlatFormEulerOSOrRHEL7X():
            cmd = "/sbin/ifconfig -a |grep -E '\<inet\>'| awk '{print $2}'"
        else:
            cmd = "/sbin/ifconfig -a |grep 'inet addr'|" \
                  " awk '{print $2}'| awk -F ':' '{print $2}'"
        output = SharedFuncs.runShellCmd(cmd)
        for eachLine in output.split('\n'):
            if (SharedFuncs.validate_ipv4(eachLine)):
                maskAddr = SharedFuncs.getMaskByIP(eachLine)
                ipMask = "%s:%s" % (eachLine, maskAddr)
                ipList.append(ipMask)
        self.result.raw = "Routing: %s [bit]%s\nlocalIP:\n%s" % (
            self.routing, routingBinary, "\n".join(ipList))

        commIP = []
        for ipMask in ipList:
            ipBinary = self.getBinaryRouting(ipMask)
            if (ipBinary == routingBinary):
                commIP.append(ipMask)

        if (len(commIP) > 1):
            self.result.rst = ResultStatus.WARNING
        else:
            self.result.rst = ResultStatus.OK
        self.result.val = "Business network segment IP: " + ", ".join(commIP)
