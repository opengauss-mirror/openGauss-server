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
from gspylib.os.gsfile import g_file


class CheckUsedPort(BaseItem):
    def __init__(self):
        super(CheckUsedPort, self).__init__(self.__class__.__name__)

    def getPortRange(self):
        portRangeValue = \
            g_file.readFile('/proc/sys/net/ipv4/ip_local_port_range')[0]
        (startPort, endPort) = portRangeValue.split()
        portRange = int(endPort) - int(startPort)

        return portRange

    def getTcpUsedPort(self):
        if (self.ipAddr):
            serviceIP = self.ipAddr
        else:
            serviceIP = SharedFuncs.getIpByHostName(self.host)

        cmd = "netstat -ano|awk '{print $4}'|grep '%s'|sort|uniq -c|" \
              "grep ' 1 '|wc -l" % serviceIP
        tcpUsed = SharedFuncs.runShellCmd(cmd)

        return int(tcpUsed)

    def getSctpUsedPort(self):
        cmd = "cat /proc/net/sctp/assocs|" \
              "awk '{print $12}'|sort|uniq -c |wc -l"
        sctpUsed = SharedFuncs.runShellCmd(cmd)

        return int(sctpUsed)

    def doCheck(self):
        portRange = self.getPortRange()
        tcpUsed = self.getTcpUsedPort()
        sctpUsed = self.getSctpUsedPort()
        defaultPortRange = 60000 - 32768
        if (portRange < defaultPortRange):
            self.result.rst = ResultStatus.WARNING
            self.result.val = "port range is %s,Check items are not passed." \
                              % portRange
            return

        if (tcpUsed > portRange * 0.8):
            self.result.rst = ResultStatus.WARNING
            self.result.val = "tcp port used is %s,Check items are" \
                              " not passed." % tcpUsed
            return

        if (sctpUsed > portRange * 0.8):
            self.result.rst = ResultStatus.WARNING
            self.result.val = "sctp port used is %s," \
                              "Check items are not passed." % sctpUsed
            return

        self.result.rst = ResultStatus.OK
        self.result.val = "port range is %s,tcp port used is %s," \
                          "sctp port used is %d,Check items pass." \
                          % (portRange, tcpUsed, sctpUsed)
        return
