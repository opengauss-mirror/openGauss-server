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
import time
import platform
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.os.gsplatform import g_Platform


class CheckNetWorkDrop(BaseItem):
    def __init__(self):
        super(CheckNetWorkDrop, self).__init__(self.__class__.__name__)

    def doCheck(self):
        """
        function: Check NetWork care package drop rate in 1 minute
        """
        ipMap = {}
        netWorkInfo = {}
        distname, version, idnum = g_Platform.dist()
        for nodeInfo in self.cluster.dbNodes:
            ipMap[nodeInfo.sshIps[0]] = nodeInfo.backIps[0]
        for sshIp in ipMap.keys():
            backIp = ipMap[sshIp]
            # get remote IP network care number
            if (g_Platform.isPlatFormEulerOSOrRHEL7X()):
                if (os.getuid() == 0):
                    cmd = """pssh -s -P -H  %s \\"/sbin/ifconfig\\"|
                    grep -B 5 \\"%s\\"|grep \\"RUNNING\\" """ \
                          % (sshIp, backIp)
                else:
                    cmd = """pssh -s -P -H  %s "/sbin/ifconfig"|
                    grep -B 5 "%s"|grep "RUNNING" """ % (sshIp, backIp)
            else:
                if (os.getuid() == 0):
                    cmd = """pssh -s -P -H  %s \\"/sbin/ifconfig\\"|
                    grep -B 5 \\"%s\\"|grep \\"Link encap\\" """ \
                          % (sshIp, backIp)
                else:
                    cmd = """pssh -s -P -H  %s "/sbin/ifconfig"|
                    grep -B 5 "%s"|grep "Link encap" """ \
                          % (sshIp, backIp)
            output = SharedFuncs.runShellCmd(cmd, self.user)
            if (g_Platform.isPlatFormEulerOSOrRHEL7X()):
                NetWorkNum = output.split('\n')[-1].split()[0].split(':')[0]
            else:
                NetWorkNum = output.split('\n')[-1].strip().split()[0]

            if (g_Platform.isPlatFormEulerOSOrRHEL7X()):
                if (os.getuid() == 0):
                    packageCmd1 = """pssh -s -P -H  %s \\"/sbin/ifconfig %s|
                    grep packets|grep RX\\" """ % (sshIp, NetWorkNum)
                else:
                    packageCmd1 = """pssh -s -P -H  %s "/sbin/ifconfig %s|
                    grep packets|grep RX" """ % (sshIp, NetWorkNum)
            else:
                if (os.getuid() == 0):
                    packageCmd1 = """pssh -s -P -H  %s \\"/sbin/ifconfig %s|
                    grep dropped|grep RX\\" """ % (sshIp, NetWorkNum)
                else:
                    packageCmd1 = """pssh -s -P -H  %s "/sbin/ifconfig %s|
                    grep dropped|grep RX" """ % (sshIp, NetWorkNum)
            output = SharedFuncs.runShellCmd(packageCmd1, self.user)
            if (g_Platform.isPlatFormEulerOSOrRHEL7X()):
                package_begin = output.split('\n')[-1].strip().split()[2]
            else:
                package_begin = \
                    output.split('\n')[-1].split(":")[1].strip().split()[0]

            if (os.getuid() == 0):
                dropCmd1 = """pssh -s -P -H %s \\"/sbin/ifconfig %s|
                grep dropped|grep RX\\" """ % (sshIp, NetWorkNum)
            else:
                dropCmd1 = """pssh -s -P -H  %s "/sbin/ifconfig %s|
                grep dropped|grep RX" """ % (sshIp, NetWorkNum)
            output = SharedFuncs.runShellCmd(dropCmd1, self.user)
            if (g_Platform.isPlatFormEulerOSOrRHEL7X()):
                drop_begin = output.split('\n')[-1].strip().split()[4]
            else:
                drop_begin = \
                    output.split('\n')[-1].split(":")[3].strip().split()[0]
            netWorkInfo[backIp] = [NetWorkNum, package_begin, drop_begin, "",
                                   ""]
        time.sleep(60)

        for sshIp in ipMap.keys():
            backIp = ipMap[sshIp]
            if (g_Platform.isPlatFormEulerOSOrRHEL7X()):
                if (os.getuid() == 0):
                    packageCmd2 = """pssh -s -P -H %s \\"/sbin/ifconfig %s|
                    grep packets|grep RX\\" """ \
                                  % (sshIp, netWorkInfo[backIp][0])
                else:
                    packageCmd2 = """pssh -s -P -H  %s "/sbin/ifconfig %s|
                    grep packets|grep RX" """ % (sshIp,
                                                 netWorkInfo[backIp][0])
            else:
                if (os.getuid() == 0):
                    packageCmd2 = """pssh -s -P -H  %s \\"/sbin/ifconfig %s|
                    grep dropped|grep RX\\" """ % (sshIp,
                                                   netWorkInfo[backIp][0])
                else:
                    packageCmd2 = """pssh -s -P -H  %s "/sbin/ifconfig %s|
                    grep dropped|grep RX" """ % (sshIp,
                                                 netWorkInfo[backIp][0])
            output = SharedFuncs.runShellCmd(packageCmd2, self.user)
            if (g_Platform.isPlatFormEulerOSOrRHEL7X()):
                package_end = output.split('\n')[-1].strip().split()[2]
            else:
                package_end = \
                    output.split('\n')[-1].split(":")[1].strip().split()[0]
            if (os.getuid() == 0):
                dropCmd2 = """pssh -s -P -H %s \\"/sbin/ifconfig %s|
                grep dropped|grep RX\\" """ % (sshIp, netWorkInfo[backIp][0])
            else:
                dropCmd2 = """pssh -s -P -H  %s "/sbin/ifconfig %s|
                grep dropped|grep RX" """ % (sshIp, netWorkInfo[backIp][0])
            output = SharedFuncs.runShellCmd(dropCmd2, self.user)
            if (g_Platform.isPlatFormEulerOSOrRHEL7X()):
                drop_end = output.split('\n')[-1].strip().split()[4]
            else:
                drop_end = \
                    output.split('\n')[-1].split(":")[3].strip().split()[0]
            netWorkInfo[backIp][3] = package_end
            netWorkInfo[backIp][4] = drop_end

        flag = True
        self.result.raw = ""
        resultStr = ""
        for ip in netWorkInfo.keys():
            packageSum = int(netWorkInfo[ip][3]) - int(netWorkInfo[ip][1])
            dropSum = int(netWorkInfo[ip][4]) - int(netWorkInfo[ip][2])
            dropRate = float(dropSum) / packageSum
            if (dropRate > 0.01):
                flag = False
                resultStr += "\nAddress %s %s communication packet loss" \
                             " rate of %.2f%%, more than 1%%." \
                             % (ip, netWorkInfo[ip][0], dropRate * 100)
            self.result.raw += "\n %s %s %s %s %.2f%%" % (
                ip, netWorkInfo[ip][0], dropSum, packageSum, dropRate * 100)
        if flag:
            self.result.rst = ResultStatus.OK
            self.result.val = "All IP communications are stable."
        else:
            self.result.rst = ResultStatus.NG
            self.result.val = resultStr
