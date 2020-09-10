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


class CheckNICModel(BaseItem):
    def __init__(self):
        super(CheckNICModel, self).__init__(self.__class__.__name__)

    def doCheck(self):
        if (self.cluster):
            LocalNodeInfo = self.cluster.getDbNodeByName(self.host)
            backIP = LocalNodeInfo.backIps[0]
        elif (self.ipAddr):
            backIP = self.ipAddr
        else:
            backIP = SharedFuncs.getIpByHostName(self.host)
        networkCardNumList = SharedFuncs.CheckNetWorkBonding(backIP)
        if networkCardNumList == "Shell command faild":
            return
        networkCardNums = []
        if (len(networkCardNumList) != 1):
            networkCardNums = networkCardNumList[1:]
        else:
            networkCardNums.append(networkCardNumList[0])
        flag = True
        for networkCardNum in networkCardNums:
            cmd = "/sbin/ethtool -i %s" % networkCardNum
            output = SharedFuncs.runShellCmd(cmd)
            self.result.raw += "[%s]\n%s\n" % (networkCardNum, output)
            NICVer = ""
            PCIAddr = ""
            for eachLine in output.split("\n"):
                if (eachLine.startswith("version:")):
                    NICVer = eachLine
                if (eachLine.startswith('bus-info:')):
                    if (len(eachLine.split(':')) == 4):
                        PCIAddr = eachLine.split(':')[2] + ':' + \
                                  eachLine.split(':')[3]
            if (NICVer):
                self.result.val += "%s\n" % (NICVer)
            else:
                self.result.val += "Failed to get NIC %s 'version' info\n" \
                                   % networkCardNum
                flag = False
            if (PCIAddr):
                cmd = "lspci |grep %s" % PCIAddr
                (status, output) = subprocess.getstatusoutput(cmd)
                self.result.raw += "%s\n" % (output)
                if (status == 0 and len(output.split(':')) == 3):
                    modelInfo = output.split(':')[2].split('(')[0]
                    self.result.val += "model: %s\n" % (modelInfo.strip())
                else:
                    self.result.val += "Failed to get NIC %s model" \
                                       " 'bus-info' info\n" % networkCardNum
                    self.result.val += "The cmd is %s " % cmd
                    flag = False
            else:
                self.result.val += "Failed to get NIC %s model" \
                                   " 'bus-info' info\n" % networkCardNum
                flag = False

        if (flag):
            self.result.rst = ResultStatus.OK
        else:
            self.result.rst = ResultStatus.NG
