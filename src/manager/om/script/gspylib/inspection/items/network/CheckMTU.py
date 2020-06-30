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
from gspylib.os.gsnetwork import g_network

networkCardNum = ""


class CheckMTU(BaseItem):
    def __init__(self):
        super(CheckMTU, self).__init__(self.__class__.__name__)
        self.expectMTU1 = None
        self.expectMTU2 = None

    def preCheck(self):
        # check current node contains cn instances if not raise  exception
        super(CheckMTU, self).preCheck()
        # check the threshold was set correctly
        if (not self.threshold.__contains__('expectMTU1')):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"]
                            % "threshold expectMTU1")
        self.expectMTU1 = self.threshold['expectMTU1']
        if (not self.threshold.__contains__('expectMTU2')):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"]
                            % "threshold expectMTU2")
        self.expectMTU2 = self.threshold['expectMTU2']

    def doCheck(self):
        global networkCardNum
        if self.cluster:
            # Get node information
            LocalNodeInfo = self.cluster.getDbNodeByName(self.host)
            # Get the IP address
            backIP = LocalNodeInfo.backIps[0]
        else:
            backIP = SharedFuncs.getIpByHostName(self.host)
        # Get the network card number
        networkCards = g_network.getAllNetworkInfo()
        for network in networkCards:
            if network.ipAddress == backIP:
                networkCardNum = network.NICNum
                networkMTU = network.MTUValue
                break
        if not networkCardNum or not networkMTU:
            raise Exception(ErrorCode.GAUSS_506["GAUSS_50619"])
        # Check the mtu value obtained is not a number
        if not str(networkMTU).isdigit():
            raise Exception(ErrorCode.GAUSS_506["GAUSS_50612"]
                            % (networkCardNum + " " + "MTU"))

        self.result.val = str(networkMTU)
        # Compare the acquired MTU with the threshold
        if (int(networkMTU) != int(self.expectMTU1) and int(
                networkMTU) != int(self.expectMTU2)):
            self.result.rst = ResultStatus.WARNING
            self.result.raw = "Warning MTU value[%s]: RealValue '%s' " \
                              "ExpectedValue '%s' or '%s'.\n" \
                              % (networkCardNum, int(networkMTU),
                                 self.expectMTU1, self.expectMTU2)
        else:
            self.result.rst = ResultStatus.OK
            self.result.raw = "[%s]MTU: %s" \
                              % (networkCardNum, str(networkMTU))

    def doSet(self):
        resultStr = ""
        (THPFile, initFile) = SharedFuncs.getTHPandOSInitFile()
        cmd = "ifconfig %s mtu 1500;" % networkCardNum
        cmd += "echo ifconfig %s mtu 1500 >> %s" % (networkCardNum, initFile)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            resultStr += "Set MTU Failed.Error : %s." % output
            resultStr += "The cmd is %s " % cmd
        if (len(resultStr) > 0):
            self.result.val = resultStr
        else:
            self.result.val = "Set MTU successfully."
