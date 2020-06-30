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
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.os.gsnetwork import g_network
from gspylib.os.gsfile import g_file
from gspylib.common.ErrorCode import ErrorCode

EXPECTED_RXTX = 4096


class CheckRXTX(BaseItem):
    def __init__(self):
        super(CheckRXTX, self).__init__(self.__class__.__name__)

    def doCheck(self):
        flag = "Normal"
        networkCardNums = []
        if (self.cluster):
            LocalNodeInfo = self.cluster.getDbNodeByName(self.host)
            backIP = LocalNodeInfo.backIps[0]
        else:
            backIP = SharedFuncs.getIpByHostName(self.host)

        allNetworkInfo = g_network.getAllNetworkInfo()
        for network in allNetworkInfo:
            if (network.ipAddress == backIP):
                networkNum = network.NICNum
                BondMode = network.networkBondModeInfo
                confFile = network.networkConfigFile
                break

        if (not networkNum or not BondMode or not confFile):
            raise Exception(ErrorCode.GAUSS_506["GAUSS_50619"])
        if (BondMode != "BondMode Null"):
            bondFile = '/proc/net/bonding/%s' % networkNum
            bondInfoList = g_file.readFile(bondFile, "Slave Interface")
            for bondInfo in bondInfoList:
                networkNum = bondInfo.split(':')[-1].strip()
                networkCardNums.append(networkNum)
        else:
            networkCardNums.append(networkNum)

        for networkCardNum in networkCardNums:
            RXvalue = ""
            TXvalue = ""
            for network in allNetworkInfo:
                if (network.NICNum == networkCardNum and
                        str(network.RXValue).strip() != "" and
                        str(network.TXValue).strip() != ""):
                    RXvalue = network.RXValue
                    TXvalue = network.TXValue
            if (not RXvalue or not TXvalue):
                flag = "Error"
                self.result.val += "Failed to obtain network card [%s]" \
                                   " RX or TX value." % networkCardNum
                continue

            if (int(RXvalue) < int(EXPECTED_RXTX)) or (
                    int(TXvalue) < int(EXPECTED_RXTX)):
                flag = "Error"
                self.result.val += "NetWork[%s]\nRX: %s\nTX: %s\n" % (
                    networkCardNum, RXvalue, RXvalue)

        self.result.raw = self.result.val
        if (flag == "Normal"):
            self.result.rst = ResultStatus.OK
        else:
            self.result.rst = ResultStatus.NG

    def doSet(self):
        if (self.cluster):
            LocalNodeInfo = self.cluster.getDbNodeByName(self.host)
            backIP = LocalNodeInfo.backIps[0]
        elif (self.ipAddr):
            backIP = self.ipAddr
        else:
            backIP = SharedFuncs.getIpByHostName(self.host)
        networkCardNumList = SharedFuncs.CheckNetWorkBonding(backIP)
        if (len(networkCardNumList) != 1):
            networkCardNums = networkCardNumList[1:]
        else:
            networkCardNums = networkCardNumList
        for networkCardNum in networkCardNums:
            cmd = "/sbin/ethtool -G %s %s %d" % (
                networkCardNum, "rx", EXPECTED_RXTX)
            cmd += ";/sbin/ethtool -G %s %s %d" % (
                networkCardNum, "tx", EXPECTED_RXTX)
            SharedFuncs.runShellCmd(cmd)
