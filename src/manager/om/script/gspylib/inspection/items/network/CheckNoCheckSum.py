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
import platform
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.os.gsfile import g_file
from gspylib.os.gsnetwork import g_network
from gspylib.os.gsfile import g_Platform
from gspylib.common.ErrorCode import ErrorCode


class CheckNoCheckSum(BaseItem):
    def __init__(self):
        super(CheckNoCheckSum, self).__init__(self.__class__.__name__)

    def getOSversion(self):
        distname, version, idnum = g_Platform.dist()
        return distname, version

    def doCheck(self):
        if (not os.path.isfile("/sys/module/sctp/parameters/no_checksums")):
            self.result.rst = ResultStatus.OK
            self.result.val = "The SCTP service is not used and the" \
                              " check item is skipped"
            return
        expect = "N"
        if (self.cluster):
            LocalNodeInfo = self.cluster.getDbNodeByName(self.host)
            serviceIP = LocalNodeInfo.backIps[0]
        else:
            serviceIP = SharedFuncs.getIpByHostName(self.host)
        for network in g_network.getAllNetworkInfo():
            if (network.ipAddress == serviceIP):
                networkCardNum = network.NICNum
                networkBond = network.networkBondModeInfo
                break
        if (not networkCardNum or not networkBond):
            raise Exception(ErrorCode.GAUSS_506["GAUSS_50619"])
        (distname, version) = self.getOSversion()
        if ((distname in ("redhat", "centos")) and
                (version in ("6.4", "6.5")) and
                networkBond != "BondMode Null"):
            expect = "Y"

        output = \
            g_file.readFile('/sys/module/sctp/parameters/no_checksums')[0]
        if (output.strip() == expect):
            self.result.rst = ResultStatus.OK
            self.result.val = "Nochecksum value is %s,Check items pass." \
                              % output.strip()
        else:
            self.result.rst = ResultStatus.NG
            self.result.val = "Nochecksum value(%s) is not %s," \
                              "Check items are not passed." \
                              % (output.strip(), expect)
