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
from gspylib.os.gsfile import g_file
from gspylib.os.gsnetwork import g_network
from gspylib.os.gsfile import g_Platform

networkCards = []


class CheckBond(BaseItem):
    def __init__(self):
        super(CheckBond, self).__init__(self.__class__.__name__)

    def doCheck(self):
        global networkCards
        if (self.cluster):
            # Get node information
            LocalNodeInfo = self.cluster.getDbNodeByName(self.host)
            # Get the IP address
            serviceIP = LocalNodeInfo.backIps[0]
        elif (self.ipAddr):
            serviceIP = self.ipAddr
        else:
            serviceIP = SharedFuncs.getIpByHostName(self.host)
        networkCards = g_network.getAllNetworkInfo()
        for network in networkCards:
            if (network.ipAddress == serviceIP):
                networkCardNum = network.NICNum
                netBondMode = network.networkBondModeInfo
                break

        self.result.val = netBondMode
        self.result.rst = ResultStatus.OK
        self.result.raw = "%s\n%s\n" % (networkCardNum, netBondMode)

        bondFile = '/proc/net/bonding/%s' % networkCardNum
        if (os.path.exists(bondFile)):
            self.result.raw += bondFile
            flag1 = g_file.readFile(bondFile, 'BONDING_OPTS')
            flag2 = g_file.readFile(bondFile, 'BONDING_MODULE_OPTS')
            if (not flag1 and not flag2):
                self.result.rst = ResultStatus.NG
                self.result.val += "\nNo 'BONDING_OPTS' or" \
                                   " 'BONDING_MODULE_OPTS' in bond" \
                                   " config file[%s]." % bondFile

    def doSet(self):
        ifcfgFileSuse = "/etc/sysconfig/network/ifcfg-%s" % networkCards
        ifcfgFileRedhat = "/etc/sysconfig/network-scripts/ifcfg-%s" \
                          % networkCards
        distname, version, idnum = g_Platform.dist()
        if (distname in ["redhat", "centos", "euleros", "openEuler"]):
            cmd = "echo BONDING_MODULE_OPTS='mode=%d " \
                  "miimon=100 use_carrier=0' >> %s " % (1, ifcfgFileRedhat)
        else:
            cmd = "echo BONDING_MODULE_OPTS='mode=%d miimon=100" \
                  " use_carrier=0' >> %s" % (1, ifcfgFileSuse)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            self.result.val = "Failed to set bond mode.\n" + \
                              "The cmd is %s " % cmd
        else:
            self.result.val = "set bond mode successfully.\n"
