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
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.os.gsfile import g_file


class CheckEtcHosts(BaseItem):
    def __init__(self):
        super(CheckEtcHosts, self).__init__(self.__class__.__name__)

    def doCheck(self):
        flag = "Normal"
        conflictsMapping = []
        commentsMapping = []
        IPMapping = {}
        IpList = []

        mappingList = g_file.readFile('/etc/hosts')
        for eachLine in mappingList:
            eachLine = eachLine.strip()
            if (eachLine == ""):
                continue
            if (not eachLine.startswith('#') and '::' not in eachLine):
                mappingInfo = " ".join(eachLine.split())
                IpList.append(mappingInfo)
        sorted(IpList)
        self.result.raw = "\n".join(IpList)

        # Check localhost Mapping
        localHost = False
        for eachIP in IpList:
            if (eachIP.find("127.0.0.1 localhost") == 0):
                localHost = True
                break
        if (not localHost):
            self.result.rst = ResultStatus.NG
            self.result.val = "The /etc/hosts does not match localhosts."
            return

        # Check conflicts Mapping and GAUSS comments Mapping
        for IPInfo in IpList:
            ipHost = IPInfo.split()
            if (len(ipHost) < 2):
                continue
            ip = IPInfo.split()[0]
            host = IPInfo.split()[1]
            if (ip == "127.0.0.1"):
                continue
            if (ip in IPMapping.keys() and host != IPMapping[ip]):
                conflictsMapping.append(IPInfo)
                conflictsMapping.append("%s %s" % (ip, IPMapping[ip]))
                flag = "Error_conflicts"
            else:
                IPMapping[ip] = host
            if (len(IPInfo.split()) > 2 and IPInfo.split()[2] == "#Gauss"):
                commentsMapping.append(IPInfo + " IP Hosts Mapping")
                flag = "Error_comments"

        if (flag == "Normal"):
            self.result.rst = ResultStatus.OK
            self.result.val = "The /etc/hosts is configured correctly."
        elif (flag == "Error_comments"):
            self.result.rst = ResultStatus.NG
            self.result.val = "The /etc/hosts has comments Mapping:\n" \
                              + "\n".join(
                commentsMapping)
        else:
            self.result.rst = ResultStatus.NG
            self.result.val = "The /etc/hosts has conflicts Mapping:\n" \
                              + "\n".join(
                conflictsMapping)
            if (len(commentsMapping) > 0):
                self.result.val += "\n\nThe /etc/hosts has " \
                                   "comments Mapping:\n" + "\n".join(
                    commentsMapping)
