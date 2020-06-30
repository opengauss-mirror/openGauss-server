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
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.common.ErrorCode import ErrorCode
from gspylib.os.gsfile import g_file


class CheckPortRange(BaseItem):
    def __init__(self):
        super(CheckPortRange, self).__init__(self.__class__.__name__)
        self.ip_local_port_range = None

    def preCheck(self):
        # check current node contains cn instances if not raise  exception
        super(CheckPortRange, self).preCheck()
        # check the threshold was set correctly
        if (not self.threshold.__contains__('ip_local_port_range')):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"]
                            % "threshold ip_local_port_range")
        self.ip_local_port_range = self.threshold['ip_local_port_range']

    def getPort(self):
        cooInst = None
        portList = {}
        dbNode = self.cluster.getDbNodeByName(self.host)
        for dnInst in dbNode.datanodes:
            portList[dnInst.port] = dnInst.instanceRole
            portList[dnInst.haPort] = dnInst.instanceRole

        return portList

    def doCheck(self):
        parRes = ""
        flag = None
        instance = {0: "CMSERVER", 1: "GTM", 2: "ETCD", 3: "COODINATOR",
                    4: "DATANODE", 5: "CMAGENT"}
        portList = self.getPort()
        # Check the port range
        output = g_file.readFile('/proc/sys/net/ipv4/ip_local_port_range')[
            0].strip()
        smallValue = output.split('\t')[0].strip()
        bigValue = output.split('\t')[1].strip()
        expect = self.ip_local_port_range.split()
        if (int(smallValue) < int(expect[0].strip()) or int(bigValue) > int(
                expect[1].strip())):
            parRes += "The value of net.ipv4.ip_local_port_range is" \
                      " incorrect, expect value is %s.\n" \
                      % self.ip_local_port_range
        parRes += "The value of net.ipv4.ip_local_port_range is %d %d." \
                  % (int(smallValue), int(bigValue))

        for port in portList.keys():
            if (int(port) <= int(bigValue) and int(port) >= int(smallValue)):
                flag = 1
                parRes += "\n            %s" \
                          % ("The instance %s port \"%d\" is incorrect."
                             % (instance[portList[port]], int(port)))
        if (flag == 1):
            self.result.rst = ResultStatus.NG
        else:
            self.result.rst = ResultStatus.OK
        self.result.val = parRes
        self.result.raw = output
