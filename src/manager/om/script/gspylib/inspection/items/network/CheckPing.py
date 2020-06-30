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
import multiprocessing
import _thread as thread
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.threads.parallelTool import parallelTool
from gspylib.os.gsnetwork import g_network

DEFAULT_PARALLEL_NUM = 12
g_lock = thread.allocate_lock()
noPassIPs = []


class CheckPing(BaseItem):
    def __init__(self):
        super(CheckPing, self).__init__(self.__class__.__name__)

    def doCheck(self):
        global noPassIPs
        allIP = []
        LocalNodeInfo = self.cluster.getDbNodeByName(self.host)
        allIP += LocalNodeInfo.backIps
        allIP += LocalNodeInfo.sshIps
        for dbInstance in LocalNodeInfo.datanodes:
            allIP += dbInstance.haIps
            allIP += dbInstance.listenIps

        sorted(allIP)
        for i in range(len(allIP) - 2, -1, -1):
            if allIP.count(allIP[i]) > 1:
                del allIP[i]
        noPassIPs = g_network.checkIpAddressList(allIP)
        if noPassIPs == []:
            self.result.rst = ResultStatus.OK
            self.result.raw = "All IP can pinged."
        else:
            self.result.rst = ResultStatus.NG
            self.result.raw = "The following IP can not pinged: \n%s" \
                              % noPassIPs
