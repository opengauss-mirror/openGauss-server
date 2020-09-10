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
from gspylib.os.gsfile import g_file
from gspylib.common.Common import DefaultValue


class CheckHyperThread(BaseItem):
    def __init__(self):
        super(CheckHyperThread, self).__init__(self.__class__.__name__)

    def doCheck(self):
        flag = "open"
        idList = []
        idCount = 0
        cores = 0
        cpuCount = 0
        cpuInfo = g_file.readFile('/proc/cpuinfo')
        for eachLine in cpuInfo:
            if (eachLine.find('physical id') >= 0):
                # get different CPU id
                cpuID = eachLine.split(':')[1].strip()
                if (not cpuID in idList):
                    idList.append(cpuID)
                    # Calculate the number of CPUs
                    idCount += 1
            if (eachLine.find('cores') >= 0):
                cores = int(eachLine.split(':')[1].strip())
            if (eachLine.find('processor') >= 0):
                cpuCount += 1

        if (cpuCount == 2 * idCount * cores):
            self.result.rst = ResultStatus.OK
        else:
            if DefaultValue.checkDockerEnv():
                return
            flag = "down"
            self.result.rst = ResultStatus.NG

        self.result.val = "Hyper-threading is %s." % flag
        self.result.raw = "the number of physical id: %d, cores: %d," \
                          " cpu counts: %d" % (idCount, cores, cpuCount)
