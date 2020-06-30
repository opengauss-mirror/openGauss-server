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
# ----------------------------------------------------------------------------s
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.hardware.gscpu import CpuInfo


class CheckCpuCount(BaseItem):
    def __init__(self):
        super(CheckCpuCount, self).__init__(self.__class__.__name__)

    def doCheck(self):

        parRes = ""
        flag = "Normal"
        cpuCount = CpuInfo.getCpuNum()

        output_online = CpuInfo.getCpuOnlineOfflineInfo()
        num = len(output_online.split('-'))
        firstValue = output_online.split('-')[0].strip()
        lastValue = output_online.split('-')[1].strip()

        output_offline = CpuInfo.getCpuOnlineOfflineInfo(False)

        if (num != 2 or int(firstValue) != 0 or int(
                lastValue) != cpuCount - 1):
            flag = "Error"
            parRes += "it exists unavailable CPU.\n " \
                      "online: %s.\n offline: %s." % (
                          output_online, output_offline)
        if (output_offline.strip() != "" and flag == "Normal"):
            flag = "Warning"

        if (flag == "Error"):
            self.result.rst = ResultStatus.NG
        elif (flag == "Warning"):
            self.result.rst = ResultStatus.WARNING
        else:
            self.result.rst = ResultStatus.OK

        self.result.val = "cpuCount: %d, online: %s, offline: %s." % (
            cpuCount, output_online, output_offline)
        self.result.raw = parRes
