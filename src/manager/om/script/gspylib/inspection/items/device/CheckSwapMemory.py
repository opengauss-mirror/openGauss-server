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


class CheckSwapMemory(BaseItem):
    def __init__(self):
        super(CheckSwapMemory, self).__init__(self.__class__.__name__)

    def doCheck(self):
        MemSwap = 0
        self.result.raw = ""
        result_swap = g_file.readFile('/proc/meminfo', "SwapTotal")[0]
        self.result.raw += result_swap
        swapInfo = result_swap.strip().split(' ')
        val = int(swapInfo[len(swapInfo) - 2])
        factor = swapInfo[len(swapInfo) - 1]
        if factor == 'kB':
            MemSwap = val * 1024
        elif (factor == ''):
            MemSwap = val

        result_mem = g_file.readFile('/proc/meminfo', "MemTotal")[0]
        self.result.raw += "\n%s" % result_mem
        memInfo = result_mem.strip().split()
        val = int(memInfo[len(memInfo) - 2])
        factor = memInfo[len(memInfo) - 1]
        if factor == 'kB':
            MemTotal = val * 1024
        elif (factor == ''):
            MemTotal = val

        if (MemSwap > MemTotal):
            self.result.rst = ResultStatus.NG
            self.result.val = "SwapMemory(%d) must be 0.\nMemTotal: %d." % (
                MemSwap, MemTotal)
        elif (MemSwap != 0):
            self.result.rst = ResultStatus.WARNING
            self.result.val = "SwapMemory(%d) must be 0.\nMemTotal: %d." % (
                MemSwap, MemTotal)
        else:
            self.result.rst = ResultStatus.OK
            self.result.val = "SwapMemory %d\nMemTotal %d." % (
                MemSwap, MemTotal)

    def doSet(self):
        resultStr = ""
        configFile = "/etc/fstab"
        cmd = "swapoff -a"
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            resultStr += "Failed to close swap information.\n Error : %s." \
                         % output
            resultStr += "The cmd is %s " % cmd
        cmd = "sed -i '/^.*swap/d' %s" % configFile
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            resultStr += "Failed to modify configuration file.\n Error : %s." \
                         % output
            resultStr += "The cmd is %s " % cmd
        if (len(resultStr) > 0):
            self.result.val = resultStr
        else:
            self.result.val = "Set SwapMemory successfully."
