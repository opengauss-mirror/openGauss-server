# -*- coding:utf-8 -*-
#############################################################################
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
# Description  : cpu.py is a utility to do something for cpu information.
#############################################################################
import os
import subprocess
import sys
import multiprocessing

sys.path.append(sys.path[0] + "/../../")
from gspylib.common.ErrorCode import ErrorCode

"""
Requirements:
1. getCpuNum() -> get real cpu number.
2. getCpuOnlineOfflineInfo(isOnlineCpu) -> get cpu online/offline information
"""


class CpuInfo(object):
    """
    function: Init the CpuInfo options
    """

    def __init__(self):
        """
        function: Init the CpuInfo options
        """

    @staticmethod
    def getCpuNum():
        """
        function : get cpu set of current board
        input  : null
        output : total CPU count
        """
        total = 0
        try:
            total = multiprocessing.cpu_count()
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_523["GAUSS_52301"] + str(e))
        return total

    @staticmethod
    def getCpuOnlineOfflineInfo(isOnlineCpu=True):
        """
        cat /sys/devices/system/cpu/online or /sys/devices/system/cpu/offline
        """
        onlineFileName = "/sys/devices/system/cpu/online"
        offlineFileName = "/sys/devices/system/cpu/offline"

        if (isOnlineCpu):
            fileName = onlineFileName
        else:
            fileName = offlineFileName

        if (not os.path.exists(fileName)):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % fileName)
        if (not os.path.isfile(fileName)):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50210"] % fileName)

        cmd = "cat '%s' 2>/dev/null" % fileName
        status, output = subprocess.getstatusoutput(cmd)
        if (status == 0):
            return output
        else:
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                            " Error: \n%s" % str(output))


g_cpu = CpuInfo()
