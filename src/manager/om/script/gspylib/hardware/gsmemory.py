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
# Description  : memory.py is a utility to do something for memory information.
#############################################################################
import sys
import psutil

sys.path.append(sys.path[0] + "/../../")
from gspylib.common.ErrorCode import ErrorCode

"""
Requirements:
get memory and swap size
"""


class memoryInfo(object):
    """
    function: Init the MemInfo options
    """

    def __init__(self):
        """
        function: Init the MemInfo options
        """

    @staticmethod
    def getMemUsedSize():
        """
        get used memory size
        """
        return psutil.virtual_memory().used

    @staticmethod
    def getMemFreeSize():
        """
        get free memory size
        """
        return psutil.virtual_memory().free

    @staticmethod
    def getSwapUsedSize():
        """
        get used swap size
        """
        return psutil.swap_memory().used

    @staticmethod
    def getSwapFreeSize():
        """
        get free swap size
        """
        return psutil.swap_memory().free

    @staticmethod
    def getSwapTotalSize():
        """
        function : Get swap memory total size
        input  : null
        output : total  memory size (byte)
        """
        total = 0
        try:
            total = psutil.swap_memory().total
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_505["GAUSS_50502"]
                            + "Error: %s" % str(e))
        return total

    @staticmethod
    def getMemTotalSize():
        """
        function : Get system virtual memory total size
        input  : null
        output : total virtual memory(byte)
        """
        total = 0
        try:
            total = psutil.virtual_memory().total
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_505["GAUSS_50502"]
                            + "Error: %s" % str(e))
        return total


g_memory = memoryInfo()
