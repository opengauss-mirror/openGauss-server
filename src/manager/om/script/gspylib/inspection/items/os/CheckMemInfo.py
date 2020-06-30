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
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.hardware.gsmemory import g_memory


class CheckMemInfo(BaseItem):
    def __init__(self):
        super(CheckMemInfo, self).__init__(self.__class__.__name__)

    def doCheck(self):
        totalMem_bit = g_memory.getMemTotalSize()
        totalMem_g = totalMem_bit / 1024 / 1024 / 1024
        self.result.rst = ResultStatus.OK
        self.result.raw = "%s bit" % totalMem_bit
        self.result.val = "totalMem: %sG" % totalMem_g
