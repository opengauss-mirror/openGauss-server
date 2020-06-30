# coding: UTF-8
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
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus


class CheckDropCache(BaseItem):
    def __init__(self):
        super(CheckDropCache, self).__init__(self.__class__.__name__)

    def doCheck(self):
        checkdropCacheCmd = "ps -ef| grep 'dropc'|grep -v 'grep'"
        (status, output) = subprocess.getstatusoutput(checkdropCacheCmd)
        if (status == 0):
            if (output):
                self.result.rst = ResultStatus.OK
                self.result.val = "The DropCache process is running"
            else:
                self.result.rst = ResultStatus.WARNING
                self.result.val = "No DropCache process is running"
        else:
            self.result.rst = ResultStatus.WARNING
            self.result.val = "No DropCache process is running"
