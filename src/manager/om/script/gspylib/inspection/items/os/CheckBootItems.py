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

import platform
import subprocess
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus


class CheckBootItems(BaseItem):
    def __init__(self):
        super(CheckBootItems, self).__init__(self.__class__.__name__)

    def doCheck(self):
        self.result.rst = ResultStatus.OK
        checkItems = ["checksum", "mtu", "cgroup", "rx", "tx"]
        bootitem = []
        bootfile = ""
        if SharedFuncs.isSupportSystemOs():
            bootfile = "/etc/rc.d/rc.local"
        else:
            bootfile = "/etc/init.d/boot.local"
        for item in checkItems:
            cmd = "grep -i %s %s" % (item, bootfile)
            (status, output) = subprocess.getstatusoutput(cmd)
            if (output):
                bootitem.append(item)
                self.result.rst = ResultStatus.NG
        if (self.result.rst == ResultStatus.OK):
            self.result.val = "no boot item added"
        else:
            self.result.val = "boot items is added:\n%s" % "\n".join(bootitem)
