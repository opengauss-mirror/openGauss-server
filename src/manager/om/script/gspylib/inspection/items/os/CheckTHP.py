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
import os
import subprocess
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.os.gsfile import g_file

THPFile = "/sys/kernel/mm/transparent_hugepage/enabled"


class CheckTHP(BaseItem):
    def __init__(self):
        super(CheckTHP, self).__init__(self.__class__.__name__)

    def collectTHPServer(self):
        if (os.path.exists(THPFile)):
            output = g_file.readFile(THPFile)[0]
            self.result.raw = output
            if (output.find('[never]') > 0):
                THPstatus = "disabled"
            else:
                THPstatus = "enabled"
        else:
            THPstatus = "disabled"
        return THPstatus

    def doCheck(self):
        THPstatus = self.collectTHPServer()
        if (THPstatus != "disabled"):
            self.result.rst = ResultStatus.NG
            self.result.val = "The THP server is '%s', " \
                              "ExpectedValue: 'disabled'." % THPstatus
        else:
            self.result.rst = ResultStatus.OK
            self.result.val = THPstatus

    def doSet(self):
        close_cmd = "(if test -f %s; then echo never > %s;fi)" % (
            THPFile, THPFile)
        (status, output) = subprocess.getstatusoutput(close_cmd)
        if (status != 0):
            self.result.val = "Failed to close THP service, " \
                              "Error: %s\n" % output + \
                              "The cmd is %s " % close_cmd
            return
        # 2.add close cmd to init file
        initFile = SharedFuncs.getInitFile()
        cmd = "sed -i '/^.*transparent_hugepage.*enabled.*echo " \
              "never.*$/d' %s &&" % initFile
        cmd += "echo \"%s\" >> %s" % (close_cmd, initFile)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            self.result.val += "Failed to add cmd to init file, " \
                               "Error: %s\n" % output + "The cmd is %s " % cmd
