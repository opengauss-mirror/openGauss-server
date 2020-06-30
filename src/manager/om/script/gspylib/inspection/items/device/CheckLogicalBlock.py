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
import glob
import subprocess
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.os.gsfile import g_file

g_expectedScheduler = "512"
result = {}


class CheckLogicalBlock(BaseItem):
    def __init__(self):
        super(CheckLogicalBlock, self).__init__(self.__class__.__name__)

    def doCheck(self):
        global result
        devices = set()
        self.result.val = ""

        files = glob.glob("/sys/block/*/queue/logical_block_size")
        for f in files:
            words = f.split("/")
            if len(words) != 6:
                continue
            devices.add(words[3].strip())

        for d in devices:
            request = \
                g_file.readFile("/sys/block/%s/queue/logical_block_size" % d)[
                    0]
            result[d] = request.strip()

        if len(result) == 0:
            self.result.val = "Warning:Not find logical block file," \
                              "please check it."
            self.result.rst = ResultStatus.WARNING

        flag = True
        for i in result.keys():
            reuqest = result[i]
            self.result.raw += "%s %s\n" % (i, reuqest)
            if (i.startswith('loop') or i.startswith('ram')):
                continue
            if int(reuqest) < int(g_expectedScheduler):
                flag = False
                self.result.val += "\nWarning:On device (%s) '" \
                                   "logicalBlock Request' RealValue '%d' " \
                                   "ExpectedValue '%d'" % (
                                       i, int(reuqest),
                                       int(g_expectedScheduler))

        if flag:
            self.result.rst = ResultStatus.OK
            self.result.val = "All disk LogicalBlock values are correct."
        else:
            self.result.rst = ResultStatus.NG

    def doSet(self):
        resultStr = ""
        for dev in result.keys():
            (THPFile, initFile) = SharedFuncs.getTHPandOSInitFile()
            cmd = " echo %s >> /sys/block/%s/queue/logical_block_size" % (
                g_expectedScheduler, dev)
            cmd += \
                " && echo \"echo %s >> " \
                "/sys/block/%s/queue/logical_block_size\" >> %s" % (
                    g_expectedScheduler, dev, initFile)
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                resultStr = "Failed to set logicalBlock Request.\n " \
                            "Error : %s." % output
                resultStr += "The cmd is %s " % cmd
        self.result.val = resultStr
