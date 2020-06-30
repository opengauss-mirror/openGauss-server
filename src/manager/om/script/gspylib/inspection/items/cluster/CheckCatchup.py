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


class CheckCatchup(BaseItem):
    def __init__(self):
        super(CheckCatchup, self).__init__(self.__class__.__name__)

    def doCheck(self):
        cmd = "ps -ef |grep '^<%s\>' | grep '\<gaussdb\>' | grep -v grep |" \
              " awk '{print $2}' |(while read arg; do gstack $arg |" \
              " grep CatchupMain; done) 2>/dev/null" % self.user
        output = SharedFuncs.runShellCmd(cmd)
        if (output != ""):
            self.result.rst = ResultStatus.NG
            self.result.val = "The gatchdb process stack contains the" \
                              " CatchupMain function."
        else:
            self.result.rst = ResultStatus.OK
            self.result.val = "The gatchdb process stack not contains" \
                              " the CatchupMain function."
        self.result.raw = cmd
