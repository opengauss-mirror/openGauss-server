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


class CheckPgPreparedXacts(BaseItem):
    def __init__(self):
        super(CheckPgPreparedXacts, self).__init__(self.__class__.__name__)

    def doCheck(self):
        sqlcmd = "SELECT * FROM pg_catalog.pgxc_prepared_xacts;"
        self.result.raw = sqlcmd

        output = SharedFuncs.runSqlCmd(sqlcmd, self.user, "", self.port,
                                       self.tmpPath, "postgres",
                                       self.mpprcFile)
        if (output != ""):
            self.result.rst = ResultStatus.NG
        else:
            self.result.rst = ResultStatus.OK
        self.result.val = output

    def doSet(self):
        cmd = "gs_clean -s -v -a -p %s -r" % self.port
        output = SharedFuncs.runShellCmd(cmd, self.user, self.mpprcFile)
        self.result.val = output
