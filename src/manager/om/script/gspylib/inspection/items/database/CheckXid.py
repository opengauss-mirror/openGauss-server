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


class CheckXid(BaseItem):
    def __init__(self):
        super(CheckXid, self).__init__(self.__class__.__name__)

    def doCheck(self):
        sqlcmd = "select txid_current();"
        output = SharedFuncs.runSqlCmd(sqlcmd, self.user, "", self.port,
                                       self.tmpPath, "postgres",
                                       self.mpprcFile)
        num = int(output)
        self.result.raw = sqlcmd
        self.result.val = "The xid value is %s" % output
        if (num <= 1000000000):
            self.result.rst = ResultStatus.OK
        elif (num <= 1800000000):
            self.result.rst = ResultStatus.WARNING
        else:
            self.result.rst = ResultStatus.NG
