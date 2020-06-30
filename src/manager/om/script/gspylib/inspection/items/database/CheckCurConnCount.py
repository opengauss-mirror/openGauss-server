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


class CheckCurConnCount(BaseItem):
    def __init__(self):
        super(CheckCurConnCount, self).__init__(self.__class__.__name__)

    def doCheck(self):
        sqlcmd1 = "show max_connections;"
        sqlcmd2 = "SELECT count(*) FROM pg_stat_activity;"
        self.result.raw = sqlcmd1 + sqlcmd2
        output1 = SharedFuncs.runSqlCmd(sqlcmd1, self.user, "", self.port,
                                        self.tmpPath, "postgres",
                                        self.mpprcFile)
        output2 = SharedFuncs.runSqlCmd(sqlcmd2, self.user, "", self.port,
                                        self.tmpPath, "postgres",
                                        self.mpprcFile)
        if (not (output1.isdigit() and output2.isdigit())):
            self.result.rst = ResultStatus.ERROR
            self.result.val = "max_connections: %s\nCurConnCount: %s" % (
                output1, output2)
        maxConnections = float(output1)
        usedConnections = float(output2)
        if (maxConnections > 0 and usedConnections > 0):
            OccupancyRate = (usedConnections // maxConnections)
            self.result.val = "%.2f%%" % (OccupancyRate * 100)
            if (OccupancyRate < 0.9):
                self.result.rst = ResultStatus.OK
            else:
                self.result.rst = ResultStatus.NG
        else:
            self.result.rst = ResultStatus.ERROR
            self.result.val = "max_connections: %s\nCurConnCount: %s" % (
                maxConnections, usedConnections)
