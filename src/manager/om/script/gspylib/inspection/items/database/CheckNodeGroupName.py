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

g_setDict = {}


class CheckNodeGroupName(BaseItem):
    def __init__(self):
        super(CheckNodeGroupName, self).__init__(self.__class__.__name__)

    def doCheck(self):
        global g_setDict
        databaseListSql = "select datname from pg_database where datname != " \
                          "'template0';"
        sqlCmd = "select group_name from pgxc_group where length(group_name)" \
                 " != length(group_name::bytea, 'SQL_ASCII');"
        output = SharedFuncs.runSqlCmd(databaseListSql, self.user, "",
                                       self.port, self.tmpPath, "postgres",
                                       self.mpprcFile)
        dbList = output.split("\n")
        resultStr = ""
        for databaseName in dbList:
            output = SharedFuncs.runSqlCmd(sqlCmd, self.user, "", self.port,
                                           self.tmpPath, databaseName,
                                           self.mpprcFile, True)
            if not output:
                continue
            else:
                g_setDict[databaseName] = output
                resultStr += "The node group name of %s with non-SQL_ASCII " \
                             "characters.\n " % databaseName
        if (resultStr):
            self.result.rst = ResultStatus.NG
            self.result.val = resultStr
        else:
            self.result.rst = ResultStatus.OK
            self.result.val = "The node group name with SQL_ASCII characters" \
                              " in all databases."

    def doSet(self):
        resultStr = ""
        i = 2
        for dbname in g_setDict.keys():
            for groupname in g_setDict[dbname]:
                sqlCmd = "set xc_maintenance_mode=on;"
                sqlCmd += "alter node group '%s' rename to " \
                          "'groupversion%d';" % (
                              groupname, i)
                sqlCmd += "set xc_maintenance_mode=off;"
                output = SharedFuncs.runSqlCmd(sqlCmd, self.user, "",
                                               self.port, self.tmpPath, dbname,
                                               self.mpprcFile, True)
                i += 1
                resultStr += output
        self.result.val = resultStr
