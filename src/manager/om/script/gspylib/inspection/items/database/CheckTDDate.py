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


class CheckTDDate(BaseItem):
    def __init__(self):
        super(CheckTDDate, self).__init__(self.__class__.__name__)

    def doCheck(self):
        databaseListSql = "select datname from pg_database " \
                          "where datcompatibility = 'TD';"
        self.result.raw = databaseListSql
        output = SharedFuncs.runSqlCmd(databaseListSql, self.user, "",
                                       self.port, self.tmpPath, "postgres",
                                       self.mpprcFile)
        if (not output.strip()):
            self.result.val = "The database with TD mode does not exist."
            self.result.rst = ResultStatus.OK
            return
        dbList = output.strip().split("\n")
        self.result.raw = "The database of TD mode is: %s\n" % ','.join(
            output.split('\n'))
        resultStr = ""
        sqlcmd = """
select ns.nspname as namespace, c.relname as table_name, 
attr.attname as column_name
from pg_attribute attr, pg_class c , pg_namespace ns
where attr.attrelid = c.oid
and ns.oid = c.relnamespace
and array_to_string(c.reloptions, ', ') like '%orientation=orc%'
and attr.atttypid = (select oid from pg_type where typname='date')
union all
select ns.nspname as namespace, c.relname as table_name, 
attr.attname as column_name
from pg_attribute attr, pg_class c , pg_namespace ns, pg_foreign_table ft
where attr.attrelid = c.oid
and c.oid = ft.ftrelid
and ns.oid = c.relnamespace
and array_to_string(ft.ftoptions, ', ') like '%format=orc%'
and attr.atttypid = (select oid from pg_type where typname='date');
"""
        for databaseName in dbList:
            output = SharedFuncs.runSqlCmd(sqlcmd, self.user, "", self.port,
                                           self.tmpPath, databaseName,
                                           self.mpprcFile, True)
            if (output):
                self.result.raw += "%s: %s" % (databaseName, output)
                tableList = output.split('\n')
                resultStr += "database[%s]: %s\n" % (
                    databaseName, ",".join(tableList))
        if (resultStr):
            self.result.rst = ResultStatus.NG
            self.result.val = resultStr
        else:
            self.result.rst = ResultStatus.OK
            self.result.val = "The orc table with the date column " \
                              "in the TD schema database does not exist."
