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


class CheckHashIndex(BaseItem):
    def __init__(self):
        super(CheckHashIndex, self).__init__(self.__class__.__name__)

    def doCheck(self):
        databaseListSql = "select datname from pg_database " \
                          "where datname != 'template0';"
        sqlcmd = """
SELECT
n.nspname AS schemaname,
c.relname AS tablename,
i.relname AS indexname,
o.amname  AS indexmethod,
pg_get_indexdef(i.oid) AS indexdef
FROM pg_index x
JOIN pg_class c ON c.oid = x.indrelid
JOIN pg_class i ON i.oid = x.indexrelid
JOIN pg_am    o ON o.oid = i.relam
LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'::"char"
AND i.relkind = 'i'::"char"
AND o.amname not in ('btree','gin','psort','cbtree');
"""
        output = SharedFuncs.runSqlCmd(databaseListSql, self.user, "",
                                       self.port, self.tmpPath, "postgres",
                                       self.mpprcFile)
        dbList = output.split("\n")
        resultStr = ""
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
            self.result.val = "There is no hash index in all databases."
