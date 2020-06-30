# coding: UTF-8
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
import json
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus


class CheckDiskFailure(BaseItem):
    def __init__(self):
        super(CheckDiskFailure, self).__init__(self.__class__.__name__)

    def doCheck(self):
        if (self.cluster.isSingleInstCluster()):
            try:
                sqltables = "select b.nspname, a.relname, reloptions from " \
                            "pg_class a, pg_namespace b where a.relnamespace" \
                            " = " \
                            "b.oid and b.nspname !~ '^pg_toast' and " \
                            "a.relkind='r' and a.relpersistence='p';"
                sqlForCustom = "select b.nspname, a.relname, reloptions " \
                               "from pg_class a, pg_namespace b where " \
                               "a.relnamespace = b.oid and b.nspname !~ " \
                               "'^pg_toast' and a.relkind='r' and " \
                               "a.relpersistence='p' and b.nspname <> " \
                               "'pg_catalog' and b.nspname <> 'cstore' and" \
                               " b.nspname <> 'information_schema' and " \
                               "b.nspname <> 'schema_cur_table_col' and " \
                               "b.nspname <> 'schema_cur_table' and " \
                               "b.nspname !~ '^pg_toast';"
                sqldb = "select datname from pg_database;"
                output = SharedFuncs.runSqlCmd(sqldb, self.user, "", self.port,
                                               self.tmpPath, "postgres",
                                               self.mpprcFile)
                dbList = output.splitlines()
                dbList.remove("template0")
                dbList.remove("template1")
                for db in dbList:
                    coltable = []
                    rowtable = []
                    if (db == "postgres"):
                        sql = sqltables
                    else:
                        sql = sqlForCustom
                    output = SharedFuncs.runSqlCmd(sql, self.user, "",
                                                   self.port, self.tmpPath,
                                                   db, self.mpprcFile)
                    tablelist = output.splitlines()
                    sql = ""
                    for tableinfo in tablelist:
                        if (len(tableinfo.split("|")) == 3):
                            schema = tableinfo.split("|")[0].strip()
                            tablename = tableinfo.split("|")[1].strip()
                            reloptions = tableinfo.split("|")[2].strip()
                            if ("column" in reloptions):
                                coltable.append("%s.%s" % (schema, tablename))
                            else:
                                rowtable.append("%s.%s" % (schema, tablename))
                        else:
                            pass
                    for table in rowtable:
                        sql += "select count(*) from %s;\n" % table
                    for table in coltable:
                        sql += "explain analyze select * from %s;\n" % table
                    SharedFuncs.runSqlCmd(sql, self.user, "", self.port,
                                          self.tmpPath, db, self.mpprcFile)
            except Exception as e:
                self.result.rst = ResultStatus.NG
                self.result.val = str(e)
                return
            self.result.rst = ResultStatus.OK
            self.result.val = "No data is distributed on the fault disk"
        else:
            self.result.rst = ResultStatus.NA
            self.result.val = "First cn is not in this host"
