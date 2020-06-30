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
import time
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus


class CheckDropColumn(BaseItem):
    def __init__(self):
        super(CheckDropColumn, self).__init__(self.__class__.__name__)

    def doCheck(self):
        sql1 = """select a.relname, b.attname ,n.nspname||'.'||a.relname 
        from pg_class a, pg_attribute b, pg_namespace n 
        where a.oid = b.attrelid 
        and b.attisdropped and n.oid = a.relnamespace;"""
        sqldb = "select datname from pg_database;"
        output = SharedFuncs.runSqlCmd(sqldb, self.user, "", self.port,
                                       self.tmpPath, "postgres",
                                       self.mpprcFile)
        dbList = output.split("\n")
        dbList.remove("template0")
        result = ""
        for db in dbList:
            output1 = SharedFuncs.runSqlSimplely(sql1, self.user, "",
                                                 self.port, self.tmpPath,
                                                 "postgres", self.mpprcFile)
            if (output1.find("(0 rows)") < 0):
                result += "%s:\n%s\n" % (db, output1)
        if (result):
            self.result.val = "Alter table drop column operation " \
                              "is did in :\n%s" % result
            self.result.rst = ResultStatus.NG
            self.result.raw = sql1
        else:
            self.result.val = "No alter table drop column operation"
            self.result.rst = ResultStatus.OK
