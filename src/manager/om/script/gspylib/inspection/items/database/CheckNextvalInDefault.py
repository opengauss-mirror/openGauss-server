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
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus


class CheckNextvalInDefault(BaseItem):
    def __init__(self):
        super(CheckNextvalInDefault, self).__init__(self.__class__.__name__)

    def doCheck(self):
        sql1 = """select distinct rt.relname from PG_ATTRDEF ad, 
(
select c.oid,c.relname from pg_class c, pgxc_class xc
where
c.oid = xc.pcrelid and  
c.relkind = 'r' and
xc.pclocatortype = 'R'
) as rt(oid,relname)
where ad.adrelid = rt.oid
and ad.adsrc like '%nextval%';
        """
        sql2 = """select relname from pg_class c, pg_namespace n
where relkind = 'S' and c.relnamespace = n.oid
and n.nspname like 'pg_temp%';
"""
        sqldb = "select datname from pg_database;"
        output = SharedFuncs.runSqlCmd(sqldb, self.user, "", self.port,
                                       self.tmpPath, "postgres",
                                       self.mpprcFile)
        dbList = output.split("\n")
        dbList.remove("template0")
        result = ""
        for db in dbList:
            output1 = SharedFuncs.runSqlCmd(sql1, self.user, "", self.port,
                                            self.tmpPath, db, self.mpprcFile)
            tmptablist = []
            if (output1):
                for tab in output1.splitlines():
                    tmpsql = "select * from %s limit 1" % tab
                    tmpout = SharedFuncs.runSqlCmd(tmpsql, self.user, "",
                                                   self.port, self.tmpPath, db,
                                                   self.mpprcFile)
                    if (tmpout):
                        tmptablist.append(tab)
            else:
                pass
            output2 = SharedFuncs.runSqlCmd(sql2, self.user, "", self.port,
                                            self.tmpPath, db, self.mpprcFile)
            if (output2):
                for tab in output2.splitlines():
                    if (tab not in tmptablist):
                        tmptablist.append(tab)
            if (tmptablist):
                result += "%s:\n%s\n" % (db, "\n".join(tmptablist))
        if (result):
            self.result.val = "there is some default expression " \
                              "contains nextval(sequence):\n%s" % result
            self.result.rst = ResultStatus.NG
        else:
            self.result.val = "no default expression " \
                              "contains nextval(sequence)"
            self.result.rst = ResultStatus.OK
