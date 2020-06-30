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


class CheckPgxcRedistb(BaseItem):
    def __init__(self):
        super(CheckPgxcRedistb, self).__init__(self.__class__.__name__)
        self.version = None

    def doCheck(self):
        databaseListSql = "select datname from pg_database " \
                          "where datname != 'template0';"
        output = SharedFuncs.runSqlCmd(databaseListSql, self.user, "",
                                       self.port, self.tmpPath, "postgres",
                                       self.mpprcFile)
        dbList = output.split("\n")
        residue = False
        residueTableSql = "select * from pg_tables " \
                          "where tablename='pgxc_redistb';"
        residueSchemaSql = "select * from pg_namespace " \
                           "where nspname='data_redis';"

        self.result.raw = residueTableSql + residueSchemaSql
        for dbName in dbList:
            # Check temporary table residue
            output = SharedFuncs.runSqlCmd(residueTableSql, self.user, "",
                                           self.port, self.tmpPath, dbName,
                                           self.mpprcFile)
            if output:
                residue = True
                self.result.val += "Redistributed " \
                                   "temporary table pgxc_redistb has " \
                                   "existed in database %s." % dbName
            # Check temporary schema residues
            output = SharedFuncs.runSqlCmd(residueSchemaSql, self.user, "",
                                           self.port, self.tmpPath, dbName,
                                           self.mpprcFile)
            if output:
                residue = True
                self.result.val += "Redistributed temporary schema " \
                                   "data_redis has existed " \
                                   "in database %s." % dbName

        if (residue):
            self.result.rst = ResultStatus.NG
        else:
            self.result.rst = ResultStatus.OK
            self.result.val += "Residue Table pgxc_redistb " \
                               "and residue schema data_redis " \
                               "do not exist in the cluster."
