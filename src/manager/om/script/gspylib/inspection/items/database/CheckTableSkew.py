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
from gspylib.common.Common import DefaultValue, ClusterCommand
from gspylib.common.ErrorCode import ErrorCode


class CheckTableSkew(BaseItem):
    def __init__(self):
        super(CheckTableSkew, self).__init__(self.__class__.__name__)

    def doCheck(self):
        security_mode_value = DefaultValue.getSecurityMode()
        if (security_mode_value == "on"):
            secMode = True
        else:
            secMode = False
        if (self.cluster.isSingleInstCluster()):
            if (secMode):
                finalresult = ""
                dbList = []
                sqlList = []
                sqlPath = os.path.realpath(
                    os.path.join(os.path.split(os.path.realpath(__file__))[0],
                                 "../../lib/checkblacklist/"))
                sqlFileName = os.path.join(sqlPath, "GetTableSkew.sql")
                if (os.path.exists(sqlFileName)):
                    try:
                        with open(sqlFileName, "r") as fp:
                            lines = fp.read()
                        sqlList = lines.split("--sqlblock")
                        sqlList.pop()
                    except Exception as e:
                        raise Exception(ErrorCode.GAUSS_502["GAUSS_50204"]
                                        % ("file:%s,Error:%s"
                                           % (sqlFileName, str(e))))
                else:
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50219"]
                                    % ("sql file:%s" % sqlFileName))
                sqldb = "select datname from pg_database;"
                (status, result, error) = ClusterCommand.excuteSqlOnLocalhost(
                    self.port, sqldb)
                if (status != 2):
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"]
                                    % sqldb + (" Error:%s" % error))
                recordsCount = len(result)
                for i in range(0, recordsCount):
                    dbList.append(result[i][0])
                dbList.remove("template0")
                dbList.remove("template1")
                for db in dbList:
                    schemaTable = []
                    for sql in sqlList:
                        sql = "set client_min_messages='error';\n" + sql
                        ClusterCommand.excuteSqlOnLocalhost(self.port, sql, db)
                    sql = "SELECT  schemaname , tablename FROM " \
                          "PUBLIC.pgxc_analyzed_skewness WHERE " \
                          "skewness_tuple > 100000;"
                    (status, result,
                     error) = ClusterCommand.excuteSqlOnLocalhost(self.port,
                                                                  sql, db)
                    if (status != 2):
                        raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"]
                                        % sql + (" Error:%s" % error))
                    else:
                        for i in iter(result):
                            schema = i[0]
                            table = i[1]
                            schemaTable.append("%s.%s" % (schema, table))
                    if (schemaTable):
                        finalresult += "%s:\n%s\n" % (
                            db, "\n".join(schemaTable))
                if (finalresult):
                    self.result.rst = ResultStatus.WARNING
                    self.result.val = "The result is not ok:\n%s" % finalresult
                else:
                    self.result.rst = ResultStatus.OK
                    self.result.val = "Data is well distributed"
            else:
                finalresult = ""
                sqlPath = os.path.realpath(
                    os.path.join(os.path.split(os.path.realpath(__file__))[0],
                                 "../../lib/checkblacklist/"))
                sqlFileName = os.path.join(sqlPath, "GetTableSkew.sql")
                sqldb = "select datname from pg_database;"
                output = SharedFuncs.runSqlCmd(sqldb, self.user, "", self.port,
                                               self.tmpPath, "postgres",
                                               self.mpprcFile)
                dbList = output.split("\n")
                dbList.remove("template0")
                dbList.remove("template1")
                for db in dbList:
                    db = db.replace("$", "\\$")
                    cmd = "gsql -d %s -p %s -f %s" % (
                        db, self.port, sqlFileName)
                    tmpout = ""
                    output = SharedFuncs.runShellCmd(cmd, self.user,
                                                     self.mpprcFile)
                    if (output.find("(0 rows)") < 0):
                        tmpresult = output.splitlines()
                        idxS = 0
                        idxE = 0
                        for idx in range(len(tmpresult)):
                            if (not tmpresult[idx].find("---+---") < 0):
                                idxS = idx - 1
                            if (tmpresult[idx].find("row)") > 0 or tmpresult[
                                idx].find("rows)") > 0):
                                idxE = idx
                        for i in range(idxS, idxE):
                            tmpout += "%s\n" % tmpresult[i]
                        finalresult += "%s:\n%s\n" % (db, tmpout)
                if (finalresult):
                    self.result.rst = ResultStatus.WARNING
                    self.result.val = "Data is not well distributed:\n%s" \
                                      % finalresult
                else:
                    self.result.rst = ResultStatus.OK
                    self.result.val = "Data is well distributed"
        else:
            self.result.rst = ResultStatus.NA
            self.result.val = "First cn is not in this host"
