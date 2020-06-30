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
import subprocess
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.common.Common import DefaultValue, ClusterCommand
from gspylib.common.ErrorCode import ErrorCode

g_result = {}


class CheckUnAnalyzeTable(BaseItem):
    def __init__(self):
        super(CheckUnAnalyzeTable, self).__init__(self.__class__.__name__)

    def doCheck(self):
        global g_result
        if (self.cluster.isSingleInstCluster()):
            sql1 = """create or replace FUNCTION get_antiempty_tables(
OUT result_tables text
) 
returns text
as $$
declare
    test_sql text;
    type cursor_type is ref cursor;
    cur_sql_stmts cursor_type;
    cur_test_sql_result cursor_type;
    test_sql_result int;
    result_tables text := '';
begin
    drop table if exists to_be_selected_check;
    create temp table to_be_selected_check as select 'select 1 from ' || 
    nspname || '.' || relname || ' limit 1;' as stmt from pg_class c, 
    pg_namespace n where c.relnamespace=n.oid and c.reltuples=0
        AND n.nspname <> 'pg_catalog'
        AND n.nspname <> 'cstore'
        AND n.nspname <> 'information_schema'
        AND n.nspname <> 'schema_cur_table_col'
        AND n.nspname <> 'schema_cur_table'
        AND c.relkind='r'
        AND c.relpersistence='p'
        AND n.nspname !~ '^pg_toast';

    open cur_sql_stmts for 'select stmt from to_be_selected_check';
    loop
        fetch cur_sql_stmts into test_sql;
        exit when cur_sql_stmts%notfound;
        open cur_test_sql_result for test_sql;
        fetch cur_test_sql_result into test_sql_result;
        if not cur_test_sql_result%notfound and 0 = position(
        'to_be_selected_check' in test_sql) then
            result_tables = result_tables || replace(replace(replace(
            test_sql, 'select 1 from ', ''), ' limit 1', ''), ';', CHR(10));
        end if ;
        close cur_test_sql_result;
    end loop;
    close cur_sql_stmts;
    drop table if exists to_be_selected_check;
    return result_tables;
end; $$
LANGUAGE 'plpgsql';"""

            sql2 = "select get_antiempty_tables();"
            sqldb = "select datname from pg_database;"
            security_mode_value = DefaultValue.getSecurityMode()
            if (security_mode_value == "on"):
                secMode = True
            else:
                secMode = False
            if (secMode):
                dbList = []
                (status, result, error) = ClusterCommand.excuteSqlOnLocalhost(
                    self.port, sqldb)
                if (status != 2):
                    raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"]
                                    % sqldb + (" Error:%s" % error))
                recordsCount = len(result)
                for i in range(0, recordsCount):
                    dbList.append(result[i][0])
                dbList.remove("template0")
                finalresult = ""
                for db in dbList:
                    tablelist = []
                    ClusterCommand.excuteSqlOnLocalhost(self.port, sql1, db)
                    ClusterCommand.excuteSqlOnLocalhost(
                        self.port, "set client_min_messages='error';create "
                                   "table to_be_selected_check(test int);", db)
                    sql2 = "set client_min_messages='error';" + sql2
                    (status, result,
                     error) = ClusterCommand.excuteSqlOnLocalhost(self.port,
                                                                  sql2, db)
                    if (status != 2):
                        raise Exception(ErrorCode.GAUSS_513["GAUSS_51300"]
                                        % sql2 + (" Error:%s" % error))
                    if (result and result[0][0]):
                        for tmptable in result[0][0].splitlines():
                            if (db == "postgres" and
                                    tmptable.upper().startswith("PMK.")):
                                pass
                            else:
                                tablelist.append(tmptable)
                        if (tablelist):
                            finalresult += "%s:\n%s\n" % (
                                db, "\n".join(tablelist))
                    g_result[db] = tablelist
                if (finalresult):
                    self.result.val = "The result is not ok:\n%s" % finalresult
                    self.result.rst = ResultStatus.NG
                else:
                    self.result.val = "All table analyzed"
                    self.result.rst = ResultStatus.OK
            else:
                # Get the database in the node, remove template0
                output = SharedFuncs.runSqlCmd(sqldb, self.user, "", self.port,
                                               self.tmpPath, "postgres",
                                               self.mpprcFile)
                dbList = output.split("\n")
                dbList.remove("template0")
                finalresult = ""
                for db in dbList:
                    tablelist = []
                    SharedFuncs.runSqlCmd(sql1, self.user, "", self.port,
                                          self.tmpPath, db, self.mpprcFile)
                    output = SharedFuncs.runSqlCmd(sql2, self.user, "",
                                                   self.port, self.tmpPath, db,
                                                   self.mpprcFile)
                    for tmptable in output.splitlines():
                        if (db == "postgres" and tmptable.upper().startswith(
                                "PMK.")):
                            pass
                        else:
                            tablelist.append(tmptable)
                    if (tablelist):
                        finalresult += "%s:\n%s\n" % (db, "\n".join(tablelist))
                    g_result[db] = tablelist
                if (finalresult):
                    self.result.val = "Tables unanalyzed:\n%s" % finalresult
                    self.result.rst = ResultStatus.NG
                else:
                    self.result.val = "All table analyzed"
                    self.result.rst = ResultStatus.OK
        else:
            self.result.rst = ResultStatus.NA
            self.result.val = "First cn is not in this host"

    def doSet(self):
        resultStr = ""
        for db in g_result.keys():
            for table in g_result[db]:
                sql = "analyze %s;" % table
                output = SharedFuncs.runSqlCmd(sql, self.user, "", self.port,
                                               self.tmpPath, db,
                                               self.mpprcFile)
                resultStr += "%s:%s Result: %s.\n" % (db, table, output)
        self.result.val = "Analyze %s successfully." % resultStr
