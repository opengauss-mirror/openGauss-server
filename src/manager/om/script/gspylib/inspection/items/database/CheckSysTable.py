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
import grp
import pwd
from multiprocessing.dummy import Pool as ThreadPool
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.common.Common import DefaultValue
from gspylib.common.ErrorCode import ErrorCode
from gspylib.inspection.common.Exception import CheckNAException
from gspylib.os.gsfile import g_file

# cn
INSTANCE_ROLE_COODINATOR = 3
# dn
INSTANCE_ROLE_DATANODE = 4

MASTER_INSTANCE = 0


class CheckSysTable(BaseItem):
    def __init__(self):
        super(CheckSysTable, self).__init__(self.__class__.__name__)
        self.database = None

    def preCheck(self):
        # check the threshold was set correctly
        if (not self.threshold.__contains__('database')):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"]
                            % "threshold database")
        self.database = self.threshold['database']

    def checkSingleSysTable(self, Instance):
        tablelist = ["pg_attribute", "pg_class", "pg_constraint",
                     "pg_partition", "pgxc_class", "pg_index", "pg_stats"]
        localPath = os.path.dirname(os.path.realpath(__file__))
        resultMap = {}
        try:
            for i in tablelist:
                sqlFile = "%s/sqlFile_%s_%s.sql" % (
                    self.tmpPath, i, Instance.instanceId)
                resFile = "%s/resFile_%s_%s.out" % (
                    self.tmpPath, i, Instance.instanceId)
                g_file.createFile(sqlFile, True, DefaultValue.SQL_FILE_MODE)
                g_file.createFile(resFile, True, DefaultValue.SQL_FILE_MODE)
                g_file.changeOwner(self.user, sqlFile)
                g_file.changeOwner(self.user, resFile)
                sql = "select * from pg_table_size('%s');" % i
                sql += "select count(*) from %s;" % i
                sql += "select * from pg_column_size('%s');" % i
                g_file.writeFile(sqlFile, [sql])

                cmd = "gsql -d %s -p %s -f %s --output %s -t -A -X" % (
                    self.database, Instance.port, sqlFile, resFile)
                if (self.mpprcFile != "" and self.mpprcFile is not None):
                    cmd = "source '%s' && %s" % (self.mpprcFile, cmd)
                SharedFuncs.runShellCmd(cmd, self.user)

                restule = g_file.readFile(resFile)
                g_file.removeFile(sqlFile)
                g_file.removeFile(resFile)

                size = restule[0].strip()
                line = restule[1].strip()
                width = restule[2].strip()
                Role = ""
                if (Instance.instanceRole == INSTANCE_ROLE_COODINATOR):
                    Role = "CN"
                elif (Instance.instanceRole == INSTANCE_ROLE_DATANODE):
                    Role = "DN"
                instanceName = "%s_%s" % (Role, Instance.instanceId)
                resultMap[i] = [instanceName, size, line, width]
            return resultMap
        except Exception as e:
            if os.path.exists(sqlFile):
                g_file.removeFile(sqlFile)
            if os.path.exists(resFile):
                g_file.removeFile(resFile)
            raise Exception(str(e))

    def checkSysTable(self):
        primaryDNidList = []
        nodeInfo = self.cluster.getDbNodeByName(self.host)
        CN = nodeInfo.coordinators
        masterDnList = SharedFuncs.getMasterDnNum(self.user, self.mpprcFile)
        for DnInstance in nodeInfo.datanodes:
            if (DnInstance.instanceId in masterDnList):
                primaryDNidList.append(DnInstance)
        if (len(CN) < 1 and len(primaryDNidList) < 1):
            raise CheckNAException(
                "There is no primary database node instance in the "
                "current node.")

        # test database Connection
        for Instance in (CN + primaryDNidList):
            if not Instance:
                continue
            sqlcmd = "select pg_sleep(1);"
            SharedFuncs.runSqlCmd(sqlcmd, self.user, "", Instance.port,
                                  self.tmpPath, self.database, self.mpprcFile)
        outputList = []
        pool = ThreadPool(DefaultValue.getCpuSet())
        results = pool.map(self.checkSingleSysTable, CN + primaryDNidList)
        pool.close()
        pool.join()
        for result in results:
            if (result):
                outputList.append(result)
        sorted(outputList)
        return outputList

    def doCheck(self):
        flag = True
        resultStr = ""
        resultStr += "Instance table           size            row      " \
                     "width row*width\n"
        outputList = self.checkSysTable()
        for resultMap in outputList:
            for table in resultMap.keys():
                resultStr += "%s  %s %s %s %s %s\n" % (
                    resultMap[table][0], table.ljust(15),
                    resultMap[table][1].ljust(15),
                    resultMap[table][2].ljust(8),
                    resultMap[table][3].ljust(5),
                    int(resultMap[table][2]) * int(resultMap[table][3]))

        self.result.val = resultStr
        self.result.raw = resultStr
        self.result.rst = ResultStatus.OK
