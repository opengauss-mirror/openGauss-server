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
from gspylib.common.ErrorCode import ErrorCode

dbList = []


class CheckDilateSysTab(BaseItem):
    def __init__(self):
        super(CheckDilateSysTab, self).__init__(self.__class__.__name__)
        self.Threshold_NG = None
        self.Threshold_Warning = None

    def preCheck(self):
        super(CheckDilateSysTab, self).preCheck()
        if (not (self.threshold.__contains__(
                'Threshold_NG') and self.threshold.__contains__(
            'Threshold_Warning'))):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"]
                            % "The threshold Threshold_NG and"
                              " Threshold_Warning ")
        if (not self.threshold['Threshold_NG'].isdigit() or not
        self.threshold['Threshold_Warning'].isdigit()):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53014"]
                            % "The threshold Threshold_NG and"
                              " Threshold_Warning ")
        self.Threshold_NG = int(self.threshold['Threshold_NG'])
        self.Threshold_Warning = int(self.threshold['Threshold_Warning'])

    def doCheck(self):
        global dbList
        self.result.rst = ResultStatus.OK
        sqldb = "select datname from pg_database;"
        output = SharedFuncs.runSqlCmd(sqldb, self.user, "", self.port,
                                       self.tmpPath, "postgres",
                                       self.mpprcFile)
        dbList = output.split("\n")
        dbList.remove("template0")
        sql = "select (pg_table_size(1259)/count(*)/247.172)::numeric(10,3)" \
              " from pg_class;"
        result = []
        for db in dbList:
            # Calculate the size with sql cmd
            output = SharedFuncs.runSqlCmd(sql, self.user, "", self.port,
                                           self.tmpPath, db, self.mpprcFile)
            if (float(output) > self.Threshold_NG):
                self.result.rst = ResultStatus.NG
                result.append(db)
            elif (float(output) > self.Threshold_Warning):
                result.append(db)
                if (self.result.rst == ResultStatus.OK):
                    self.result.rst = ResultStatus.WARNING

        if (self.result.rst == ResultStatus.OK):
            self.result.val = "no system table dilate"
        else:
            self.result.val = "there is system table dilate in" \
                              " databases:\n%s" % "\n".join(result)

    def doSet(self):
        reslutStr = ""
        sqlCmd = "cluster pg_attribute using" \
                 " pg_attribute_relid_attnum_index;" \
                 "cluster pg_class using pg_class_oid_index;" \
                 "cluster pg_type using pg_type_oid_index;" \
                 "cluster pg_proc using pg_proc_oid_index;" \
                 "cluster pg_depend using pg_depend_depender_index;" \
                 "cluster pg_index using pg_index_indexrelid_index;" \
                 "cluster pg_namespace using pg_namespace_oid_index;" \
                 "cluster pgxc_class using pgxc_class_pcrelid_index;" \
                 "vacuum full pg_statistic;"
        for databaseName in dbList:
            for sql in sqlCmd.split(';'):
                output = SharedFuncs.runSqlCmd(sql, self.user, "", self.port,
                                               self.tmpPath, databaseName,
                                               self.mpprcFile)
                reslutStr += output
        self.result.val = reslutStr
