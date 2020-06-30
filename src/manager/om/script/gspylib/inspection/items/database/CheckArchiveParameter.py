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
import subprocess
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus


class CheckArchiveParameter(BaseItem):
    def __init__(self):
        super(CheckArchiveParameter, self).__init__(self.__class__.__name__)

    def doCheck(self):
        sqlcmd = "show archive_mode;"
        self.result.raw = sqlcmd

        output = SharedFuncs.runSqlCmd(sqlcmd, self.user, "", self.port,
                                       self.tmpPath, "postgres",
                                       self.mpprcFile)
        if output.strip() == "on":
            sqlcmd = "show archive_command;"
            self.result.raw = sqlcmd
            output = SharedFuncs.runSqlCmd(sqlcmd, self.user, "", self.port,
                                           self.tmpPath, "postgres",
                                           self.mpprcFile)
            cooInst = self.cluster.getDbNodeByName(self.host).coordinators[0]
            dataInst = self.cluster.getDbNodeByName(self.host).datanodes[0]
            if ((self.cluster.isSingleInstCluster() and not (
                    output.find("%s" % dataInst.datadir) >= 0)) and not (
                    output.find("%s" % cooInst.datadir) >= 0)):
                self.result.rst = ResultStatus.NG
            else:
                self.result.rst = ResultStatus.OK
        else:
            self.result.rst = ResultStatus.OK
        self.result.val = output

    def doSet(self):
        resultStr = ""
        cooInst = self.cluster.getDbNodeByName(self.host).coordinators[0]
        dataInst = self.cluster.getDbNodeByName(self.host).datanodes[0]
        if self.cluster.isSingleInstCluster():
            cmd = "gs_guc reload -N all -I " \
                  "all -c \"archive_command = 'cp -P --remove-destination" \
                  " %s %s/pg_xlog/archive/%s'\" " % dataInst.datadir
        else:
            cmd = "gs_guc reload -N all -I " \
                  "all -c \"archive_command = 'cp -P --remove-destination" \
                  " %s %s/pg_xlog/archive/%s'\"" % cooInst.datadir
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            resultStr = "Failed to set ArchiveMode.\n Error : %s." % output
            resultStr += "The cmd is %s " % cmd
        else:
            resultStr = "Set ArchiveMode successfully."
        self.result.val = resultStr
