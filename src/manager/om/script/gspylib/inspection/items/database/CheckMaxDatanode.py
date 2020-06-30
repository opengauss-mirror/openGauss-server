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
from gspylib.common.ErrorCode import ErrorCode

g_value = 0


class CheckMaxDatanode(BaseItem):
    def __init__(self):
        super(CheckMaxDatanode, self).__init__(self.__class__.__name__)
        self.nodeCount = None
        self.dnCount = None

    def preCheck(self):
        # check current node contains cn instances if not raise  exception
        super(CheckMaxDatanode, self).preCheck()
        # check the threshold was set correctly
        if (not self.threshold.__contains__('nodeCount')):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"]
                            % "threshold nodeCount")
        if (not self.threshold.__contains__('dnCount')):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"]
                            % "threshold dnCount")

        self.nodeCount = self.threshold['nodeCount']
        self.dnCount = self.threshold['dnCount']

    def doCheck(self):
        global g_value
        dataNum = int(self.nodeCount) * int(self.dnCount)
        sqlcmd = "SELECT setting FROM pg_settings WHERE " \
                 "name='comm_max_datanode';"
        self.result.raw = sqlcmd
        comm_max_datanode = SharedFuncs.runSqlCmd(sqlcmd, self.user, "",
                                                  self.port, self.tmpPath,
                                                  "postgres", self.mpprcFile)

        if comm_max_datanode.isdigit() and dataNum > int(comm_max_datanode):
            if (dataNum < 256):
                g_value = 256
            elif (dataNum < 512):
                g_value = 512
            elif (dataNum < 1024):
                g_value = 1024
            else:
                value = 2048
            self.result.rst = ResultStatus.WARNING
            self.result.val = "Invalid value for GUC parameter " \
                              "comm_max_datanode: %s. Expect value: %s" % (
                                  comm_max_datanode, g_value)
        else:
            self.result.rst = ResultStatus.OK
            self.result.val = "dataNum: %s, comm_max_datanode: %s" % (
                dataNum, comm_max_datanode)

        self.result.raw = sqlcmd

    def doSet(self):
        cmd = " gs_guc set -N all -I all -c " \
              "'comm_max_datanode=%d'; " % g_value
        cmd += " gs_guc set -N all -I all -c " \
               "'comm_max_datanode=%d' " % g_value
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            self.result.val += "Falied to set comm_max_datanode.\n Error : " \
                               "%s. " % output
            self.result.val += "The cmd is %s " % cmd
        else:
            self.result.val += "Set comm_max_datanode successfully."
