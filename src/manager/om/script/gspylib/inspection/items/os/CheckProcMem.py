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

from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.common.ErrorCode import ErrorCode

# master
MASTER_INSTANCE = 0
# standby
STANDBY_INSTANCE = 1


class CheckProcMem(BaseItem):
    def __init__(self):
        super(CheckProcMem, self).__init__(self.__class__.__name__)
        self.percentm = 0.8
        self.percentt = 0.9

    def preCheck(self):
        # check the threshold was set correctly
        if (not self.threshold.__contains__('percent_total')
                or not self.threshold.__contains__('percent_max')):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"] % "threshold")
        try:
            self.percentt = float(self.threshold['percent_total'])
            self.percentm = float(self.threshold['percent_max'])
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53011"] %
                            "CheckProcMem" + "Error: %s" % str(e))

    def doCheck(self):
        cmd = "free -g | grep Mem | awk '{print $2}' 2>/dev/null"
        totalMem = SharedFuncs.runShellCmd(cmd)
        cmd = "free -g | grep Mem | awk '{print $3}' 2>/dev/null"
        usedMem = SharedFuncs.runShellCmd(cmd)
        if (int(usedMem) > int(totalMem) * self.percentt):
            self.result.rst = ResultStatus.NG
            self.result.val = "Memory usage exceeded threshold"
            return
        cmd = "show max_process_memory;"
        cnPort = None
        masterDnPort = None
        slaveDnPort = None

        self.node = self.cluster.getDbNodeByName(self.host)
        if self.node.coordinators:
            cnPort = self.node.coordinators[0].port

        masterDnlist = SharedFuncs.getMasterDnNum(self.user, self.mpprcFile)
        for datanode in self.node.datanodes:
            if (datanode.instanceId in masterDnlist):
                masterDnPort = datanode.port
                break
            elif (
                    datanode.instanceType == MASTER_INSTANCE
                    or datanode.instanceType == STANDBY_INSTANCE):
                slaveDnPort = datanode.port
                break
        if (cnPort):
            output = SharedFuncs.runSqlCmd(cmd, self.user, "", cnPort,
                                           self.tmpPath, "postgres",
                                           self.mpprcFile)
        elif (masterDnPort):
            output = SharedFuncs.runSqlCmd(cmd, self.user, "", masterDnPort,
                                           self.tmpPath, "postgres",
                                           self.mpprcFile)
        elif (slaveDnPort):
            output = SharedFuncs.runSqlCmd(cmd, self.user, "", slaveDnPort,
                                           self.tmpPath, "postgres",
                                           self.mpprcFile, True)
        else:
            self.result.val = "There's no master database node " \
                              "or slave database node in this node"
            self.result.rst = ResultStatus.OK
            return
        if (output.upper().endswith("GB")):
            maxProcessM = int(output[:-2]) * 1024 * 1024 * self.percentm
        elif (output.upper().endswith("MB")):
            maxProcessM = int(output[:-2]) * 1024 * self.percentm
        elif (output.upper().endswith("KB")):
            maxProcessM = int(output[:-2]) * self.percentm
        else:
            self.result.val = \
                "Can not get the correct value of max_process_memroy"
            self.result.rst = ResultStatus.NG
            return
        cmd = "ps ux | grep gaussdb | awk '{print $6}'"
        output = SharedFuncs.runShellCmd(cmd)
        for line in output.splitlines():
            procM = int(line)
            if (procM > maxProcessM):
                self.result.val = \
                    "Memroy usage of some gaussdb process exceeded threshold"
                self.result.rst = ResultStatus.NG
                return

        self.result.rst = ResultStatus.OK
        self.result.val = "Memory is sufficient"
