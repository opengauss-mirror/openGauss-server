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
import subprocess
import json
import multiprocessing
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.inspection.common.Exception import CheckNAException
from gspylib.common.ErrorCode import ErrorCode

# master
MASTER_INSTANCE = 0
# standby
STANDBY_INSTANCE = 1
# dummy standby
DUMMY_STANDBY_INSTANCE = 2

# cn
INSTANCE_ROLE_COODINATOR = 3
# dn
INSTANCE_ROLE_DATANODE = 4

g_gucDist = {}
RecommendedMaxMem = 0


class CheckMaxProcMemory(BaseItem):
    def __init__(self):
        super(CheckMaxProcMemory, self).__init__(self.__class__.__name__)
        self.Threshold_NG = None

    def preCheck(self):
        super(CheckMaxProcMemory, self).preCheck()
        # check the threshold was set correctly
        if (not self.threshold.__contains__('Threshold_NG')):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"]
                            % "The threshold Threshold_NG")
        if (not self.threshold['Threshold_NG'].isdigit()):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53014"]
                            % "The threshold Threshold_NG")
        self.Threshold_NG = int(self.threshold['Threshold_NG'])

    def checkInstanceGucValue(self, Instance):
        """
        get CN/DN instance guc parameters
        """
        global g_gucDist
        Role = ""
        needm = False
        if (Instance.instanceRole == INSTANCE_ROLE_COODINATOR):
            needm = False
        elif (self.checkMaster(Instance.instanceId)):
            needm = False
        else:
            needm = True
        sqlcmd = "select setting from pg_settings " \
                 "where name='max_process_memory';"
        output = SharedFuncs.runSqlCmd(sqlcmd, self.user, "", Instance.port,
                                       self.tmpPath, "postgres",
                                       self.mpprcFile, needm)
        if (Instance.instanceRole == INSTANCE_ROLE_COODINATOR):
            Role = "CN"
        elif (Instance.instanceRole == INSTANCE_ROLE_DATANODE):
            Role = "DN"
        instanceName = "%s_%s" % (Role, Instance.instanceId)
        g_gucDist[instanceName] = output

    def checkMaster(self, instanceId):
        cmd = "gs_om -t query |grep %s" % (instanceId)
        output = SharedFuncs.runShellCmd(cmd, self.user, self.mpprcFile)
        line = output.splitlines()[0]
        instanceinfo = line.split()
        for idx in range(len(instanceinfo)):
            if (instanceinfo[idx] == str(instanceId)):
                if (instanceinfo[idx + 2] == "Primary"):
                    return True
                else:
                    return False
        return False

    def doCheck(self):
        """

        """
        global g_gucDist
        global RecommendedMaxMem
        DNidList = []
        nodeInfo = self.cluster.getDbNodeByName(self.host)
        CN = nodeInfo.coordinators
        for DnInstance in nodeInfo.datanodes:
            if (self.checkMaster(DnInstance.instanceId)):
                DNidList.append(DnInstance)
        if (len(CN) < 1 and len(DNidList) < 1):
            self.result.rst = ResultStatus.NA
            self.result.val = "NA"
            return

        # test database Connection
        for Instance in (CN + DNidList):
            if not Instance:
                continue
            sqlcmd = "select pg_sleep(1);"
            output = SharedFuncs.runSqlCmd(sqlcmd, self.user, "",
                                           Instance.port, self.tmpPath,
                                           'postgres',
                                           self.mpprcFile)
            self.checkInstanceGucValue(Instance)
        cmd = "/sbin/sysctl -a |grep vm.min_free_kbytes|awk '{print $3}'"
        min_free_kbytes = int(SharedFuncs.runShellCmd(cmd).splitlines()[-1])
        cmd = "free -k | grep 'Mem'| grep -v 'grep'|awk '{print $2}'"
        raw = int(SharedFuncs.runShellCmd(cmd))
        if (min_free_kbytes * 100 > raw * 5):
            RecommendedMaxMem = int((raw * 0.7) // (len(DNidList) + 1))
        else:
            RecommendedMaxMem = int((raw * 0.8) // (len(DNidList) + 1))
        self.result.rst = ResultStatus.OK
        result = "RecommendedMaxMem is %s\n" % RecommendedMaxMem
        for key, value in g_gucDist.items():
            if (int(value) > RecommendedMaxMem):
                self.result.rst = ResultStatus.NG
                result += "%s : %s\n" % (key, value)
        if (self.result.rst == ResultStatus.OK):
            self.result.val = "parameter max_process_memory setting is ok"
        else:
            self.result.val = "parameter max_process_memory " \
                              "setting should not be bigger than " \
                              "recommended(kb):%s:\n%s" % (
            RecommendedMaxMem, result)

    def doSet(self):
        resultStr = ""
        cmd = "su - %s -c \"source %s;gs_guc set " \
              "-N all -I all -c 'max_process_memory=%s'\"" % (
        self.user, self.mpprcFile, RecommendedMaxMem)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            resultStr += "Set CN instance Failed.\n Error : %s." % output
            resultStr += "The cmd is %s " % cmd
        cmd = "su - %s -c \"source %s;gs_guc set " \
              "-N all -I all -c 'max_process_memory=%s'\"" % (
        self.user, self.mpprcFile, RecommendedMaxMem)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            resultStr += "Set  database node  instance Failed.\n " \
                         "Error : %s." % output
            resultStr += "The cmd is %s " % cmd
        if (len(resultStr) > 0):
            self.result.val = resultStr
        else:
            self.result.val = "Set max_process_memory successfully."
