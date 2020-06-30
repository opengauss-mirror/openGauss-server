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
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.os.gsfile import g_file

# Conf file name constant
POSTGRESQL_CONF = "postgresql.conf"
INSTANCE_ROLE_DATANODE = 4
g_result = []


class CheckDebugSwitch(BaseItem):
    def __init__(self):
        super(CheckDebugSwitch, self).__init__(self.__class__.__name__)

    def obtainDataDirLength(self, nodeInfo):
        """
        function: Obtain data dir length
        input: NA
        output: int, list
        """
        # Get the longest path
        DirLength = 0
        # Get the DB instance and the longest DB path
        for inst in nodeInfo.datanodes:
            if (len(inst.datadir) > DirLength):
                DirLength = len(inst.datadir)

        return DirLength

    def checkSingleParaFile(self, inst, desc, INDENTATION_VALUE_INT):
        """
        function: Check the log_min_messages parameter for each instance
        input: String, String, int
        output: int
        """
        # The instance directory must exist
        if (not os.path.exists(inst.datadir) or len(
                os.listdir(inst.datadir)) == 0):
            g_result.append(
                "%s: Abnormal reason: The directory doesn't exist"
                " or is empty." % (
                        "%s(%s) log_min_messages parameter" % (
                    desc, inst.datadir)).ljust(INDENTATION_VALUE_INT))
            return -1
        paraPath = ""
        # Gets the database node configuration file
        if inst.instanceRole == INSTANCE_ROLE_DATANODE:
            paraPath = os.path.join(inst.datadir, POSTGRESQL_CONF)
        else:
            g_result.append(
                "%s: Abnormal reason: Invalid instance type: %s." % (
                    ("%s(%s) log_min_messages parameter " % (
                        desc, inst.datadir)).ljust(INDENTATION_VALUE_INT),
                    inst.instanceRole))
            return - 1
        # The instance configuration file must exist
        if (not os.path.exists(paraPath)):
            g_result.append("%s: Abnormal reason: %s does not exist." % (
                ("%s(%s) log_min_messages parameter " % (
                    desc, inst.datadir)).ljust(INDENTATION_VALUE_INT),
                paraPath))
            return -1
        # Gets the log_min_messages parameter in the configuration file
        output = g_file.readFile(paraPath, "log_min_messages")
        value = None
        for line in output:
            line = line.split('#')[0].strip()
            if (line.find('log_min_messages') >= 0 and line.find('=') > 0):
                value = line.split('=')[1].strip()
                break
        if not value:
            value = "warning"
        # Determines whether the log_min_messages parameter is valid
        if (value.lower() != "warning"):
            g_result.append(
                "%s: Warning reason: The parameter 'log_min_messages(%s)'"
                " value is incorrect. It should be 'warning'."
                % (("%s(%s) log_min_messages parameter(%s)"
                    % (desc, paraPath, value)).ljust(INDENTATION_VALUE_INT),
                   value))
            return -1
        g_result.append("%s: Normal" % (
                "%s(%s) log_min_messages parameter(%s)" % (
            desc, paraPath, value)).ljust(
            INDENTATION_VALUE_INT))
        return 0

    def doCheck(self):
        global g_result
        g_result = []
        nodeInfo = self.cluster.getDbNodeByName(self.host)
        intervalLen = self.obtainDataDirLength(nodeInfo)
        resultList = []
        self.result.val = ""
        INDENTATION_VALUE_INT = intervalLen + 64
        # Check all DB instance debug switch
        for inst in nodeInfo.datanodes:
            resultList.append(
                self.checkSingleParaFile(inst, "DN", INDENTATION_VALUE_INT))
        if (-1 in resultList):
            self.result.rst = ResultStatus.WARNING
        else:
            self.result.rst = ResultStatus.OK
        for detail in g_result:
            self.result.val = self.result.val + '%s\n' % detail

    def doSet(self):
        nodeInfo = self.cluster.getDbNodeByName(self.host)
        intervalLen = self.obtainDataDirLength(nodeInfo)
        flag = 0
        resultStr = ""
        INDENTATION_VALUE_INT = intervalLen + 64
        for inst in nodeInfo.datanodes:
            flag = self.checkSingleParaFile(inst, "DN", INDENTATION_VALUE_INT)
            if (flag == -1):
                cmd = "gs_guc set -N all -I all -c" \
                      " 'log_min_messages = warning'"
                (status, output) = subprocess.getstatusoutput(cmd)
                if (status != 0):
                    resultStr += "Falied to set database node " \
                                 "log_min_massages.\n Error : %s" % output + \
                                 " Command: %s.\n" % cmd
        if (len(resultStr) > 0):
            self.result.val = resultStr
        else:
            self.result.val = "Set log_min_messages successfully."
