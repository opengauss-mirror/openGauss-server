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
import math
import subprocess
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.common.ErrorCode import ErrorCode
from gspylib.common.DbClusterStatus import DbClusterStatus
from gspylib.common.Common import ClusterCommand


class CheckDBParams(BaseItem):
    def __init__(self):
        super(CheckDBParams, self).__init__(self.__class__.__name__)

    def doCheck(self):
        # Gets the current node information
        nodeInfo = self.cluster.getDbNodeByName(self.host)
        # Get the number of instances
        InatorsList = nodeInfo.datanodes
        # Get local primary DB id
        primaryDNidList = self.getLocalPrimaryDNid(nodeInfo)
        self.result.raw = ""
        # Determine if there are DB instances
        if (len(primaryDNidList) < 1):
            self.result.raw = "There is no primary database node " \
                              "instance in the current node."
            self.result.rst = ResultStatus.OK
            return
        for inst in InatorsList:
            self.CheckGaussdbParameters(inst, nodeInfo, primaryDNidList)
        if (self.result.rst != ResultStatus.NG):
            self.result.rst = ResultStatus.OK

    def getLocalPrimaryDNid(self, nodeInfo):
        """
        function: Get local primary DNid
        input: NA
        output: NA
        """
        tmpFile = os.path.join(self.tmpPath, "gauss_dn_status.dat")
        primaryDNidList = []
        try:
            # Use cm_ctl to query the current node instance
            cmd = ClusterCommand.getQueryStatusCmd(self.user, nodeInfo.name,
                                                   tmpFile)
            SharedFuncs.runShellCmd(cmd, self.user, self.mpprcFile)
            # Match query results and cluster configuration
            clusterStatus = DbClusterStatus()
            clusterStatus.initFromFile(tmpFile)
            if (os.path.exists(tmpFile)):
                os.remove(tmpFile)
            # Find the master DB instance
            for dbNode in clusterStatus.dbNodes:
                for instance in dbNode.datanodes:
                    if instance.status == 'Primary':
                        primaryDNidList.append(instance.instanceId)
            return primaryDNidList
        except Exception as e:
            if (os.path.exists(tmpFile)):
                os.remove(tmpFile)
            raise Exception(str(e))

    def CheckSingleGaussdbParameter(self, port, desc,
                                    INDENTATION_VALUE_INT=60):
        """
        function: check gaussdb instance parameters
        input: int, string, int
        output: bool
        """
        sqlResultFile = ""
        try:
            flag = True
            # Generate different temporary files when parallel
            # Identify by instance number
            # Remove parentheses from the instance number
            InstNum = desc.replace('(', '')
            InstNum = InstNum.replace(')', '')
            # get max connection number
            sqlcmd = "show max_connections;"
            output = SharedFuncs.runSqlCmd(sqlcmd, self.user, "", port,
                                           self.tmpPath, "postgres",
                                           self.mpprcFile)
            maxConnections = int(output)
            if (desc.find("CN(") < 0):
                self.result.raw += "The max number of %s connections " \
                                   "is %s.\n" % (desc, maxConnections)
            # get shared_buffers size
            GB = 1 * 1024 * 1024 * 1024
            MB = 1 * 1024 * 1024
            kB = 1 * 1024
            shared_buffers = 0
            # Execute the query command
            sqlcmd = "show shared_buffers;"
            output = SharedFuncs.runSqlCmd(sqlcmd, self.user, "", port,
                                           self.tmpPath, "postgres",
                                           self.mpprcFile)
            shared_buffer_size = str(output)
            # The result of the conversion query is a regular display
            if shared_buffer_size[0:-2].isdigit() and (
                    (shared_buffer_size[-2:] > "GB") - (
                    shared_buffer_size[-2:] < "GB")) == 0:
                shared_buffers = int(shared_buffer_size[0:-2]) * GB
            if shared_buffer_size[0:-2].isdigit() and (
                    (shared_buffer_size[-2:] > "MB") - (
                    shared_buffer_size[-2:] < "MB")) == 0:
                shared_buffers = int(shared_buffer_size[0:-2]) * MB
            if shared_buffer_size[0:-2].isdigit() and (
                    (shared_buffer_size[-2:] > "kB") - (
                    shared_buffer_size[-2:] < "kB")) == 0:
                shared_buffers = int(shared_buffer_size[0:-2]) * kB
            if shared_buffer_size[0:-1].isdigit() and (
                    (shared_buffer_size[-2:] > "B") - (
                    shared_buffer_size[-2:] < "B")) == 0:
                shared_buffers = int(shared_buffer_size[0:-1])

            # check shared_buffers
            strCmd = "cat /proc/sys/kernel/shmmax"
            status, shmmax = subprocess.getstatusoutput(strCmd)
            if (status != 0):
                self.result.raw += "Failed to obtain shmmax parameters." \
                                   " Command: %s.\n" % strCmd
                flag = False
            # check shmall parameters
            strCmd = "cat /proc/sys/kernel/shmall"
            status, shmall = subprocess.getstatusoutput(strCmd)
            if (status != 0):
                self.result.raw += "Failed to obtain shmall parameters." \
                                   " Command: %s.\n" % strCmd
                flag = False
            # get PAGESIZE
            strCmd = "getconf PAGESIZE"
            status, PAGESIZE = subprocess.getstatusoutput(strCmd)
            if (status != 0):
                self.result.raw += "Failed to obtain PAGESIZE." \
                                   " Command: %s.\n" % strCmd
                flag = False
            if (shared_buffers < 128 * kB):
                self.result.raw += "Shared_buffers must be greater " \
                                   "than or equal to 128KB.\n"
                flag = False
            elif (shared_buffers > int(shmmax)):
                self.result.raw += "Shared_buffers must be less" \
                                   " than shmmax(%d).\n" % int(shmmax)
                flag = False
            elif (shared_buffers > int(shmall) * int(PAGESIZE)):
                self.result.raw += "Shared_buffers must be less " \
                                   "than shmall*PAGESIZE(%d).\n" \
                                   % int(shmall) * int(PAGESIZE)
                flag = False
            else:
                self.result.raw += "%s Shared buffers size is %s.\n" \
                                   % (desc, shared_buffer_size)
            # check sem    
            if (desc.find("CN(") >= 0):
                strCmd = "cat /proc/sys/kernel/sem"
                status, output = subprocess.getstatusoutput(strCmd)
                if (status != 0):
                    self.result.raw += "Failed to obtain sem parameters." \
                                       " Error: %s.\n" % output + \
                                       " Command: %s.\n" % strCmd
                    flag = False
                paramList = output.split("\t")
                if (int(paramList[0]) < 17):
                    self.result.raw += "The system limit for the maximum" \
                                       " number of semaphores per set" \
                                       " (SEMMSL) must be greater than or" \
                                       " equal to 17. The current SEMMSL " \
                                       "value is: " + str(paramList[0]) \
                                       + ".\n"
                    flag = False

                if (int(paramList[3]) < math.ceil(
                        (maxConnections + 150) // 16)):
                    self.result.raw += "The system limit for the maximum" \
                                       " number of semaphore sets (SEMMNI)" \
                                       " must be greater than or equal to" \
                                       " the value(math.ceil((" \
                                       "maxConnections + 150) / 16)) " + \
                                       str(math.ceil((maxConnections +
                                                      150) // 16)) + \
                                       ", The current SEMMNI value is: " + \
                                       str(paramList[3]) + ".\n"
                    flag = False
                elif (int(paramList[1]) < math.ceil(
                        (maxConnections + 150) // 16) * 17):
                    self.result.raw += "The system limit for the maximum" \
                                       " number of semaphores (SEMMNS) must" \
                                       " be greater than or equal to the" \
                                       " value(math.ceil((maxConnections" \
                                       " + 150) / 16) * 17) " \
                                       + str(math.ceil((maxConnections +
                                                        150) // 16) * 17) + \
                                       ", The current SEMMNS value is: " + \
                                       str(paramList[1]) + ".\n"
                    flag = False
                else:
                    self.result.raw += "The max number of %s connections" \
                                       " is %s.\n" % (desc, maxConnections)
            if (os.path.exists(sqlResultFile)):
                os.remove(sqlResultFile)
            return flag
        except Exception as e:
            if (os.path.exists(sqlResultFile)):
                os.remove(sqlResultFile)
            raise Exception(ErrorCode.GAUSS_513["GAUSS_51306"] %
                            (("The max number of %s connections.\n" %
                              desc).ljust(INDENTATION_VALUE_INT), str(e)))

    def CheckGaussdbParameters(self, inst, nodeInfo, primaryDNidList):
        """
        function: Check gaussdb instance parameters
        input: instance
        output: NA
        """
        INDENTATION_VALUE_INT = 50
        resultList = []
        try:
            # Check all master DB instances
            if (primaryDNidList != []):
                if (inst in nodeInfo.datanodes):
                    if inst.instanceId in primaryDNidList:
                        resultList.append(
                            self.CheckSingleGaussdbParameter(
                                inst.port, "DN(%s)" % str(inst.instanceId),
                                INDENTATION_VALUE_INT))
            if (False in resultList):
                self.result.rst = ResultStatus.NG
                return
        except Exception as e:
            raise Exception(str(e))

    def doSet(self):
        resultStr = ""
        cmd = "gs_guc set -N all -I all -c" \
              " 'shared_buffers=1GB' -c 'max_connections=400'"
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            resultStr += "Falied to set cn shared_buffers.\nError : %s" \
                         % output + " Command: %s.\n" % cmd
        cmd = "gs_guc set -N all -I all -c 'shared_buffers=1GB'" \
              " -c 'max_connections=3000'"
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            resultStr += "Falied to set database node shared_buffers.\n" \
                         "Error : %s" % output + " Command: %s.\n" % cmd
        if (len(resultStr) > 0):
            self.result.val = resultStr
        else:
            self.result.val = "Set shared_buffers successfully."
