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
from gspylib.common.DbClusterStatus import DbClusterStatus
from gspylib.common.Common import ClusterCommand
from gspylib.os.gsfile import g_file

KEY_FILE_MODE = 600


class CheckClusterState(BaseItem):
    def __init__(self):
        super(CheckClusterState, self).__init__(self.__class__.__name__)

    def doCheck(self):
        tmpFile = os.path.join(self.tmpPath, "gauss_cluster_status.dat")
        tmpFileName = os.path.join(self.tmpPath, "abnormal_node_status.dat")
        try:
            self.result.val = ""
            self.result.raw = ""
            # Check the cluster status with cm_ctl
            cmd = ClusterCommand.getQueryStatusCmd(self.user, "", tmpFile)
            output = SharedFuncs.runShellCmd(cmd, self.user, self.mpprcFile)
            self.result.raw += output
            # Check whether the cluster needs to be balanced
            # Check whether redistribution is required
            # Initialize cluster status information for temporary file
            clusterStatus = DbClusterStatus()
            clusterStatus.initFromFile(tmpFile)
            # Get the status of cluster
            statusInfo = clusterStatus.getClusterStauts(self.user)
            self.result.val = statusInfo
            if clusterStatus.isAllHealthy():
                self.result.rst = ResultStatus.OK
                if os.path.exists(tmpFile):
                    os.remove(tmpFile)
                return
            # If the abnormal node is present, create a temporary file
            # and print out the details
            g_file.createFile(tmpFileName, True, KEY_FILE_MODE)
            with open(tmpFileName, "w+") as tmpFileFp:
                for dbNode in clusterStatus.dbNodes:
                    if not dbNode.isNodeHealthy():
                        dbNode.outputNodeStatus(tmpFileFp, self.user, True)
                tmpFileFp.flush()
                tmpFileFp.seek(0)
                self.result.raw = tmpFileFp.read()
            if self.result.raw == "":
                self.result.raw = "Failed to obtain the cluster status."
            self.result.rst = ResultStatus.NG
            # Delete the temporary file
            if os.path.exists(tmpFileName):
                os.remove(tmpFileName)
            if os.path.exists(tmpFile):
                os.remove(tmpFile)
        except Exception as e:
            if os.path.exists(tmpFile):
                os.remove(tmpFile)
            if os.path.exists(tmpFileName):
                os.remove(tmpFileName)
            raise Exception(str(e))


