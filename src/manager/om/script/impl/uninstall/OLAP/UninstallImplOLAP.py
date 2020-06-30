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
import sys

sys.path.append(sys.path[0] + "/../../")

from gspylib.common.Common import DefaultValue
from gspylib.os.gsfile import g_file
from impl.uninstall.UninstallImpl import UninstallImpl


class UninstallImplOLAP(UninstallImpl):
    """
    init the command options
    save command line parameter values
    """

    def __init__(self, unstallation):
        self.logFile = unstallation.logFile
        self.cleanInstance = unstallation.cleanInstance

        self.localLog = unstallation.localLog
        self.user = unstallation.user
        self.group = unstallation.group
        self.mpprcFile = unstallation.mpprcFile
        self.localMode = unstallation.localMode
        self.logger = unstallation.logger
        self.sshTool = unstallation.sshTool
        self.tmpDir = DefaultValue.getTmpDirFromEnv(self.user)
        try:
            # Initialize the unstallation.clusterInfo variable
            unstallation.initClusterInfoFromStaticFile(self.user)
            self.clusterInfo = unstallation.clusterInfo
            nodeNames = self.clusterInfo.getClusterNodeNames()
            # Initialize the self.sshTool variable
            unstallation.initSshTool(nodeNames,
                                     DefaultValue.TIMEOUT_PSSH_UNINSTALL)
            self.sshTool = unstallation.sshTool
        except Exception as e:
            self.logger.logExit(str(e))

    def checkEnv(self):
        """
        function: check if GAUSS_ENV is 2
        input : NA
        output: NA
        """
        try:
            DefaultValue.checkUser(self.user)
        except Exception as e:
            self.logger.exitWithError(str(e))

    def ReCleanEtcdPath(self):
        """
        function: make sure the etcd path is clean.
        input : NA
        output: NA
        """
        # check if need delete instance
        if (not self.cleanInstance):
            self.logger.debug("No need to redelete etcd path.")
            return

        if (self.localMode):
            for dbnode in self.clusterInfo.dbNodes:
                if (dbnode.name == DefaultValue.GetHostIpOrName()):
                    if (len(dbnode.etcds) > 0):
                        etcdDir = dbnode.etcds[0].datadir
                        self.logger.debug("Clean etcd path %s in node: %s." % (
                            etcdDir, dbnode.name))
                        g_file.cleanDirectoryContent(etcdDir)
        else:
            for dbnode in self.clusterInfo.dbNodes:
                if (len(dbnode.etcds) > 0):
                    etcdDir = dbnode.etcds[0].datadir
                    cmd = g_file.SHELL_CMD_DICT["cleanDir4"] % etcdDir
                    self.logger.debug("Clean etcd path %s in node: %s." % (
                        etcdDir, dbnode.name))
                    (status, output) = self.sshTool.getSshStatusOutput(cmd, [
                        dbnode.name], self.mpprcFile)
                    if (status[dbnode.name] != DefaultValue.SUCCESS):
                        self.logger.debug("Clean etcd failed: %s" % output)
