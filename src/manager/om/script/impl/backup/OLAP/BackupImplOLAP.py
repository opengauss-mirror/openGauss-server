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
import os
import sys

sys.path.append(sys.path[0] + "/../../")
from gspylib.common.Common import DefaultValue
from gspylib.common.OMCommand import OMCommand
from gspylib.common.ErrorCode import ErrorCode
from gspylib.os.gsfile import g_file
from impl.backup.BackupImpl import BackupImpl


class BackupImplOLAP(BackupImpl):
    """
    The class is used to do perform backup
    or restore binary files and parameter files.
    """

    def __init__(self, backupObj):
        """
        function: Constructor
        input : backupObj
        output: NA
        """
        super(BackupImplOLAP, self).__init__(backupObj)

    def parseConfigFile(self):
        """
        function: Parsing configuration files
        input : NA
        output: NA
        """
        self.context.logger.log("Parsing configuration files.")

        try:
            self.context.initClusterInfoFromStaticFile(self.context.user)
            nodeNames = self.context.clusterInfo.getClusterNodeNames()
            if (self.context.nodename == ""):
                self.context.nodename = nodeNames
            else:
                remoteNode = self.context.clusterInfo.getDbNodeByName(
                    self.context.nodename)
                if (remoteNode is None):
                    raise Exception(ErrorCode.GAUSS_512["GAUSS_51209"] % (
                        "the node", self.context.nodename))
                self.context.nodename = [self.context.nodename]

            if (self.context.action == BackupImpl.ACTION_RESTORE):
                self.context.initSshTool(self.context.nodename,
                                         DefaultValue.TIMEOUT_PSSH_BACKUP)
            else:
                self.context.initSshTool(nodeNames,
                                         DefaultValue.TIMEOUT_PSSH_BACKUP)
        except Exception as e:
            raise Exception(str(e))

        self.context.logger.log("Successfully parsed the configuration file.")

    def doRemoteBackup(self):
        """
        function: Get user and group
        input : NA
        output: NA
        """
        self.context.logger.log("Performing remote backup.")
        localHostName = DefaultValue.GetHostIpOrName()
        tmp_backupDir = "%s/backupTemp_%d" % (
            DefaultValue.getTmpDirFromEnv(), os.getpid())
        cmd = "%s -U %s --nodeName %s -P %s -B %s  -l %s --ingore_miss" % \
              (OMCommand.getLocalScript("Local_Backup"),
               self.context.user,
               localHostName,
               tmp_backupDir,
               self.context.backupDir,
               self.context.localLog)

        if self.context.isParameter:
            cmd += " -p"
        if self.context.isBinary:
            cmd += " -b"
        self.context.logger.debug("Remote backup command is %s." % cmd)

        try:
            if (not os.path.exists(tmp_backupDir)):
                os.makedirs(tmp_backupDir,
                            DefaultValue.KEY_DIRECTORY_PERMISSION)

            (status, output) = self.context.sshTool.getSshStatusOutput(cmd)
            for node in status.keys():
                if (status[node] != DefaultValue.SUCCESS):
                    raise Exception(output)

            if self.context.isParameter:
                self.__distributeBackupFile(tmp_backupDir, "parameter")
            if self.context.isBinary:
                self.__distributeBackupFile(tmp_backupDir, "binary")

            DefaultValue.cleanFileDir(tmp_backupDir, self.context.sshTool)

            self.context.logger.log("Remote backup succeeded.")
            self.context.logger.log("Successfully backed up cluster files.")
        except Exception as e:
            DefaultValue.cleanFileDir(tmp_backupDir, self.context.sshTool)
            raise Exception(str(e))

    def __distributeBackupFile(self, tmp_backupDir, flag):
        """
        function: distribute Backup File
        input : tmp_backupDir, flag
        output: NA
        """
        # compresses the configuration files for all node backups
        tarFiles = "%s_*.tar" % flag
        tarName = "%s.tar" % flag
        cmd = g_file.SHELL_CMD_DICT["compressTarFile"] \
              % (tmp_backupDir, tarName,
                 tarFiles, DefaultValue.KEY_FILE_MODE, tarName)
        if (flag == "parameter"):
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                raise Exception(
                    ErrorCode.GAUSS_514["GAUSS_51400"]
                    % cmd + " Error:\n%s" % output)
        else:
            (status, output) = self.context.sshTool.getSshStatusOutput(
                cmd, self.context.nodename)
            for node in status.keys():
                if (status[node] != DefaultValue.SUCCESS):
                    raise Exception(output)

        # prepares the backup directory for the specified node
        cmd = g_file.SHELL_CMD_DICT["createDir"] \
              % (self.context.backupDir, self.context.backupDir,
                 DefaultValue.KEY_DIRECTORY_MODE)
        (status, output) = self.context.sshTool.getSshStatusOutput(
            cmd, self.context.nodename)
        for node in status.keys():
            if (status[node] != DefaultValue.SUCCESS):
                raise Exception(output)

        # send backup package to the specified node from the local node
        if (flag == "parameter"):
            self.context.sshTool.scpFiles(
                "%s/%s.tar" % (tmp_backupDir, flag), \
                self.context.backupDir, self.context.nodename)
        else:
            originalFile = "'%s'/%s.tar" % (tmp_backupDir, flag)
            targetFile = "'%s'/%s.tar" % (self.context.backupDir, flag)
            cmd = g_file.SHELL_CMD_DICT["copyFile"] % (
                originalFile, targetFile)
            (status, output) = self.context.sshTool.getSshStatusOutput(
                cmd, self.context.nodename)
            for node in status.keys():
                if (status[node] != DefaultValue.SUCCESS):
                    raise Exception(output)

    def __cleanTmpTar(self):
        """
        function: delete tmp tar package
        input : NA
        output: NA
        """
        cmd = ""
        if (self.context.isParameter and self.context.isBinary):
            cmd += g_file.SHELL_CMD_DICT["deleteBatchFiles"] % (
                    "%s/parameter_" % self.context.backupDir)
            cmd += " ; "
            cmd += g_file.SHELL_CMD_DICT["deleteBatchFiles"] % (
                    "%s/binary_" % self.context.backupDir)
        elif (self.context.isParameter and not self.context.isBinary):
            cmd += g_file.SHELL_CMD_DICT["deleteBatchFiles"] % (
                    "%s/parameter_" % self.context.backupDir)
        elif (not self.context.isParameter and self.context.isBinary):
            cmd += g_file.SHELL_CMD_DICT["deleteBatchFiles"] % (
                    "%s/binary_" % self.context.backupDir)

        try:
            (status, output) = self.context.sshTool.getSshStatusOutput(cmd)
            for node in status.keys():
                if status[node] != DefaultValue.SUCCESS:
                    raise Exception(output)

        except Exception as e:
            raise Exception(str(e))

    def doRemoteRestore(self):
        """
        function: Get user and group
        input : NA
        output: NA
        """
        self.context.logger.log("Performing remote restoration.")

        cmd = "%s -U %s -l %s --ingore_miss" % (
            OMCommand.getLocalScript("Local_Restore"),
            self.context.user,
            self.context.localLog)
        if (self.context.backupDir != ""):
            cmd += " -P %s" % self.context.backupDir
        if self.context.isParameter:
            cmd += " -p"
        if self.context.isBinary:
            cmd += " -b"
        self.context.logger.debug("Remote restoration command: %s." % cmd)

        try:
            (status, output) = self.context.sshTool.getSshStatusOutput(cmd)
            for node in status.keys():
                if status[node] != DefaultValue.SUCCESS:
                    raise Exception(output)

            self.__cleanTmpTar()
            self.context.logger.log("Successfully restored cluster files.")
        except Exception as e:
            self.__cleanTmpTar()
            raise Exception(str(e))
