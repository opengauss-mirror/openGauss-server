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
import sys
import subprocess
import time
import json
import re
import csv
import traceback

from datetime import datetime, timedelta
from gspylib.common.Common import DefaultValue, ClusterCommand
from gspylib.common.DbClusterInfo import instanceInfo, \
    dbNodeInfo, dbClusterInfo, compareObject
from gspylib.common.OMCommand import OMCommand
from gspylib.common.ErrorCode import ErrorCode
from gspylib.threads.SshTool import SshTool
from gspylib.common.VersionInfo import VersionInfo
from gspylib.os.gsplatform import g_Platform
from gspylib.os.gsfile import g_file
from impl.upgrade.UpgradeConst import GreyUpgradeStep
import impl.upgrade.UpgradeConst as Const


class OldVersionModules():
    """
    class: old version modules
    """

    def __init__(self):
        """
        function: constructor
        """
        # old cluster information
        self.oldDbClusterInfoModule = None
        # old cluster status
        self.oldDbClusterStatusModule = None


class UpgradeImpl:
    """
    Class: The class is used to do perform upgrade
    """
    def __init__(self, upgrade):
        """
        function: constructor
        """
        self.context = upgrade
        self.newCommitId = ""
        self.oldCommitId = ""

    def exitWithRetCode(self, action, succeed=True, msg=""):
        """
        funtion: should be called after cmdline parameter check
        input : action, succeed, msg, strategy
        output: NA
        """
        #########################################
        # doUpgrade
        #
        # binary-upgrade      success    failure
        #                     0          1
        #
        # binary-rollback     success    failure
        #                     2          3

        # commit-upgrade      success    failure
        #                     5          1
        #########################################

        #########################################
        # choseStrategy
        #                     success    failure
        #                     4          1
        #########################################
        if not succeed:
            if action == Const.ACTION_AUTO_ROLLBACK:
                retCode = 3
            else:
                retCode = 1
        elif action in [Const.ACTION_SMALL_UPGRADE,
                        Const.ACTION_LARGE_UPGRADE,
                        Const.ACTION_INPLACE_UPGRADE]:
            retCode = 0
        elif action == Const.ACTION_AUTO_ROLLBACK:
            retCode = 2
        elif action == Const.ACTION_CHOSE_STRATEGY:
            retCode = 4
        elif action == Const.ACTION_COMMIT_UPGRADE:
            retCode = 5
        else:
            retCode = 1

        if msg != "":
            if self.context.logger is not None:
                if succeed:
                    self.context.logger.log(msg)
                else:
                    self.context.logger.error(msg)
            else:
                print(msg)
        sys.exit(retCode)

    def initGlobalInfos(self):
        """
        function: init global infos
        input : NA
        output: NA
        """
        self.context.logger.debug("Init global infos", "addStep")
        self.context.sshTool = SshTool(
            self.context.clusterNodes, self.context.localLog,
            DefaultValue.TIMEOUT_PSSH_BINARY_UPGRADE)
        self.initClusterConfig()
        self.context.logger.debug("Successfully init global infos", "constant")

    def setClusterDetailInfo(self):
        """
        function: set cluster detail info
        input  : NA
        output : NA
        """
        self.context.clusterInfo.setCnCount()
        for dbNode in self.context.clusterInfo.dbNodes:
            dbNode.setDnDetailNum()
            dbNode.cmsNum = len(dbNode.cmservers)
            dbNode.gtmNum = len(dbNode.gtms)
        self.context.clusterInfo.setClusterDnCount()

    def checkExistsProcess(self, greyNodeNames):
        """
        function: check exists process
        input  : greyNodeNames
        output : NA
        """
        pass

    def removeOmRollbackProgressFile(self):
        """
        function: remove om rollback process file
        input  : NA
        output : NA
        """
        self.context.logger.debug("Remove the om rollback"
                                  " record progress file.")
        fileName = os.path.join(self.context.tmpDir,
                                ".upgrade_task_om_rollback_result")
        cmd = "(if [ -f '%s' ];then rm -f '%s';fi)" % (fileName, fileName)
        DefaultValue.execCommandWithMode(cmd,
                                         "remove om rollback "
                                         "record progress file",
                                         self.context.sshTool,
                                         self.context.isSingle,
                                         self.context.mpprcFile)

    def initOmRollbackProgressFile(self):
        """
        function: init om rollback process file
        input  : NA
        output : NA
        """
        filePath = os.path.join(self.context.tmpDir,
                                ".upgrade_task_om_rollback_result")
        cmd = "echo \"OM:RUN\" > %s" % filePath
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            self.context.logger.debug("The cmd is %s " % cmd)
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50205"] % filePath
                            + "Error: \n%s" % str(output))

        if (not self.context.isSingle):
                # send file to remote nodes
            self.context.sshTool.scpFiles(filePath, self.context.tmpDir)
        self.context.logger.debug("Successfully write file %s." % filePath)

    def run(self):
        """
        function: Do upgrade
        input : NA
        output: NA
        """
        # the action may be changed in each step,
        # if failed in auto-rollback,
        # we will check if we need to rollback
        action = self.context.action
        # upgrade backup path
        self.context.tmpDir = DefaultValue.getTmpDirFromEnv(self.context.user)
        if self.context.tmpDir == "":
            raise Exception(ErrorCode.GAUSS_518["GAUSS_51800"] % "$PGHOST")
        self.context.upgradeBackupPath = \
            "%s/%s" % (self.context.tmpDir, "binary_upgrade")
        try:
            self.initGlobalInfos()
            self.removeOmRollbackProgressFile()

            # 4. get upgrade type
            # After choseStrategy, it will assign action to self.context.action
            # to do full-upgrade or binary-upgrade
            if self.context.action == Const.ACTION_AUTO_UPGRADE:
                self.context.action = self.choseStrategy()
                self.context.logger.debug(
                    "%s execution takes %s steps in total" % (
                        Const.GS_UPGRADECTL, ClusterCommand.countTotalSteps(
                            Const.GS_UPGRADECTL, self.context.action)))
                # If get upgrade strategy failed,
                # then try to get rollback strategy.
                # Set strategyFlag as True to check
                # upgrade parameter is correct or not
                self.doInplaceBinaryUpgrade()
            # After choseStrategy, it will assign action to self.context.action
            elif self.context.action == Const.ACTION_AUTO_ROLLBACK:
                # because if we rollback with auto rollback,
                # we will rollback all the nodes,
                # but if we rollback under upgrade,
                # we will only rollback specified nodes
                self.context.action = self.choseStrategy()
                self.exitWithRetCode(Const.ACTION_AUTO_ROLLBACK,
                                     self.doInplaceBinaryRollback())
            elif self.context.action == Const.ACTION_COMMIT_UPGRADE:
                self.context.action = self.choseStrategy()
                self.doInplaceCommitUpgrade()
            else:
                self.doChoseStrategy()
        except Exception as e:
            self.context.logger.debug(traceback.format_exc() + str(e))
            if not self.context.sshTool:
                self.context.sshTool = SshTool(
                    self.context.clusterNodes, self.context.logger,
                    DefaultValue.TIMEOUT_PSSH_BINARY_UPGRADE)
            if action == Const.ACTION_AUTO_ROLLBACK and \
                    self.checkBakPathNotExists():
                self.context.logger.log("No need to rollback.")
                self.exitWithRetCode(action, True)
            else:
                self.context.logger.error(str(e))
                self.exitWithRetCode(action, False, str(e))

    def checkBakPathNotExists(self):
        """
        check binary_upgrade exists on all nodes,
        :return: True if not exists on all nodes
        """
        try:
            cmd = "if [ -d '%s' ]; then echo 'GetDir'; else echo 'NoDir'; fi" \
                  % self.context.upgradeBackupPath
            self.context.logger.debug("Command for checking if upgrade bak "
                                      "path exists: %s" % cmd)
            outputCollect = self.context.sshTool.getSshStatusOutput(cmd)[1]
            if outputCollect.find('GetDir') >= 0:
                self.context.logger.debug("Checking result: %s"
                                          % outputCollect)
                return False
            self.context.logger.debug("Path %s does not exists on all node."
                                      % self.context.upgradeBackupPath)
            return True
        except Exception:
            self.context.logger.debug("Failed to check upgrade bak path.")
            return False

    def doChoseStrategy(self):
        """
        function: chose the strategy for upgrade
        input : NA
        output: NA
        """
        self.context.logger.debug("Choosing strategy.")
        try:
            self.context.action = self.choseStrategy()
            # we only support binary-upgrade.
            if self.context.action in [Const.ACTION_SMALL_UPGRADE,
                                       Const.ACTION_LARGE_UPGRADE]:
                self.exitWithRetCode(Const.ACTION_CHOSE_STRATEGY,
                                     True,
                                     "Upgrade strategy: %s."
                                     % self.context.action)
            # Use inplace upgrade under special case
            else:
                self.exitWithRetCode(Const.ACTION_CHOSE_STRATEGY,
                                     True,
                                     "Upgrade strategy: %s."
                                     % self.context.action)
        except Exception as e:
            self.exitWithRetCode(Const.ACTION_CHOSE_STRATEGY, False, str(e))
        self.context.logger.debug("Successfully got the upgrade strategy.")

    def choseStrategy(self):
        """
        function: chose upgrade strategy
        input : NA
        output: NA
        """
        upgradeAction = None
        try:
            # get new cluster info
            newVersionFile = VersionInfo.get_version_file()
            newClusterVersion, newClusterNumber, newCommitId = \
                VersionInfo.get_version_info(newVersionFile)
            gaussHome = DefaultValue.getInstallDir(self.context.user)
            if gaussHome == "":
                raise Exception(ErrorCode.GAUSS_518["GAUSS_51800"]
                                % "$GAUSSHOME")
            if not os.path.islink(gaussHome):
                raise Exception(ErrorCode.GAUSS_529["GAUSS_52915"])
            newPath = gaussHome + "_%s" % newCommitId
            # new app dir should exist after preinstall,
            # then we can use chose strategy
            if not os.path.exists(newPath):
                if self.context.action != Const.ACTION_AUTO_ROLLBACK:
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"]
                                    % newPath)
            self.context.logger.debug(
                "Successfully obtained version information"
                " of new clusters by %s." % newVersionFile)

            # get the old cluster info, if binary_upgrade does not exists,
            # try to copy from other nodes
            oldPath = self.getClusterAppPath(Const.OLD)
            if oldPath == "":
                self.context.logger.debug("Cannot get the old install "
                                          "path from table and file.")
                oldPath = os.path.realpath(gaussHome)
            self.context.logger.debug("Old cluster app path is %s" % oldPath)

            oldVersionFile = "%s/bin/upgrade_version" % oldPath
            try:
                (oldClusterVersion, oldClusterNumber, oldCommitId) = \
                    VersionInfo.get_version_info(oldVersionFile)
                self.context.logger.debug("Successfully obtained version"
                                          " information of old clusters by %s."
                                          % oldVersionFile)
            except Exception as e:
                if os.path.exists(self.context.upgradeBackupPath):
                    # if upgradeBackupPath exist,
                    # it means that we do rollback first.
                    # and we get cluster version from the backup file
                    possibOldVersionFile = "%s/old_upgrade_version" \
                                           % self.context.upgradeBackupPath
                    self.context.logger.debug(str(e))
                    self.context.logger.debug(
                        "Try to get the version information from %s."
                        % possibOldVersionFile)
                    (oldClusterVersion, oldClusterNumber, oldCommitId) = \
                        VersionInfo.get_version_info(possibOldVersionFile)
                else:
                    raise Exception(str(e))

            if self.context.action == Const.ACTION_AUTO_ROLLBACK or \
                    self.context.action == Const.ACTION_COMMIT_UPGRADE:
                inplace_upgrade_flag_file = "%s/inplace_upgrade_flag" \
                                            % self.context.upgradeBackupPath
                # we do rollback by the backup directory
                if os.path.isfile(inplace_upgrade_flag_file):
                    self.context.logger.debug(
                        "inplace upgrade flag exists, "
                        "use inplace rollback or commit.")
                    self.context.is_inplace_upgrade = True

            # if last success commit upgrade_type is grey upgrade,
            # the symbolic link should point to the
            # old app path with old commit id
            if oldCommitId == newCommitId:
                raise Exception(ErrorCode.GAUSS_529["GAUSS_52901"])
            self.context.logger.debug(
                "Successfully obtained version information of new and old "
                "clusters.\n           The old cluster number:%s, the new "
                "cluster number:%s." % (oldClusterNumber, newClusterNumber))
            if oldClusterVersion > newClusterVersion:
                raise Exception(ErrorCode.GAUSS_529["GAUSS_52902"]
                                % (oldClusterVersion, newClusterVersion))

            self.checkLastUpgrade(newCommitId)

            if float(newClusterNumber) < float(oldClusterNumber):
                raise Exception(ErrorCode.GAUSS_516["GAUSS_51629"]
                                % newClusterNumber)
            elif float(newClusterNumber) == float(oldClusterNumber):
                upgradeAction = Const.ACTION_INPLACE_UPGRADE
            else:
                if int(float(newClusterNumber)) > int(float(oldClusterNumber)):
                    raise Exception(ErrorCode.GAUSS_529["GAUSS_52904"]
                                    + "This cluster version is "
                                      "not supported upgrade.")
                elif ((float(newClusterNumber) - int(float(newClusterNumber)))
                      > (float(oldClusterNumber) -
                         int(float(oldClusterNumber)))):
                    raise Exception(ErrorCode.GAUSS_529["GAUSS_52904"]
                                    + "This cluster version is "
                                      "not supported upgrade.")
                else:
                    raise Exception(ErrorCode.GAUSS_516["GAUSS_51629"]
                                    % newClusterNumber)
            self.context.logger.debug("The matched upgrade strategy is: %s."
                                      % upgradeAction)
            self.context.newClusterVersion = newClusterVersion
            self.context.newClusterNumber = newClusterNumber
            self.context.oldClusterVersion = oldClusterVersion
            self.context.oldClusterNumber = oldClusterNumber
            self.context.newClusterAppPath = newPath
            self.context.oldClusterAppPath = oldPath
            self.newCommitId = newCommitId
            self.oldCommitId = oldCommitId
            return upgradeAction
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_529["GAUSS_52900"] % str(e)
                            + " Do nothing this time.")

    def checkLastUpgrade(self, newCommitId):
        """
        check the last fail upgrade type is same with this time
        check the last upgrade version is same with this time
        under grey upgrade, if under inplace upgrade, we will
        rollback first, under grey upgrade, we will upgrade again
        """
        if self.context.action == Const.ACTION_AUTO_UPGRADE:
            stepFile = os.path.join(self.context.upgradeBackupPath,
                                    Const.GREY_UPGRADE_STEP_FILE)
            cmd = "if [ -f '%s' ]; then echo 'True';" \
                  " else echo 'False'; fi" % stepFile
            (resultMap, outputCollect) = \
                self.context.sshTool.getSshStatusOutput(cmd)
            self.context.logger.debug(
                "The result of checking grey upgrade step flag"
                " file on all nodes is:\n%s" % outputCollect)
            if self.context.is_inplace_upgrade:
                # if the grey upgrade rollback failed, it should have file,
                # so cannot do grey upgrade now
                if outputCollect.find('True') >= 0:
                    ermsg = ErrorCode.GAUSS_502["GAUSS_50200"] \
                            % Const.GREY_UPGRADE_STEP_FILE \
                            + "In grey upgrade process, " \
                              "cannot do inplace upgrade!"
                    raise Exception(str(ermsg))
        elif self.context.action == Const.ACTION_AUTO_ROLLBACK or \
                self.context.action == Const.ACTION_COMMIT_UPGRADE:
            self.checkNewCommitid(newCommitId)

    def checkNewCommitid(self, newCommitId):
        """
        the commitid is in version.cfg, it should be same with the record
        commitid in record app directory file
        :param newCommitId: version.cfg line 3
        :return: NA
        """
        newPath = self.getClusterAppPath(Const.NEW)
        if newPath != "":
            LastNewCommitId = newPath[-8:]
            # When repeatedly run gs_upgradectl script,
            # this time upgrade version should be same
            # with last record upgrade version
            if newCommitId != LastNewCommitId:
                raise Exception(ErrorCode.GAUSS_529["GAUSS_52935"])

    def checkOldClusterVersion(self, gaussdbPath, oldClusterVersionFile):
        """
        check old cluster version
        input : gaussdbPath, oldClusterVersionFile
        output:
            1. (0,"V100R00XCXX")
            2. (999,"NAC00Version")
            3. (1, errorMsg)
        otherwise raise exception
        """
        if os.path.isfile(oldClusterVersionFile):
            cmd = "cat %s" % oldClusterVersionFile
        else:
            gaussdbFile = "%s/gaussdb" % gaussdbPath
            if not os.path.exists(gaussdbFile):
                self.context.logger.debug("The %s does not exist."
                                          " Cannot obtain old cluster"
                                          " version." % gaussdbFile)
                return 1, " The %s does not exist. Cannot " \
                          "obtain old cluster version" % gaussdbFile
            if not os.path.isfile(gaussdbFile):
                self.context.logger.debug("The %s is not a file. "
                                          "Cannot obtain old cluster"
                                          " version." % gaussdbFile)
                return 1, " The %s is not a file. Cannot " \
                          "obtain old cluster version" % gaussdbFile
            # get old cluster version by gaussdb
            # the information of gaussdb like this:
            #    gaussdb Gauss200 V100R00XCXX build xxxx
            #    compiled at xxxx-xx-xx xx:xx:xx
            cmd = "export LD_LIBRARY_PATH=%s/lib:$LD_LIBRARY_PATH;%s " \
                  "--version" % (os.path.dirname(gaussdbPath), gaussdbFile)

        self.context.logger.debug("Command for getting old"
                                  " cluster version:%s" % cmd)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status == 0 and re.compile(r'V[0-9]{3}R[0-9]{3}C[0-9]{2}'
                                      ).search(str(output)) is not None:
            return 0, re.compile(
                r'V[0-9]{3}R[0-9]{3}C[0-9]{2}').search(str(output)).group()
        else:
            self.context.logger.debug("Failed to obtain old cluster"
                                      " version. Error: \n%s" % str(output))
            return 999, "NAC00Version"

    def setGUCValue(self, gucKey, gucValue, actionType="reload"):
        """
        function: do gs_guc
        input : gucKey - parameter name
                gucValue - parameter value
                actionType - guc action type(set/reload). default is 'reload'
        """
        userProfile = DefaultValue.getMpprcFile()
        if gucValue != "":
            gucStr = "%s='%s'" % (gucKey, gucValue)
        else:
            gucStr = "%s" % gucKey

        cmd = "source %s ;" % userProfile
        cmd += "gs_guc %s -N all -I all -c \"%s\"" % (actionType, gucStr)
        self.context.logger.debug("Command for setting "
                                  "GUC parameter %s: %s" % (gucKey, cmd))
        (status, output) = DefaultValue.retryGetstatusoutput(cmd)
        return status, output

    def setClusterReadOnlyMode(self):
        """
        function: set cluster read only mode
        input  : NA
        output : int
        """
        self.context.logger.debug("Setting up the cluster read-only mode.")
        (status, output) = self.setGUCValue("default_transaction_read_only",
                                            "true")
        if status == 0:
            self.context.logger.debug("successfully set the "
                                      "cluster read-only mode.")
            return 0
        else:
            self.context.logger.debug(
                "Failed to set default_transaction_read_only parameter."
                + " Error: \n%s" % str(output))
            return 1

    def unSetClusterReadOnlyMode(self):
        """
        function: Canceling the cluster read-only mode
        input : NA
        output: 0  successfully
                1  failed
        """
        self.context.logger.debug("Canceling the cluster read-only mode.")
        # un set cluster read only mode
        (status, output) = self.setGUCValue("default_transaction_read_only",
                                            "false")
        if status == 0:
            self.context.logger.debug("Successfully cancelled the"
                                      " cluster read-only mode.")
            return 0
        else:
            self.context.logger.debug(
                "Failed to set default_transaction_read_only parameter."
                + " Error: \n%s" % str(output))
            return 1

    def stopCluster(self):
        """
        function: Stopping the cluster
        input : NA
        output: NA
        """
        self.context.logger.log("Stopping the cluster.", "addStep")
        # Stop cluster applications
        cmd = "%s -U %s -R %s -t %s" % (
            OMCommand.getLocalScript("Local_StopInstance"),
            self.context.user, self.context.clusterInfo.appPath,
            Const.UPGRADE_TIMEOUT_CLUSTER_STOP)
        self.context.logger.debug("Command for stop cluster: %s" % cmd)
        DefaultValue.execCommandWithMode(
            cmd, "Stop cluster", self.context.sshTool,
            self.context.isSingle or self.context.localMode,
            self.context.mpprcFile)
        self.context.logger.log("Successfully stopped cluster.")

    def startCluster(self):
        """
        function: start cluster
        input : NA
        output: NA
        """
        cmd = "%s -U %s -R %s -t %s" % (
            OMCommand.getLocalScript("Local_StartInstance"),
            self.context.user, self.context.clusterInfo.appPath,
            Const.UPGRADE_TIMEOUT_CLUSTER_START)
        DefaultValue.execCommandWithMode(
            cmd, "Start cluster", self.context.sshTool,
            self.context.isSingle or self.context.localMode,
            self.context.mpprcFile)
        self.context.logger.log("Successfully started cluster.")

    def createCommitFlagFile(self):
        """
        function: create a flag file, if this file exists,
                  means that user have called commit interface,
                  but still not finished. if create failed, script should exit.
        input : NA
        output: NA
        """
        commitFlagFile = "%s/commitFlagFile" % self.context.upgradeBackupPath
        self.context.logger.debug("Start to create the commit flag file.")
        try:
            cmd = "(if [ -d '%s' ]; then touch '%s'; fi) " % (
                self.context.upgradeBackupPath, commitFlagFile)
            DefaultValue.execCommandWithMode(cmd,
                                             "create commit flag file",
                                             self.context.sshTool,
                                             self.context.isSingle,
                                             self.context.mpprcFile)
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50206"]
                            % ("commit flag file: %s" % str(e)))
        self.context.logger.debug("Successfully created the commit flag file.")

    def checkCommitFlagFile(self):
        """
        function: check if commit flag file exists.
        input : NA
        output: return 0, If there is the file commitFlagFile.
                else, return 1
        """
        commitFlagFile = "%s/commitFlagFile" % self.context.upgradeBackupPath
        if (os.path.isfile(commitFlagFile)):
            return 0
        else:
            return 1

    def createInplaceUpgradeFlagFile(self):
        """
        function: create inplace upgrade flag file on
                  all nodes if is doing inplace upgrade
                  1.check if is inplace upgrade
                  2.get new and old cluster version number
                  3.write file
        Input: NA
        output : NA
        """
        self.context.logger.debug("Start to create inplace upgrade flag file.")
        try:
            newClusterNumber = self.context.newClusterNumber
            oldClusterNumber = self.context.oldClusterNumber

            inplace_upgrade_flag_file = "%s/inplace_upgrade_flag" % \
                                        self.context.upgradeBackupPath
            g_file.createFile(inplace_upgrade_flag_file)
            g_file.writeFile(inplace_upgrade_flag_file,
                             ["newClusterNumber:%s" % newClusterNumber], 'a')
            g_file.writeFile(inplace_upgrade_flag_file,
                             ["oldClusterNumber:%s" % oldClusterNumber], 'a')
            if (not self.context.isSingle):
                self.context.sshTool.scpFiles(inplace_upgrade_flag_file,
                                              self.context.upgradeBackupPath)

            self.context.logger.debug("Successfully created inplace"
                                      " upgrade flag file.")
        except Exception as e:
            raise Exception(str(e))

    def setUpgradeFromParam(self, ClusterVersionNumber, isCheck=True):
        """
        function: set upgrade_from parameter
        Input : oldClusterNumber, isCheck
        output : NA
        """
        self.context.logger.debug("Set upgrade_from guc parameter.")
        workingGrandVersion = int(float(ClusterVersionNumber) * 1000)
        cmd = "gs_guc set -Z cmagent -N all -I all -c " \
              "'upgrade_from=%s'" % workingGrandVersion
        self.context.logger.debug("Command for setting cmagent"
                                  " parameter: %s." % cmd)
        try:
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                self.context.logger.debug("Set upgrade_from parameter"
                                          " failed. cmd:%s\nOutput:%s"
                                          % (cmd, str(output)))
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                                + "Error: \n%s" % str(output))
            if isCheck:
                gucStr = "%s:%s" % ("upgrade_from",
                                    str(workingGrandVersion).strip())
                self.checkParam(gucStr)
            self.context.logger.debug("Successfully set cmagent parameter "
                                      "upgrade_from=%s." % workingGrandVersion)
        except Exception as e:
            if self.context.action == Const.ACTION_INPLACE_UPGRADE or not \
                    self.context.forceRollback:
                raise Exception(str(e))
            self.context.logger.log("Failed to set upgrade_from,"
                                    " please set it manually with"
                                    " command: \n%s" % str(cmd))


    def setUpgradeMode(self, mode):
        """
        function: set upgrade_mode parameter
        Input : mode
        output : NA
        """
        try:
            self.setUpgradeModeGuc(mode)
        except Exception as e:
            if self.context.action == Const.ACTION_INPLACE_UPGRADE or \
                    not self.context.forceRollback:
                raise Exception(str(e))
            try:
                self.setUpgradeModeGuc(mode, "set")
            except Exception as e:
                self.context.logger.log("Failed to set upgrade_mode,"
                                        " please set it manually.")

    def setUpgradeModeGuc(self, mode, setType="reload"):
        """
        function: set upgrade mode guc
        input  : mode, setType
        output : NA
        """
        self.context.logger.debug("Set upgrade_mode guc parameter.")
        cmd = "gs_guc %s -Z coordinator -Z datanode -N all " \
              "-I all -c 'upgrade_mode=%d'" % (setType, mode)
        self.context.logger.debug("Command for setting database"
                                  " node parameter: %s." % cmd)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            self.context.logger.debug("Set upgrade_mode parameter "
                                      "failed. cmd:%s\nOutput:%s"
                                      % (cmd, str(output)))
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                            + "Error: \n%s" % str(output))
        gucStr = "upgrade_mode:%d" % mode
        self.checkParam(gucStr)
        self.context.logger.debug("Successfully set "
                                  "upgrade_mode to %d." % mode)

    def checkParam(self, gucStr):
        """
        function: check the cmagent guc value
        Input : gucStr the guc key:value string
        output : NA
        """
        self.context.logger.debug("Start to check GUC value %s." % gucStr)
        try:
            # send cmd to that node and exec
            cmd = "%s -t %s -U %s --upgrade_bak_path=%s" \
                  " --guc_string=%s -l %s" % \
                  (OMCommand.getLocalScript("Local_Upgrade_Utility"),
                   Const.ACTION_CHECK_GUC,
                   self.context.user,
                   self.context.upgradeBackupPath,
                   gucStr,
                   self.context.localLog)
            self.context.logger.debug("Command for checking"
                                      " parameter: %s." % cmd)
            DefaultValue.execCommandWithMode(cmd,
                                             "check GUC value",
                                             self.context.sshTool,
                                             self.context.isSingle,
                                             self.context.mpprcFile)
            self.context.logger.debug("Successfully checked guc value.")
        except Exception as e:
            raise Exception(str(e))

    def floatMoreThan(self, numOne, numTwo):
        """
        function: float more than
        input  : numOne, numTwo
        output : True/False
        """
        if float(numOne) - float(numTwo) > float(Const.DELTA_NUM):
            return True
        return False

    def floatLessThan(self, numOne, numTwo):
        """
        function: float less than
        input: numOne, numTwo
        output: True/False
        """
        if float(numOne) - float(numTwo) < float(-Const.DELTA_NUM):
            return True
        return False

    def floatEqualTo(self, numOne, numTwo):
        """
        function: float equal to
        input: numOne, numTwo
        output: True/False
        """
        if float(-Const.DELTA_NUM) < (float(numOne) - float(numTwo)) \
                < float(Const.DELTA_NUM):
            return True
        return False

    def floatGreaterOrEqualTo(self, numOne, numTwo):
        """
        function: float greater or equal to
        input: numOne, numTwo
        output: True/False
        """
        if self.floatMoreThan(numOne, numTwo) or \
                self.floatEqualTo(numOne, numTwo):
            return True
        return False

    def doInplaceBinaryUpgrade(self):
        """
        function: do binary upgrade, which essentially replace the binary files
        input : NA
        output: NA
        """
        # 1. distribute new package to every nodes.
        self.distributeXml()
        # 2. check whether we should do rollback or not.
        if not self.doInplaceBinaryRollback():
            self.exitWithRetCode(Const.ACTION_AUTO_ROLLBACK, False)
        try:
            self.checkUpgrade()

            # 3. before do binary upgrade, we must make sure the cluster is
            # Normal and the database could be connected
            #    if not, exit.
            self.startCluster()
            if self.unSetClusterReadOnlyMode() != 0:
                raise Exception("NOTICE: "
                                + ErrorCode.GAUSS_529["GAUSS_52907"])
            self.recordNodeStepInplace(Const.ACTION_INPLACE_UPGRADE,
                                       Const.BINARY_UPGRADE_STEP_INIT_STATUS)

            (status, output) = self.doHealthCheck(Const.OPTION_PRECHECK)
            if status != 0:
                self.exitWithRetCode(Const.ACTION_INPLACE_UPGRADE, False,
                                     ErrorCode.GAUSS_516["GAUSS_51601"]
                                     % "cluster" + output)
            # 4.record the old and new app dir in file
            self.recordDirFile()

            if self.setClusterReadOnlyMode() != 0:
                raise Exception(ErrorCode.GAUSS_529["GAUSS_52908"])

            # after checkUpgrade, the bak path is ready, we can use it now
            # create inplace upgrade flag file if is doing inplace upgrade
            self.createInplaceUpgradeFlagFile()
            # 7. backup current application and configuration.
            # The function only be used by binary upgrade.
            #    to ensure the transaction atomicity,
            #    it will be used with checkUpgrade().
            self.backupNodeVersion()
            # 8. stop old cluster
            self.recordNodeStepInplace(Const.ACTION_INPLACE_UPGRADE,
                                       Const.BINARY_UPGRADE_STEP_STOP_NODE)
            self.context.logger.debug("Start to stop all instances"
                                      " on the node.", "addStep")
            self.stopCluster()
            self.context.logger.debug("Successfully stop all"
                                      " instances on the node.", "constant")
            # 9. back cluster config. including this:
            #    cluster_static_config
            #    cluster_dynamic_config
            #    etc/gscgroup_xxx.cfg
            #    lib/postgresql/pg_plugin
            #    server.key.cipher
            #    server.key.rand
            #    Data Studio lib files
            #    gds files
            #    physical catalog files if performing inplace upgrade
            self.recordNodeStepInplace(
                Const.ACTION_INPLACE_UPGRADE,
                Const.BINARY_UPGRADE_STEP_BACKUP_VERSION)
            self.backupClusterConfig()

            # 10. Upgrade application on node
            #     install new bin file
            self.recordNodeStepInplace(Const.ACTION_INPLACE_UPGRADE,
                                       Const.BINARY_UPGRADE_STEP_UPGRADE_APP)
            self.installNewBin()

            # 11. restore the cluster config. including this:
            #    cluster_static_config
            #    cluster_dynamic_config
            #    etc/gscgroup_xxx.cfg
            #    lib/postgresql/pg_plugin
            #    server.key.cipher 
            #    server.key.rand
            #    Data Studio lib files
            #    gds files
            #    cn cert files
            #    At the same time, sync newly added guc for instances
            self.restoreClusterConfig()
            self.syncNewGUC()
            # 12. modify GUC parameter unix_socket_directory
            self.modifySocketDir()
            # 13. start new cluster
            self.recordNodeStepInplace(Const.ACTION_INPLACE_UPGRADE,
                                       Const.BINARY_UPGRADE_STEP_START_NODE)
            self.context.logger.debug("Start to start all instances"
                                      " on the node.", "addStep")

            # update catalog
            # start cluster in normal mode
            self.CopyCerts()
            self.context.createGrpcCa()
            self.context.logger.debug("Successfully createGrpcCa.")
            self.switchBin(Const.NEW)

            self.startCluster()
            self.context.logger.debug("Successfully start all "
                                      "instances on the node.", "constant")
            # 14. check the cluster status
            (status, output) = self.doHealthCheck(Const.OPTION_POSTCHECK)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_516["GAUSS_51601"]
                                % "cluster" + output)

            # 15. record precommit step status
            self.recordNodeStepInplace(Const.ACTION_INPLACE_UPGRADE,
                                       Const.BINARY_UPGRADE_STEP_PRE_COMMIT)
            self.printPrecommitBanner()
        except Exception as e:
            self.context.logger.error(str(e))
            self.context.logger.log("Binary upgrade failed. Rollback"
                                    " to the original cluster.")
            # do rollback
            self.exitWithRetCode(Const.ACTION_AUTO_ROLLBACK,
                                 self.doInplaceBinaryRollback())
        self.exitWithRetCode(Const.ACTION_INPLACE_UPGRADE, True)

    def doInplaceCommitUpgrade(self):
        """
        function: commit binary upgrade and clean up backup files
                  1. unset read-only
                  2. drop old PMK schema
                  3. restore UDF
                  4. clean backup catalog physical
                   files if doing inplace upgrade
                  5. clean up other upgrade tmp files
        input : NA
        output: NA
        """
        if self.getNodeStepInplace() != Const.BINARY_UPGRADE_STEP_PRE_COMMIT:
            raise Exception(ErrorCode.GAUSS_529["GAUSS_52916"]
                            + " Please check if previous upgrade"
                              " operation was successful or if"
                              " upgrade has already been committed.")
        # create commit flag file
        self.createCommitFlagFile()

        # variable to indicate whether we should keep step file
        # and cleanup list file for re-entry
        cleanUpSuccess = True

        # 1.unset read-only
        if self.unSetClusterReadOnlyMode() != 0:
            self.context.logger.log("NOTICE: "
                                    + ErrorCode.GAUSS_529["GAUSS_52907"])
            cleanUpSuccess = False

        # 2. drop old PMK schema
        # we sleep 10 seconds first because DB might be updating
        # ha status after unsetting read-only
        time.sleep(10)

        if not cleanUpSuccess:
            self.context.logger.log("NOTICE: Cleanup is incomplete during"
                                    " commit. Please re-commit upgrade once"
                                    " again or cleanup manually")
            self.exitWithRetCode(Const.ACTION_INPLACE_UPGRADE, False)
        else:
            # 8. clean up other upgrade tmp files
            # and uninstall inplace upgrade support functions
            self.cleanInstallPath(Const.OLD)
            self.cleanBinaryUpgradeBakFiles()

            self.context.logger.log("Commit binary upgrade succeeded.")
            self.exitWithRetCode(Const.ACTION_INPLACE_UPGRADE, True)

    def setNewVersionGuc(self):
        """
        function: set new Version guc
        input  : NA
        output : NA
        """
        pass

    def setActionFile(self):
        """
        set the action from step file, if not find, set it to large upgrade,
        if the upgrade type is small upgrade, but we set it to large upgrade,
        just kill the cm agent as expense, take no effect to transaction
        But if the action should be large, we does not set the upgrade_mode,
        some new feature will not opened
        :return: NA
        """
        stepFile = os.path.join(self.context.upgradeBackupPath,
                                Const.GREY_UPGRADE_STEP_FILE)
        self.context.logger.debug("Get the action from file %s." % stepFile)
        if not os.path.exists(stepFile) or os.path.isfile(stepFile):
            self.context.logger.debug("Step file does not exists or not file,"
                                      " cannot get action from it. "
                                      "Set it to large upgrade.")
            self.context.action = Const.ACTION_LARGE_UPGRADE
            return
        with open(stepFile, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                self.context.action = row['upgrade_action']
                break
        self.context.logger.debug("Set the action to %s"
                                  % self.context.action)

    def getClusterAppPath(self, mode=Const.OLD):
        """
        if cannot get path from table, try to get from the backup file
        :param mode:
        :return:
        """
        self.context.logger.debug("Get the install path from table or file.")
        path = self.getClusterAppPathFromFile(mode)
        return path

    def getClusterAppPathFromFile(self, mode=Const.OLD):
        """
        get the app path from backup dir, mode is new or old,
        :param mode: 'old', 'new'
        :return: the real path of appPath
        """
        dirFile = "%s/%s" % (self.context.upgradeBackupPath,
                             Const.RECORD_UPGRADE_DIR)
        self.context.logger.debug("Get the %s app path from file %s"
                                  % (mode, dirFile))
        if mode not in [Const.OLD, Const.NEW]:
            raise Exception(traceback.format_exc())
        if not os.path.exists(dirFile):
            self.context.logger.debug(ErrorCode.GAUSS_502["GAUSS_50201"]
                                      % dirFile)
            if self.checkBakPathNotExists():
                return ""
            # copy the binary_upgrade dir from other node,
            # if one node is damaged while binary_upgrade may disappear,
            # user repair one node before commit, and send the commit
            # command to the repair node, we need to copy the
            # dir from remote node
            cmd = "if [ -f '%s' ]; then echo 'GetFile';" \
                  " else echo 'NoThisFile'; fi" % dirFile
            self.context.logger.debug("Command for checking file: %s" % cmd)
            (status, output) = self.context.sshTool.getSshStatusOutput(
                cmd, self.context.clusterNodes, self.context.mpprcFile)
            outputMap = self.context.sshTool.parseSshOutput(
                self.context.clusterNodes)
            self.context.logger.debug("Output: %s" % output)
            copyNode = ""
            for node in self.context.clusterNodes:
                if status[node] == DefaultValue.SUCCESS:
                    if 'GetFile' in outputMap[node]:
                        copyNode = node
                        break
            if copyNode:
                if not os.path.exists(self.context.upgradeBackupPath):
                    self.context.logger.debug("Create directory %s."
                                              % self.context.tmpDir)
                    g_file.createDirectory(
                        self.context.upgradeBackupPath, True,
                        DefaultValue.KEY_DIRECTORY_MODE)
                self.context.logger.debug("Copy the directory %s from node %s."
                                          % (self.context.upgradeBackupPath,
                                             copyNode))
                cmd = g_Platform.getRemoteCopyCmd(
                    self.context.upgradeBackupPath, self.context.tmpDir,
                    str(copyNode), False, 'directory')
                self.context.logger.debug("Command for copying "
                                          "directory: %s" % cmd)
                DefaultValue.execCommandLocally(cmd)
            else:
                # binary_upgrade exists, but no step file
                return ""
        if not os.path.isfile(dirFile):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50210"] % dirFile)
        with open(dirFile, 'r') as fp:
            retLines = fp.readlines()
        if len(retLines) != 2:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50222"] % dirFile)
        if mode == Const.OLD:
            path = retLines[0].strip()
        else:
            path = retLines[1].strip()
        # if can get the path from file, the path must be valid,
        # otherwise the file is damaged accidentally
        DefaultValue.checkPathVaild(path)
        if not os.path.exists(path):
            if mode == Const.NEW and \
                    self.context.action == Const.ACTION_AUTO_ROLLBACK:
                self.context.logger.debug("Under rollback, the new "
                                          "cluster app path does not exists.")
            elif mode == Const.OLD and \
                    self.context.action == Const.ACTION_COMMIT_UPGRADE:
                self.context.logger.debug("Under commit, no need to "
                                          "check the old path exists.")
            else:
                self.context.logger.debug(traceback.format_exc())
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % path)
        self.context.logger.debug("Successfully Get the app"
                                  " path [%s] from file" % path)
        return path

    def printPrecommitBanner(self):
        """
        funcation: if in pre-commit status, and do not execute
                   the commit cmd, then can print this message
        input : NA
        output: NA
        """
        self.context.logger.log("Upgrade main process has been finished,"
                                " user can do some check now.")
        self.context.logger.log("Once the check done, please execute "
                                "following command to commit upgrade:")
        xmlFile = self.context.xmlFile \
            if len(self.context.xmlFile) else "XMLFILE"
        self.context.logger.log("\n    gs_upgradectl -t "
                                "commit-upgrade -X %s   \n" % xmlFile)

    def doInplaceBinaryRollback(self):
        """
        function: rollback the upgrade of binary
        input : NA
        output: return True, if the operation is done successfully.
                return False, if the operation failed.
        """
        self.context.logger.log("Performing inplace rollback.")
        # step flag
        # Const.BINARY_UPGRADE_NO_NEED_ROLLBACK value is -2
        # Const.INVALID_UPRADE_STEP value is -1
        # Const.BINARY_UPGRADE_STEP_INIT_STATUS value is 0
        # Const.BINARY_UPGRADE_STEP_BACKUP_STATUS value is 1
        # Const.BINARY_UPGRADE_STEP_STOP_NODE value is 2
        # Const.BINARY_UPGRADE_STEP_BACKUP_VERSION value is 3
        # Const.BINARY_UPGRADE_STEP_UPGRADE_APP value is 4
        # Const.BINARY_UPGRADE_STEP_START_NODE value is 5
        # Const.BINARY_UPGRADE_STEP_PRE_COMMIT value is 6
        self.distributeXml()
        step = self.getNodeStepInplace()
        if step == Const.BINARY_UPGRADE_NO_NEED_ROLLBACK:
            self.context.logger.log("Rollback succeeded.")
            return True

        # if step <= -1, it means the step file is broken, exit.
        if step <= Const.INVALID_UPRADE_STEP:
            self.context.logger.debug("Invalid upgrade step: %s." % str(step))
            return False

        # if step value is Const.BINARY_UPGRADE_STEP_PRE_COMMIT
        # and find commit flag file,
        # means user has commit upgrade, then can not do rollback
        if step == Const.BINARY_UPGRADE_STEP_PRE_COMMIT:
            if not self.checkCommitFlagFile():
                self.context.logger.log(
                    "Upgrade has already been committed, "
                    "can not execute rollback command any more.")
                return False

        try:
            self.checkStaticConfig()
            # Mark that we leave pre commit status,
            # so that if we fail at the first few steps,
            # we won't be allowed to commit upgrade any more.
            if step == Const.BINARY_UPGRADE_STEP_PRE_COMMIT:
                self.recordNodeStepInplace(
                    Const.ACTION_INPLACE_UPGRADE,
                    Const.BINARY_UPGRADE_STEP_START_NODE)

            if step >= Const.BINARY_UPGRADE_STEP_START_NODE:
                self.restoreClusterConfig(True)
                self.switchBin(Const.OLD)
                self.stopCluster()
                self.recordNodeStepInplace(
                    Const.ACTION_INPLACE_UPGRADE,
                    Const.BINARY_UPGRADE_STEP_UPGRADE_APP)

            if step >= Const.BINARY_UPGRADE_STEP_UPGRADE_APP:
                self.restoreNodeVersion()
                self.restoreClusterConfig(True)
                self.recordNodeStepInplace(
                    Const.ACTION_INPLACE_UPGRADE,
                    Const.BINARY_UPGRADE_STEP_BACKUP_VERSION)

            if step >= Const.BINARY_UPGRADE_STEP_BACKUP_VERSION:
                self.recordNodeStepInplace(
                    Const.ACTION_INPLACE_UPGRADE,
                    Const.BINARY_UPGRADE_STEP_STOP_NODE)

            if step >= Const.BINARY_UPGRADE_STEP_STOP_NODE:
                self.startCluster()
                self.recordNodeStepInplace(
                    Const.ACTION_INPLACE_UPGRADE,
                    Const.BINARY_UPGRADE_STEP_INIT_STATUS)

            if step >= Const.BINARY_UPGRADE_STEP_INIT_STATUS:
                if self.unSetClusterReadOnlyMode() != 0:
                    raise Exception("NOTICE: " +
                                    ErrorCode.GAUSS_529["GAUSS_52907"])
                self.cleanBinaryUpgradeBakFiles(True)
                self.cleanInstallPath(Const.NEW)
        except Exception as e:
            self.context.logger.error(str(e))
            self.context.logger.log("Rollback failed.")
            return False

        self.context.logger.log("Rollback succeeded.")
        return True

    def getSqlHeader(self):
        """
        function: get sql header
        input  : NA
        output : NA
        """
        header = ["START TRANSACTION;"]
        header.append("SET %s = on;" % Const.ON_INPLACE_UPGRADE)
        header.append("SET search_path = 'pg_catalog';")
        header.append("SET local client_min_messages = NOTICE;")
        header.append("SET local log_min_messages = NOTICE;")
        return header

    def getFileNameList(self, filePathName):
        """
        function: get file name list
        input  : filePathName
        output : []
        """
        filePath = "%s/upgrade_sql/%s" % (self.context.upgradeBackupPath,
                                          filePathName)
        allFileList = os.listdir(filePath)
        upgradeFileList = []
        if len(allFileList) == 0:
            return []
        for each_sql_file in allFileList:
            if not os.path.isfile("%s/%s" % (filePath, each_sql_file)):
                continue
            prefix = each_sql_file.split('.')[0]
            resList = prefix.split('_')
            if len(resList) != 5:
                continue
            file_num = "%s.%s" % (resList[3], resList[4])

            if self.floatMoreThan(float(file_num),
                                  self.context.oldClusterNumber) and \
                    self.floatGreaterOrEqualTo(self.context.newClusterNumber,
                                               float(file_num)):
                upgradeFileList.append(each_sql_file)
        return upgradeFileList

    def initClusterInfo(self, dbClusterInfoPath):
        """
        function: init the cluster 
        input : dbClusterInfoPath
        output: dbClusterInfo
        """
        clusterInfoModules = OldVersionModules()
        fileDir = os.path.dirname(os.path.realpath(dbClusterInfoPath))
        sys.path.insert(0, fileDir)
        # init cluster information
        clusterInfoModules.oldDbClusterInfoModule = __import__('DbClusterInfo')
        sys.path.remove(fileDir)
        return clusterInfoModules.oldDbClusterInfoModule.dbClusterInfo()

    def initOldClusterInfo(self, dbClusterInfoPath):
        """
        function: init old cluster information
        input : dbClusterInfoPath
        output: clusterInfoModules.oldDbClusterInfoModule.dbClusterInfo()
        """
        clusterInfoModules = OldVersionModules()
        fileDir = os.path.dirname(os.path.realpath(dbClusterInfoPath))
        # script and OldDbClusterInfo.py are in the same PGHOST directory
        sys.path.insert(0, fileDir)
        # V1R8 DbClusterInfo.py is "from gspylib.common.ErrorCode import
        # ErrorCode"
        sys.path.insert(0, os.path.join(fileDir, "script"))
        # init old cluster information
        clusterInfoModules.oldDbClusterInfoModule = \
            __import__('OldDbClusterInfo')
        return clusterInfoModules.oldDbClusterInfoModule.dbClusterInfo()

    def initClusterConfig(self):
        """
        function: init cluster info
        input : NA
        output: NA
        """
        gaussHome = \
            DefaultValue.getEnvironmentParameterValue("GAUSSHOME",
                                                      self.context.user)
        # $GAUSSHOME must has available value.
        if gaussHome == "":
            raise Exception(ErrorCode.GAUSS_518["GAUSS_51800"] % "$GAUSSHOME")
        (appPath, appPathName) = os.path.split(gaussHome)
        commonDbClusterInfoModule = \
            "%s/bin/script/gspylib/common/DbClusterInfo.py" % gaussHome
        commonStaticConfigFile = "%s/bin/cluster_static_config" % gaussHome
        try:
            if self.context.action == Const.ACTION_INPLACE_UPGRADE:

                # get DbClusterInfo.py and cluster_static_config both of backup
                # path and install path
                # get oldClusterInfo
                #     if the backup file exists, we use them;
                #     if the install file exists, we use them;
                #     else, we can not get oldClusterInfo, exit.
                # backup path exists
                commonDbClusterInfoModuleBak = "%s/../OldDbClusterInfo.py" % \
                                               self.context.upgradeBackupPath
                commonStaticConfigFileBak = "%s/../cluster_static_config" % \
                                            self.context.upgradeBackupPath

                # if binary.tar exist, decompress it
                if os.path.isfile("%s/%s" % (self.context.upgradeBackupPath,
                                             self.context.binTarName)):
                    cmd = "cd '%s'&&tar xfp '%s'" % \
                          (self.context.upgradeBackupPath,
                           self.context.binTarName)
                    (status, output) = subprocess.getstatusoutput(cmd)
                    if (status != 0):
                        raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] %
                                        cmd + "Error: \n%s" % str(output))

                if (os.path.isfile(commonDbClusterInfoModuleBak)
                        and os.path.isfile(commonStaticConfigFileBak)):
                    try:
                        # import old module
                        # init old cluster config
                        self.context.oldClusterInfo = \
                            self.initOldClusterInfo(
                                commonDbClusterInfoModuleBak)
                        self.context.oldClusterInfo.initFromStaticConfig(
                            self.context.user, commonStaticConfigFileBak)
                    except Exception as e:
                        # maybe the old cluster is V1R5C00 TR5 version, not
                        # support specify static config file
                        # path for initFromStaticConfig function,
                        # so use new cluster format try again
                        self.context.oldClusterInfo = dbClusterInfo()
                        self.context.oldClusterInfo.initFromStaticConfig(
                            self.context.user, commonStaticConfigFileBak)
                # if backup path not exist, then use install path
                elif (os.path.isfile(commonDbClusterInfoModule)
                      and os.path.isfile(commonStaticConfigFile)):
                    # import old module
                    # init old cluster config
                    self.context.oldClusterInfo = \
                        self.initClusterInfo(commonDbClusterInfoModule)
                    self.context.oldClusterInfo.initFromStaticConfig(
                        self.context.user, commonStaticConfigFile)
                else:
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] %
                                    "static config file")

                # get the accurate logPath
                logPathWithUser = DefaultValue.getEnv("GAUSSLOG")
                DefaultValue.checkPathVaild(logPathWithUser)
                splitMark = "/%s" % self.context.user
                self.context.oldClusterInfo.logPath = \
                    logPathWithUser[0:(logPathWithUser.rfind(splitMark))]

                # init new cluster config
                #     if xmlFile != "",  init it by initFromXml();
                #     else, using oldClusterInfo
                if self.context.xmlFile != "":
                    # get clusterInfo
                    # if falied to do dbClusterInfo, it means the
                    # DbClusterInfo.py is not correct
                    # we will use the backup file to instead of it
                    self.context.clusterInfo = dbClusterInfo()
                    try:
                        self.context.clusterInfo.initFromXml(
                            self.context.xmlFile)
                    except Exception as e:
                        self.context.logger.error(str(e))
                        try:
                            # init clusterinfo from backup dbclusterinfo
                            self.context.clusterInfo = \
                                self.initOldClusterInfo(
                                    commonDbClusterInfoModuleBak)
                            self.context.clusterInfo.initFromXml(
                                self.context.xmlFile)
                        except Exception as e:
                            try:
                                self.context.clusterInfo = \
                                    self.initClusterInfo(
                                        commonDbClusterInfoModule)
                                self.context.clusterInfo.initFromXml(
                                    self.context.xmlFile)
                            except Exception as e:
                                raise Exception(str(e))
                    # verify cluster config info between old and new cluster
                    self.verifyClusterConfigInfo(self.context.clusterInfo,
                                                 self.context.oldClusterInfo)
                    # after doing verifyClusterConfigInfo(),
                    # the clusterInfo and oldClusterInfo are be changed, 
                    # so we should do init it again
                    self.context.clusterInfo = dbClusterInfo()
                    try:
                        self.context.clusterInfo.initFromXml(
                            self.context.xmlFile)
                    except Exception as e:
                        self.context.logger.debug(str(e))
                        try:
                            # init clusterinfo from backup dbclusterinfo
                            self.context.clusterInfo = \
                                self.initOldClusterInfo(
                                    commonDbClusterInfoModuleBak)
                            self.context.clusterInfo.initFromXml(
                                self.context.xmlFile)
                        except Exception as e:
                            try:
                                self.context.clusterInfo = \
                                    self.initClusterInfo(
                                        commonDbClusterInfoModule)
                                self.context.clusterInfo.initFromXml(
                                    self.context.xmlFile)
                            except Exception as e:
                                raise Exception(str(e))
                else:
                    self.context.clusterInfo = self.context.oldClusterInfo
            elif (self.context.action == Const.ACTION_CHOSE_STRATEGY
                  or self.context.action == Const.ACTION_COMMIT_UPGRADE):
                # after switch to new bin, the gausshome points to newversion,
                # so the oldClusterNumber is same with
                # newClusterNumber, the oldClusterInfo is same with new
                try:
                    self.context.oldClusterInfo = self.context.clusterInfo
                    if os.path.isfile(commonDbClusterInfoModule) and \
                            os.path.isfile(commonStaticConfigFile):
                        # import old module
                        # init old cluster config
                        self.context.oldClusterInfo = \
                            self.initClusterInfo(commonDbClusterInfoModule)
                        self.context.oldClusterInfo.initFromStaticConfig(
                            self.context.user, commonStaticConfigFile)
                    else:
                        raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] %
                                        "static config file")
                except Exception as e:
                    # upgrade backup path
                    if (os.path.exists(
                            "%s/%s/bin/script/util/DbClusterInfo.py" % (
                            self.context.upgradeBackupPath, appPathName))):
                        binaryModuleBak = \
                            "%s/%s/bin/script/util/DbClusterInfo.py" % \
                            (self.context.upgradeBackupPath, appPathName)
                    else:
                        binaryModuleBak = \
                            "%s/%s/bin/script/gspylib/common/" \
                            "DbClusterInfo.py" % \
                            (self.context.upgradeBackupPath, appPathName)
                    binaryStaticConfigFileBak = \
                        "%s/%s/bin/cluster_static_config" % \
                        (self.context.upgradeBackupPath, appPathName)

                    if os.path.isfile(binaryModuleBak) and \
                            os.path.isfile(binaryStaticConfigFileBak):
                        # import old module
                        # init old cluster config
                        commonDbClusterInfoModuleBak = \
                            "%s/../OldDbClusterInfo.py" % \
                            self.context.upgradeBackupPath
                        self.context.oldClusterInfo = \
                            self.initOldClusterInfo(
                                commonDbClusterInfoModuleBak)
                        self.context.oldClusterInfo.initFromStaticConfig(
                            self.context.user, binaryStaticConfigFileBak)
                    else:
                        raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] %
                                        "static config file")
            elif (self.context.action in
                  [Const.ACTION_SMALL_UPGRADE, Const.ACTION_AUTO_UPGRADE,
                   Const.ACTION_LARGE_UPGRADE, Const.ACTION_AUTO_ROLLBACK]):
                # 1. get new cluster info
                self.context.clusterInfo = dbClusterInfo()
                self.context.clusterInfo.initFromXml(self.context.xmlFile)
                # 2. get oldClusterInfo
                # when under rollback
                # the gausshome may point to old or new clusterAppPath,
                # so we must choose from the record table
                # when upgrade abnormal nodes, the gausshome points to
                # newClusterAppPath

                oldPath = self.getClusterAppPath()
                if oldPath != "" and os.path.exists(oldPath):
                    self.context.logger.debug("The old install path is %s" %
                                              oldPath)
                    commonDbClusterInfoModule = \
                        "%s/bin/script/gspylib/common/DbClusterInfo.py" % \
                        oldPath
                    commonStaticConfigFile = \
                        "%s/bin/cluster_static_config" % oldPath
                else:
                    self.context.logger.debug("The old install path is %s"
                                              % os.path.realpath(gaussHome))
                if (os.path.isfile(commonDbClusterInfoModule)
                        and os.path.isfile(commonStaticConfigFile)):
                    # import old module
                    # init old cluster config
                    self.context.oldClusterInfo = \
                        self.initClusterInfo(commonDbClusterInfoModule)
                    self.context.oldClusterInfo.initFromStaticConfig(
                        self.context.user, commonStaticConfigFile)
                else:
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] %
                                    "static config file")

                staticClusterInfo = dbClusterInfo()
                config = os.path.join(gaussHome, "bin/cluster_static_config")
                if not os.path.isfile(config):
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] %
                                    os.path.realpath(config))
                staticClusterInfo.initFromStaticConfig(self.context.user,
                                                       config)

                # verify cluster config info between old and new cluster
                self.verifyClusterConfigInfo(self.context.clusterInfo,
                                             staticClusterInfo)
                # after doing verifyClusterConfigInfo(), the clusterInfo and
                # oldClusterInfo are be changed,
                # so we should do init it again
                self.context.clusterInfo = dbClusterInfo()
                # we will get the self.context.newClusterAppPath in
                # choseStrategy
                self.context.clusterInfo.initFromXml(self.context.xmlFile)
                self.context.logger.debug("Successfully init cluster config.")
            else:
                raise Exception(ErrorCode.GAUSS_500["GAUSS_50004"] % 't' +
                                " Value: %s" % self.context.action)
        except Exception as e:
            self.context.logger.debug(traceback.format_exc())
            self.exitWithRetCode(self.context.action, False, str(e))

    def verifyClusterConfigInfo(self, clusterInfo, oldClusterInfo,
                                ignoreFlag="upgradectl"):
        """
        function: verify cluster config info between xml and static config
        input : clusterInfo, oldClusterInfo
        output: NA
        """
        try:
            # should put self.context.clusterInfo before
            # self.context.oldClusterInfo,
            # because self.context.oldClusterInfo is not the istance of
            # dbCluster
            # covert new cluster information to compare cluster
            compnew = self.covertToCompCluster(clusterInfo)
            # covert old cluster information to compare cluster
            compold = self.covertToCompCluster(oldClusterInfo)
            # do compare
            # if it is not same, print it.
            theSame, tempbuffer = compareObject(compnew, compold,
                                                "clusterInfo", [], ignoreFlag)
            if (theSame):
                self.context.logger.log("Static configuration matched with "
                                        "old static configuration files.")
            else:
                msg = "Instance[%s] are not the same.\nXml cluster " \
                      "information: %s\nStatic cluster information: %s\n" % \
                      (tempbuffer[0], tempbuffer[1], tempbuffer[2])
                self.context.logger.debug("The old cluster information is "
                                          "from the cluster_static_config.")
                raise Exception(ErrorCode.GAUSS_512["GAUSS_51217"] +
                                "Error: \n%s" % msg.strip("\n"))
        except Exception as e:
            raise Exception(str(e))

    def covertToCompCluster(self, dbclusterInfo):
        """
        function: covert to comp cluster
        input : clusterInfo, oldClusterInfo
        output: compClusterInfo
        """
        # init dbcluster class
        compClusterInfo = dbClusterInfo()
        # get name
        compClusterInfo.name = dbclusterInfo.name
        # get appPath
        compClusterInfo.appPath = dbclusterInfo.appPath
        # get logPath
        compClusterInfo.logPath = dbclusterInfo.logPath

        for dbnode in dbclusterInfo.dbNodes:
            compNodeInfo = dbNodeInfo()
            # get datanode instance information
            for datanode in dbnode.datanodes:
                compNodeInfo.datanodes.append(
                    self.coverToCompInstance(datanode))
            # get node information
            compClusterInfo.dbNodes.append(compNodeInfo)
        return compClusterInfo

    def coverToCompInstance(self, compinstance):
        """
        function: cover to comp instance
                  1. get instanceId
                  2. get mirrorId
                  3. get port
                  4. get datadir
                  5. get instanceType
                  6. get listenIps
                  7. get haIps
        input : compinstance
        output: covertedInstanceInfo
        """
        covertedInstanceInfo = instanceInfo()
        # get instanceId
        covertedInstanceInfo.instanceId = compinstance.instanceId
        # get mirrorId
        covertedInstanceInfo.mirrorId = compinstance.mirrorId
        # get port
        covertedInstanceInfo.port = compinstance.port
        # get datadir
        covertedInstanceInfo.datadir = compinstance.datadir
        # get instanceType
        covertedInstanceInfo.instanceType = compinstance.instanceType
        # get listenIps
        covertedInstanceInfo.listenIps = compinstance.listenIps
        # get haIps
        covertedInstanceInfo.haIps = compinstance.haIps
        return covertedInstanceInfo

    def distributeXml(self):
        """
        function: distribute package to every host
        input : NA
        output: NA
        """
        self.context.logger.debug("Distributing xml configure file.",
                                  "addStep")

        try:

            hosts = self.context.clusterInfo.getClusterNodeNames()
            hosts.remove(DefaultValue.GetHostIpOrName())

            # Send xml file to every host
            DefaultValue.distributeXmlConfFile(self.context.sshTool,
                                               self.context.xmlFile,
                                               hosts,
                                               self.context.mpprcFile,
                                               self.context.isSingle)
        except Exception as e:
            raise Exception(str(e))

        self.context.logger.debug("Successfully distributed xml "
                                  "configure file.", "constant")

    def recordNodeStepInplace(self, action, step):
        """
        function: record step info on all nodes
        input : action, step
        output: NA
        """
        try:
            # record step info on local node

            tempPath = self.context.upgradeBackupPath
            filePath = os.path.join(tempPath, Const.INPLACE_UPGRADE_STEP_FILE)
            cmd = "echo \"%s:%d\" > %s" % (action, step, filePath)
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50205"] %
                                filePath + "Error: \n%s" % str(output))

            if not self.context.isSingle:
                # send file to remote nodes
                self.context.sshTool.scpFiles(filePath, tempPath)
            self.context.logger.debug("Successfully wrote step file[%s:%d]."
                                      % (action, step))
        except Exception as e:
            raise Exception(str(e))

    def distributeFile(self, step_file):
        """
        function: distribute file
        input  : step_file
        output : NA
        """
        self.context.logger.debug("Distribute the file %s" % step_file)
        # send the file to each node
        hosts = self.context.clusterInfo.getClusterNodeNames()
        hosts.remove(DefaultValue.GetHostIpOrName())
        if not self.context.isSingle:
            stepDir = os.path.normpath(os.path.dirname(step_file))
            self.context.sshTool.scpFiles(step_file, stepDir, hosts)
        self.context.logger.debug("Successfully distribute the file %s"
                                  % step_file)

    def getNodeStepInplace(self):
        """
        function: Get the upgrade step info for inplace upgrade
        input : action
        output: the upgrade step info
        """
        try:
            tempPath = self.context.upgradeBackupPath
            # get file path and check file exists
            filePath = os.path.join(tempPath, Const.INPLACE_UPGRADE_STEP_FILE)
            if not os.path.exists(filePath):
                self.context.logger.debug("The cluster status is Normal. "
                                          "No need to rollback.")
                return Const.BINARY_UPGRADE_NO_NEED_ROLLBACK

            # read and check record format
            stepInfo = g_file.readFile(filePath)[0]
            stepList = stepInfo.split(":")
            if len(stepList) != 2:
                raise Exception(ErrorCode.GAUSS_500["GAUSS_50004"] % filePath)

            recordType = stepList[0].strip()
            recordStep = stepList[1].strip()
            # check upgrade type
            # the record value must be consistent with the upgrade type
            if self.context.action != recordType:
                raise Exception(ErrorCode.GAUSS_500["GAUSS_50004"] % "t" +
                                "Input upgrade type: %s record upgrade type: "
                                "%s\nMaybe you chose the wrong interface." %
                                (self.context.action, recordType))
            # if record value is not digit, exit.
            if not recordStep.isdigit() or int(recordStep) > \
                    Const.BINARY_UPGRADE_STEP_PRE_COMMIT or \
                    int(recordStep) < Const.INVALID_UPRADE_STEP:
                raise Exception(ErrorCode.GAUSS_516["GAUSS_51633"] %
                                recordStep)
        except Exception as e:
            self.context.logger.error(str(e))
            return Const.INVALID_UPRADE_STEP
        self.context.logger.debug("The rollback step is %s" % recordStep)
        return int(recordStep)

    def checkStep(self, step):
        """
        function: check step
        input  : step
        output : NA
        """
        if not step.isdigit() or \
                int(step) > GreyUpgradeStep.STEP_BEGIN_COMMIT or \
                int(step) < Const.INVALID_UPRADE_STEP:
            raise Exception(ErrorCode.GAUSS_516["GAUSS_51633"] % str(step))

    ##########################################################################
    # Offline upgrade functions
    ##########################################################################
    def checkUpgrade(self):
        """
        function: Check the environment for upgrade
        input : action
        output: NA
        """
        self.context.logger.log("Checking upgrade environment.", "addStep")
        try:
            # Check the environment for upgrade
            cmd = "%s -t %s -R '%s' -l '%s' -N '%s' -X '%s'" % \
                  (OMCommand.getLocalScript("Local_Check_Upgrade"),
                   self.context.action,
                   self.context.oldClusterAppPath,
                   self.context.localLog,
                   self.context.newClusterAppPath,
                   self.context.xmlFile)
            self.context.logger.debug("Command for checking upgrade "
                                      "environment: %s." % cmd)
            DefaultValue.execCommandWithMode(cmd,
                                             "check upgrade environment",
                                             self.context.sshTool,
                                             self.context.isSingle,
                                             self.context.mpprcFile)
            self.context.logger.log("Successfully checked upgrade "
                                    "environment.", "constant")
        except Exception as e:
            self.context.logger.log("Failed to check upgrade environment.",
                                    "constant")
            raise Exception(str(e))


    def backupClusterConfig(self):
        """
        function: Backup the cluster config
        input : NA
        output: NA
        """
        # backup list:
        #    cluster_static_config
        #    cluster_dynamic_config
        #    etc/gscgroup_xxx.cfg
        #    lib/postgresql/pg_plugin
        #    server.key.cipher 
        #    server.key.rand
        #    datasource.key.cipher
        #    datasource.key.rand
        #    utilslib
        #    /share/sslsert/ca.key
        #    /share/sslsert/etcdca.crt
        #    catalog physical files
        #    Data Studio lib files
        #    gds files
        #    javaUDF
        #    postGIS
        #    hadoop_odbc_connector extension files
        #    libsimsearch etc files and lib files
        self.context.logger.log("Backing up cluster configuration.", "addStep")
        try:
            # send cmd to all node and exec
            cmd = "%s -t %s -U %s -V %d --upgrade_bak_path=%s -l %s" % \
                  (OMCommand.getLocalScript("Local_Upgrade_Utility"),
                   Const.ACTION_BACKUP_CONFIG,
                   self.context.user,
                   int(float(self.context.oldClusterNumber) * 1000),
                   self.context.upgradeBackupPath,
                   self.context.localLog)
            self.context.logger.debug("Command for backing up cluster "
                                      "configuration: %s" % cmd)
            DefaultValue.execCommandWithMode(cmd,
                                             "backup config files",
                                             self.context.sshTool,
                                             self.context.isSingle,
                                             self.context.mpprcFile)

            # backup hotpatch info file
            self.backupHotpatch()
            # backup version file.
            self.backup_version_file()
        except Exception as e:
            raise Exception(str(e))

        self.context.logger.log("Successfully backed up cluster "
                                "configuration.", "constant")

    def syncNewGUC(self):
        """
        function: sync newly added guc during inplace upgrade.
                  For now, we only sync guc of cm_agent and cm_server
        input : NA
        output: NA
        """
        self.context.logger.debug("Start to sync new guc.", "addStep")
        try:
            # send cmd to all node and exec
            cmd = "%s -t %s -U %s --upgrade_bak_path=%s " \
                  "--new_cluster_app_path=%s -l %s" % \
                  (OMCommand.getLocalScript("Local_Upgrade_Utility"),
                   Const.ACTION_SYNC_CONFIG,
                   self.context.user,
                   self.context.upgradeBackupPath,
                   self.context.newClusterAppPath,
                   self.context.localLog,)
            self.context.logger.debug(
                "Command for synchronizing new guc: %s" % cmd)
            DefaultValue.execCommandWithMode(cmd,
                                             "sync new guc",
                                             self.context.sshTool,
                                             self.context.isSingle,
                                             self.context.mpprcFile)
        except Exception as e:
            self.context.logger.debug("Failed to synchronize new guc.",
                                      "constant")
            raise Exception(str(e))
        self.context.logger.debug("Successfully synchronized new guc.",
                                  "constant")

    def cleanExtensionFiles(self):
        """
        function: clean extension library and config files
        input: NA
        output: 0 / 1
        """
        try:
            # clean extension library and config files
            hadoop_odbc_connector = "%s/lib/postgresql/" \
                                    "hadoop_odbc_connector.so" % \
                                    self.context.oldClusterInfo.appPath
            extension_config01 = "%s/share/postgresql/extension/" \
                                 "hadoop_odbc_connector--1.0.sql" % \
                                 self.context.oldClusterInfo.appPath
            extension_config02 = "%s/share/postgresql/extension/" \
                                 "hadoop_odbc_connector.control" % \
                                 self.context.oldClusterInfo.appPath
            extension_config03 = "%s/share/postgresql/extension/hadoop_odbc_" \
                                 "connector--unpackaged--1.0.sql" % \
                                 self.context.oldClusterInfo.appPath

            cmd = "(if [ -f '%s' ];then rm -f '%s';fi)" % \
                  (hadoop_odbc_connector, hadoop_odbc_connector)
            cmd += " && (if [ -f '%s' ];then rm -f '%s';fi)" % \
                   (extension_config01, extension_config01)
            cmd += " && (if [ -f '%s' ];then rm -f '%s';fi)" % \
                   (extension_config02, extension_config02)
            cmd += " && (if [ -f '%s' ];then rm -f '%s';fi)" % \
                   (extension_config03, extension_config03)
            self.context.logger.debug("Command for cleaning extension "
                                      "library and config files: %s" % cmd)
            DefaultValue.execCommandWithMode(
                cmd, "clean extension library and config files",
                self.context.sshTool, self.context.isSingle,
                self.context.mpprcFile)
            self.context.logger.debug("Command for cleaning extension "
                                      "library and config files: %s" % cmd)
            return 0
        except Exception as e:
            self.context.logger.debug("Fail to clean extension library and "
                                      "config files.output:%s" % str(e))
            return 1

    def waitClusterForNormal(self, waitTimeOut=300):
        """
        function: Wait the node become Normal
        input : waitTimeOut
        output: NA
        """
        self.context.logger.log("Waiting for the cluster status to "
                                "become normal.")
        dotCount = 0
        # get the end time
        endTime = datetime.now() + timedelta(seconds=int(waitTimeOut))
        while True:
            time.sleep(5)
            sys.stdout.write(".")
            dotCount += 1
            if dotCount >= 12:
                dotCount = 0
                sys.stdout.write("\n")

            (checkStatus, checkResult) = \
                OMCommand.doCheckStaus(self.context.user, 0)
            if checkStatus == 0:
                if dotCount != 0:
                    sys.stdout.write("\n")
                self.context.logger.log("The cluster status is normal.")
                break

            if datetime.now() >= endTime:
                if dotCount != 0:
                    sys.stdout.write("\n")
                self.context.logger.debug(checkResult)
                raise Exception("Timeout." + "\n" +
                                ErrorCode.GAUSS_516["GAUSS_51602"])

        if checkStatus != 0:
            self.context.logger.debug(checkResult)
            raise Exception(ErrorCode.GAUSS_516["GAUSS_51607"] % "cluster")

    def getLcgroupnameList(self, jsonFile):
        """
        function: get Lc group name list
        input: jsonFile
        output: []
        """
        para = {}
        lcgroupnamelist = []
        try:
            with open(jsonFile, "r") as fp_json:
                para = json.load(fp_json)
        except Exception as e:
            raise Exception(str(e))
        if (para):
            lcgroupnamelist = para['lcgroupnamelist']
            while '' in lcgroupnamelist:
                lcgroupnamelist.remove('')
        return lcgroupnamelist

    def restoreClusterConfig(self, isRollBack=False):
        """
        function: Restore the cluster config
        input : isRollBack
        output: NA
        """
        # restore list:
        #    cluster_dynamic_config
        #    etc/gscgroup_xxx.cfg
        #    lib/postgresql/pg_plugin
        #    server.key.cipher 
        #    server.key.rand
        #    datasource.key.cipher
        #    datasource.key.rand
        #    utilslib
        #    /share/sslsert/ca.key
        #    /share/sslsert/etcdca.crt
        #    Data Studio lib files
        #    gds files
        #    javaUDF
        #    postGIS
        #    hadoop_odbc_connector extension files
        #    libsimsearch etc files and lib files
        if isRollBack:
            self.context.logger.log("Restoring cluster configuration.")
        else:
            self.context.logger.log("Restoring cluster configuration.",
                                    "addStep")
        try:
            if isRollBack:
                self.rollbackHotpatch()
            else:
                # restore static configuration
                cmd = "%s -t %s -U %s -V %d --upgrade_bak_path=%s " \
                      "--new_cluster_app_path=%s -l %s" % \
                      (OMCommand.getLocalScript("Local_Upgrade_Utility"),
                       Const.ACTION_RESTORE_CONFIG,
                       self.context.user,
                       int(float(self.context.oldClusterNumber) * 1000),
                       self.context.upgradeBackupPath,
                       self.context.newClusterAppPath,
                       self.context.localLog)

                self.context.logger.debug("Command for restoring "
                                          "config files: %s" % cmd)
                DefaultValue.execCommandWithMode(cmd,
                                                 "restore config files",
                                                 self.context.sshTool,
                                                 self.context.isSingle,
                                                 self.context.mpprcFile)
                # change the owner of application
                cmd = "chown -R %s:%s '%s'" % \
                      (self.context.user, self.context.group,
                       self.context.newClusterAppPath)
                DefaultValue.execCommandWithMode(
                    cmd, "change the owner of application",
                    self.context.sshTool, self.context.isSingle,
                    self.context.mpprcFile)
        except Exception as e:
            raise Exception(str(e))
        if isRollBack:
            self.context.logger.log("Successfully restored "
                                    "cluster configuration.")
        else:
            self.context.logger.log("Successfully restored cluster "
                                    "configuration.", "constant")

    def checkStaticConfig(self):
        """
        function: Check if static config file exists in bin dir,
                  if not exists, restore it from backup dir
        input : NA
        output: NA
        """
        self.context.logger.log("Checking static configuration files.")
        try:
            # check static configuration path
            staticConfigPath = "%s/bin" % self.context.oldClusterAppPath
            # restore static configuration
            cmd = "(if [ ! -f '%s/cluster_static_config' ];then cp " \
                  "%s/cluster_static_config %s/bin;fi)" % \
                  (staticConfigPath, self.context.upgradeBackupPath,
                   self.context.oldClusterAppPath)
            DefaultValue.execCommandWithMode(cmd,
                                             "restore static configuration",
                                             self.context.sshTool,
                                             self.context.isSingle,
                                             self.context.mpprcFile)
        except Exception as e:
            raise Exception(str(e))
        self.context.logger.log("Successfully checked static "
                                "configuration files.")

    def backupNodeVersion(self):
        """
        function: Backup current application and configuration.
                  The function only be used by binary upgrade.
                  To ensure the transaction atomicity,
                  it will be used with checkUpgrade().
        input : NA
        output: NA
        """
        self.context.logger.log("Backing up current application "
                                "and configurations.", "addStep")
        try:
            # back up environment variables
            cmd = "cp '%s' '%s'_gauss" % (self.context.userProfile,
                                          self.context.userProfile)
            self.context.logger.debug(
                "Command for backing up environment file: %s" % cmd)
            DefaultValue.execCommandWithMode(cmd,
                                             "back up environment variables",
                                             self.context.sshTool,
                                             self.context.isSingle,
                                             self.context.mpprcFile)

            # back up application and configuration
            cmd = "%s -U %s -P %s -p -b -l %s" % \
                  (OMCommand.getLocalScript("Local_Backup"), self.context.user,
                   self.context.upgradeBackupPath, self.context.localLog)
            self.context.logger.debug(
                "Command for backing up application: %s" % cmd)
            DefaultValue.execCommandWithMode(
                cmd, "back up application and configuration",
                self.context.sshTool, self.context.isSingle,
                self.context.mpprcFile)

        except Exception as e:
            # delete binary backup directory
            delCmd = g_file.SHELL_CMD_DICT["deleteDir"] % \
                     (self.context.tmpDir, os.path.join(self.context.tmpDir,
                                                        'backupTemp_*'))
            DefaultValue.execCommandWithMode(delCmd,
                                             "delete binary backup directory",
                                             self.context.sshTool,
                                             self.context.isSingle,
                                             self.context.mpprcFile)
            raise Exception(str(e))

        self.context.logger.log("Successfully backed up current "
                                "application and configurations.", "constant")

    def restoreNodeVersion(self):
        """
        function: Restore the application and configuration
                  1. restore old version
                  2. restore environment variables
        input : NA
        output: NA
        """
        self.context.logger.log("Restoring application and configurations.")

        try:
            # restore old version
            cmd = "%s -U %s -P %s -p -b -l %s" % \
                  (OMCommand.getLocalScript("Local_Restore"),
                   self.context.user, self.context.upgradeBackupPath,
                   self.context.localLog)
            self.context.logger.debug("Command for restoring "
                                      "old version: %s" % cmd)
            DefaultValue.execCommandWithMode(cmd,
                                             "restore old version",
                                             self.context.sshTool,
                                             self.context.isSingle,
                                             self.context.mpprcFile)

            # restore environment variables
            cmd = "(if [ -f '%s'_gauss ];then mv '%s'_gauss '%s';fi)" % \
                  (self.context.userProfile, self.context.userProfile,
                   self.context.userProfile)
            self.context.logger.debug("Command for restoring environment file:"
                                      " %s" % cmd)
            DefaultValue.execCommandWithMode(cmd,
                                             "restore environment variables",
                                             self.context.sshTool,
                                             self.context.isSingle,
                                             self.context.mpprcFile)
        except Exception as e:
            raise Exception(str(e))

        self.context.logger.log("Successfully restored application and "
                                "configuration.")

    def modifySocketDir(self):
        """
        function: modify unix socket directory
        input : NA
        output: NA
        """
        self.context.logger.log("Modifying the socket path.", "addStep")
        try:
            # modifying the socket path for all CN/DN instance
            (status, output) = self.setGUCValue(
                "unix_socket_directory",
                DefaultValue.getTmpDirAppendMppdb(self.context.user), "set")
            if (status != 0):
                raise Exception(ErrorCode.GAUSS_500["GAUSS_50007"] % "GUC" +
                                " Error: \n%s" % str(output))

            userProfile = DefaultValue.getMpprcFile()
        except Exception as e:
            raise Exception(str(e))

        self.context.logger.log("Successfully modified socket path.",
                                "constant")

    ###########################################################################
    # Rollback upgrade functions
    ###########################################################################
    def cleanBackupFiles(self):
        """
        function: Clean backup files.
        input : action
        output : NA
        """
        try:
            # clean backup files
            cmd = "(if [ -f '%s/OldDbClusterInfo.py' ]; then rm -f " \
                  "'%s/OldDbClusterInfo.py'; fi) &&" % \
                   (self.context.tmpDir, self.context.tmpDir)
            cmd += "(if [ -f '%s/OldDbClusterInfo.pyc' ]; then rm -f " \
                   "'%s/OldDbClusterInfo.pyc'; fi) &&" % \
                   (self.context.tmpDir, self.context.tmpDir)
            cmd += "(if [ -d '%s/script' ]; then rm -rf '%s/script'; " \
                   "fi) &&" %  (self.context.tmpDir, self.context.tmpDir)
            cmd += "(if [ -f '%s/oldclusterinfo' ]; then rm -f " \
                   "'%s/oldclusterinfo'; fi) &&" % \
                   (self.context.tmpDir, self.context.tmpDir)
            cmd += "(if [ -f '%s/oldclusterGUC' ]; then rm -f " \
                   "'%s/oldclusterGUC'; fi) &&" % \
                   (self.context.tmpDir, self.context.tmpDir)
            cmd += "(if [ -f '%s/cluster_static_config' ]; then rm -f " \
                   "'%s/cluster_static_config'; fi) &&" % \
                   (self.context.tmpDir, self.context.tmpDir)
            cmd += "(if [ -f '%s/c_functionfilelist.dat' ]; then rm -f " \
                   "'%s/c_functionfilelist.dat'; fi) &&" % \
                   (self.context.tmpDir, self.context.tmpDir)
            cmd += "(if [ -f '%s'_gauss ]; then rm -f '%s'_gauss ; fi) &&" % \
                   (self.context.userProfile, self.context.userProfile)
            cmd += "(if [ -f '%s/oldclusterinfo.json' ]; then rm -f " \
                   "'%s/oldclusterinfo.json'; fi) &&" % \
                   (self.context.tmpDir, self.context.tmpDir)
            cmd += "(if [ -f '%s/%s' ]; then rm -f '%s/%s'; fi) &&" % \
                   (self.context.tmpDir, Const.CLUSTER_CNSCONF_FILE,
                    self.context.tmpDir, Const.CLUSTER_CNSCONF_FILE)
            cmd += "(rm -f '%s'/gauss_crontab_file_*) &&" % self.context.tmpDir
            cmd += "(if [ -d '%s' ]; then rm -rf '%s'; fi) " % \
                  (self.context.upgradeBackupPath,
                   self.context.upgradeBackupPath)
            self.context.logger.debug("Command for clean "
                                      "backup files: %s" % cmd)
            DefaultValue.execCommandWithMode(cmd,
                                             "clean backup files",
                                             self.context.sshTool,
                                             self.context.isSingle,
                                             self.context.mpprcFile)

        except Exception as e:
            raise Exception(str(e))

    def cleanBinaryUpgradeBakFiles(self, isRollBack=False):
        """
        function: Clean back up files, include cluster_static_config,
                  cluster_dynamic_config, binary.tar, parameter.tar.
        input : isRollBack
        output: NA
        """
        if (isRollBack):
            self.context.logger.debug("Cleaning backup files.")
        else:
            self.context.logger.debug("Cleaning backup files.", "addStep")

        try:
            # clean backup files
            self.cleanBackupFiles()
        except Exception as e:
            raise Exception(str(e))
        if (isRollBack):
            self.context.logger.debug("Successfully cleaned backup files.")
        else:
            self.context.logger.debug("Successfully cleaned backup files.",
                                      "constant")

    ###########################################################################
    # Rollback upgrade functions
    ###########################################################################

    def doHealthCheck(self, checkPosition):
        """
        function: Do health check, if healthy, return 0, else return 1
        input : checkPosition
        output: 0  successfully
                1  failed
        """
        #######################################################################
        # When do binary-upgrade:
        #       Const.OPTION_PRECHECK        -> cluster Normal
        #                              -> database can connec
        #       Const.OPTION_POSTCHECK       -> cluster Normal
        #                              -> package version Normal
        #                              -> database can connec
        #######################################################################
        self.context.logger.log("Start to do health check.", "addStep")

        status = 0
        output = ""

        if (checkPosition == Const.OPTION_PRECHECK):
            if (self.checkClusterStatus(checkPosition, True) != 0):
                output += "\n    Cluster status does not match condition."
            if (self.checkConnection() != 0):
                output += "\n    Database could not be connected."
        elif (checkPosition == Const.OPTION_POSTCHECK):
            if (self.checkClusterStatus(checkPosition) != 0):
                output += "\n    Cluster status is Abnormal."
            if not self.checkVersion(
                    self.context.newClusterVersion,
                    self.context.clusterInfo.getClusterNodeNames()):
                output += "\n    The gaussdb version is inconsistent."
            if (self.checkConnection() != 0):
                output += "\n    Database could not be connected."
        else:
            # Invalid check position
            output += "\n    Invalid check position."
        if (output != ""):
            status = 1
        # all check has been pass, return 0
        self.context.logger.log("Successfully checked cluster status.",
                                "constant")
        return (status, output)

    def checkVersion(self, checkinfo, checknodes):
        """
        function: Check if the node have been upgraded, if gaussdb bin
                  file verison is same on all host, return 0, else retrun 1
        input : checkinfo, checknodes
        output: 0  successfully
                1  failed
        """
        self.context.logger.debug(
            "Start to check gaussdb version consistency.")
        if self.context.isSingle:
            self.context.logger.debug("There is single cluster,"
                                      " no need to check it.")
            return True

        try:
            # checking gaussdb bin file version VxxxRxxxCxx or commitid
            cmd = "source %s;%s -t %s -v %s -U %s -l %s" % \
                  (self.context.userProfile,
                   OMCommand.getLocalScript("Local_Check_Upgrade"),
                   Const.ACTION_CHECK_VERSION,
                   checkinfo,
                   self.context.user,
                   self.context.localLog)
            self.context.logger.debug("Command for checking gaussdb version "
                                      "consistency: %s." % cmd)
            (status, output) = \
                self.context.sshTool.getSshStatusOutput(cmd, checknodes)
            for node in status.keys():
                failFlag = "Failed to check version information"
                if status[node] != DefaultValue.SUCCESS or \
                        output.find(failFlag) >= 0:
                    raise Exception(ErrorCode.GAUSS_529["GAUSS_52929"] +
                                    "Error: \n%s" % str(output))
            # gaussdb bin file version is same on all host, return 0
            self.context.logger.debug("Successfully checked gaussdb"
                                      " version consistency.")
            return True
        except Exception as e:
            self.context.logger.debug(str(e))
            return False

    def checkClusterStatus(self, checkPosition=Const.OPTION_PRECHECK,
                           doDetailCheck=False):
        """
        function: Check cluster status, if NORMAL, return 0, else return 1
                  For grey upgrade, if have switched to new bin, we will remove
                  abnormal nodes and then return 0, else return 1
        input : checkPosition, doDetailCheck
        output: 0  successfully
                1  failed
        """
        self.context.logger.debug("Start to check cluster status.")
        # build query cmd
        # according to the implementation of the results to determine whether
        # the implementation of success
        cmd = "source %s;gs_om -t query" % self.context.userProfile
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            self.context.logger.debug(
                "Failed to execute command %s.\nStatus:%s\nOutput:%s" %
                (cmd, status, output))
            return 1
        self.context.logger.debug(
            "Successfully obtained cluster status information. "
            "Cluster status information:\n%s" % output)
        if output.find("Normal") < 0:
            self.context.logger.debug("The cluster_state is Abnormal.")
            if checkPosition == Const.OPTION_POSTCHECK:
                if output.find("Degraded") < 0:
                    self.context.logger.debug("The cluster_state is not "
                                              "Degraded under postcheck.")
                    return 1
            else:
                return 1

        # do more check if required
        if doDetailCheck:
            cluster_state_check = False
            redistributing_check = False
            for line in output.split('\n'):
                if len(line.split(":")) != 2:
                    continue
                (key, value) = line.split(":")
                if key.strip() == "cluster_state" and \
                        value.strip() == "Normal":
                    cluster_state_check = True
                elif key.strip() == "redistributing" and value.strip() == "No":
                    redistributing_check = True
            if cluster_state_check and redistributing_check:
                self.context.logger.debug("Cluster_state must be Normal, "
                                          "redistributing must be No.")
                return 0
            else:
                self.context.logger.debug(
                    "Cluster status information does not meet the upgrade "
                    "condition constraints. When upgrading, cluster_state must"
                    " be Normal, redistributing must be No and balanced"
                    " must be Yes.")
                return 1

        # cluster is NORMAL, return 0
        return 0

    def waitClusterNormalDegrade(self, waitTimeOut=300):
        """
        function: Check if cluster status is Normal for each main step of
                  online upgrade
        input : waitTimeOut, default is 60.
        output : NA
        """
        # get the end time
        self.context.logger.log("Wait for the cluster status normal "
                                "or degrade.")
        endTime = datetime.now() + timedelta(seconds=int(waitTimeOut))
        while True:
            cmd = "source %s;cm_ctl query" % self.context.userProfile
            (status, output) = subprocess.getstatusoutput(cmd)
            if status == 0 and (output.find("Normal") >= 0 or
                                output.find("Degraded") >= 0):
                self.context.logger.debug(
                    "The cluster status is normal or degrade now.")
                break

            if datetime.now() >= endTime:
                self.context.logger.debug("The cmd is %s " % cmd)
                raise Exception("Timeout." + "\n" +
                                ErrorCode.GAUSS_516["GAUSS_51602"])
            else:
                self.context.logger.debug(
                    "Cluster status has not reach normal. Wait for another 3"
                    " seconds.\n%s" % output)
                time.sleep(3)  # sleep 3 seconds

    def checkConnection(self):
        """
        function: Check if cluster accept connecitons,
                  upder inplace upgrade, all DB should be connected
                  under grey upgrade, makesure all CN in nodes that does not
                  under upgrade process or extracted abnormal nodes can be
                  connected if accpet connection, return 0, else return 1
                  1. find a cn instance
                  2. connect this cn and exec sql cmd
        input : NA
        output: 0  successfully
                1  failed
        """
        self.context.logger.debug("Start to check database connection.")
        for dbNode in self.context.clusterInfo.dbNodes:
            if len(dbNode.datanodes) == 0 or dbNode.name:
                continue
            for dnInst in dbNode.datanodes:
                # connect this DB and exec sql cmd
                sql = "SELECT 1;"
                (status, output) = \
                    ClusterCommand.remoteSQLCommand(
                        sql, self.context.user, dnInst.hostname, dnInst.port,
                        False, DefaultValue.DEFAULT_DB_NAME,
                        IsInplaceUpgrade=True)
                if status != 0 or not output.isdigit():
                    self.context.logger.debug(
                        "Failed to execute SQL on [%s]: %s. Error: \n%s" %
                        (dnInst.hostname, sql, str(output)))
                    return 1

        # all DB accept connection, return 0
        self.context.logger.debug("Successfully checked database connection.")
        return 0

    def createBakPath(self):
        """
        function: create bak path
        input  : NA
        output : NA
        """
        cmd = "(if [ ! -d '%s' ]; then mkdir -p '%s'; fi)" % \
              (self.context.upgradeBackupPath, self.context.upgradeBackupPath)
        cmd += " && (chmod %d -R %s)" % (DefaultValue.KEY_DIRECTORY_MODE,
                                         self.context.upgradeBackupPath)
        self.context.logger.debug("Command for creating directory: %s" % cmd)
        DefaultValue.execCommandWithMode(cmd,
                                         "create binary_upgrade path",
                                         self.context.sshTool,
                                         self.context.isSingle,
                                         self.context.mpprcFile)

    def recordDirFile(self):
        """
        function: record dir file
        input: NA
        output: NA
        """
        self.context.logger.debug("Create the file to record "
                                  "old and new app directory.")
        # write the old cluster number and new cluster number into backup dir
        appDirRecord = os.path.join(self.context.upgradeBackupPath,
                                    Const.RECORD_UPGRADE_DIR)
        g_file.createFile(appDirRecord, True, DefaultValue.KEY_FILE_MODE)
        g_file.writeFile(appDirRecord, [self.context.oldClusterAppPath,
                                        self.context.newClusterAppPath], 'w')
        self.distributeFile(appDirRecord)
        self.context.logger.debug("Successfully created the file to "
                                  "record old and new app directory.")

    def copyBakVersion(self):
        """
        under commit, if we have cleaned old install path, then node disabled,
        we cannot get old version,
        under choseStrategy, we will not pass the check
        :return:NA
        """
        versionFile = os.path.join(self.context.oldClusterAppPath,
                                   "bin/upgrade_version")
        bakVersionFile = os.path.join(self.context.upgradeBackupPath,
                                      "old_upgrade_version")
        cmd = "(if [ -f '%s' ]; then cp -f -p '%s' '%s';fi)" % \
              (versionFile, versionFile, bakVersionFile)
        cmd += " && (chmod %d %s)" % \
               (DefaultValue.KEY_FILE_MODE, bakVersionFile)
        DefaultValue.execCommandWithMode(cmd,
                                         "copy upgrade_version file",
                                         self.context.sshTool,
                                         self.context.isSingle,
                                         self.context.mpprcFile)

    def cleanInstallPath(self, cleanNew=Const.NEW):
        """
        function: after grey upgrade succeed, clean old install path
        input : cleanNew
        output: NA
        """
        self.context.logger.debug("Cleaning %s install path." % cleanNew,
                                  "addStep")
        # clean old install path
        if cleanNew == Const.NEW:
            installPath = self.context.newClusterAppPath
        elif cleanNew == Const.OLD:
            installPath = self.context.oldClusterAppPath
        else:
            raise Exception(ErrorCode.GAUSS_529["GAUSS_52937"])

        cmd = "%s -t %s -U %s -R %s -l %s" % \
              (OMCommand.getLocalScript("Local_Upgrade_Utility"),
               Const.ACTION_CLEAN_INSTALL_PATH,
               self.context.user,
               installPath,
               self.context.localLog)
        if self.context.forceRollback:
            cmd += " --force"
        self.context.logger.debug("Command for clean %s install path: %s" %
                                  (cleanNew, cmd))
        DefaultValue.execCommandWithMode(cmd,
                                         "clean %s install path" % cleanNew,
                                         self.context.sshTool,
                                         self.context.isSingle,
                                         self.context.mpprcFile)
        self.context.logger.log("Successfully cleaned %s install path." %
                                cleanNew, "constant")

    def installNewBin(self):
        """
        function: install new binary in a new directory
                  1. get env GAUSSLOG
                  2. get env PGHOST
                  3. install new bin file
                  4. sync old config to new bin path
                  5. update env
        input: none
        output: none
        """
        try:
            self.context.logger.log("Installing new binary.", "addStep")

            # install new bin file
            cmd = "%s -t 'install_cluster' -U %s:%s -R '%s' -P %s -c %s" \
                  " -l '%s' -X '%s' -T -u" % \
                  (OMCommand.getLocalScript("Local_Install"),
                   self.context.user,
                   self.context.group,
                   self.context.newClusterAppPath,
                   self.context.tmpDir,
                   self.context.clusterInfo.name,
                   self.context.localLog,
                   self.context.xmlFile)
            self.context.logger.debug(
                "Command for installing new binary: %s." % cmd)
            DefaultValue.execCommandWithMode(cmd,
                                             "install new application",
                                             self.context.sshTool,
                                             self.context.isSingle,
                                             self.context.mpprcFile)
            self.context.logger.debug(
                "Successfully installed new binary files.")
        except Exception as e:
            self.context.logger.debug("Failed to install new binary files.")
            raise Exception(str(e))

    def backupHotpatch(self):
        """
        function: backup hotpatch config file patch.info in xxx/data/hotpatch
        input : NA
        output: NA
        """
        self.context.logger.debug("Start to backup hotpatch.")
        try:
            cmd = "%s -t %s -U %s --upgrade_bak_path=%s " \
                  "--new_cluster_app_path=%s -l %s" % \
                  (OMCommand.getLocalScript("Local_Upgrade_Utility"),
                   Const.ACTION_BACKUP_HOTPATCH,
                   self.context.user,
                   self.context.upgradeBackupPath,
                   self.context.newClusterAppPath,
                   self.context.localLog)
            DefaultValue.execCommandWithMode(cmd,
                                             "backup hotpatch files",
                                             self.context.sshTool,
                                             self.context.isSingle,
                                             self.context.mpprcFile)
        except Exception as e:
            raise Exception(" Failed to backup hotpatch config file." + str(e))
        self.context.logger.log("Successfully backup hotpatch config file.")

    def rollbackHotpatch(self):
        """
        function: backup hotpatch config file patch.info in xxx/data/hotpatch
        input : NA
        output: NA
        """
        self.context.logger.debug("Start to rollback hotpatch.")
        try:
            cmd = "%s -t %s -U %s --upgrade_bak_path=%s -l %s -X '%s'" % \
                  (OMCommand.getLocalScript("Local_Upgrade_Utility"),
                   Const.ACTION_ROLLBACK_HOTPATCH,
                   self.context.user,
                   self.context.upgradeBackupPath,
                   self.context.localLog,
                   self.context.xmlFile)
            if self.context.forceRollback:
                cmd += " --force"
            DefaultValue.execCommandWithMode(cmd,
                                             "rollback hotpatch",
                                             self.context.sshTool,
                                             self.context.isSingle,
                                             self.context.mpprcFile)
        except Exception as e:
            raise Exception(" Failed to rollback hotpatch config file."
                            + str(e))
        self.context.logger.log("Successfully rollback hotpatch config file.")

    def backup_version_file(self):
        """
        Backup the old version file.
        """
        oldVersionFile = "%s/bin/%s" % \
                         (self.context.oldClusterAppPath,
                          DefaultValue.DEFAULT_DISABLED_FEATURE_FILE_NAME)
        oldLicenseFile = "%s/bin/%s" % (self.context.oldClusterAppPath,
                                        DefaultValue.DEFAULT_LICENSE_FILE_NAME)

        cmd = "(if [ -d %s ] && [ -f %s ]; then cp -f %s %s; fi) && " % \
              (self.context.upgradeBackupPath, oldVersionFile, oldVersionFile,
               self.context.upgradeBackupPath)
        cmd += "(if [ -d %s ] && [ -f %s ]; then cp -f %s %s; fi)" % \
               (self.context.upgradeBackupPath, oldLicenseFile, oldLicenseFile,
                self.context.upgradeBackupPath)

        self.context.logger.debug(
            "Execute command to backup the product version file and the "
            "license control file: %s" % cmd)
        DefaultValue.execCommandWithMode(cmd,
                                         "Backup old gaussdb.version file.",
                                         self.context.sshTool,
                                         self.context.isSingle,
                                         self.context.mpprcFile)

    def getTimeFormat(self, seconds):
        """
        format secends to h-m-s
        input:int
        output:int
        """
        seconds = int(seconds)
        if seconds == 0:
            return 0
        # Converts the seconds to standard time
        hour = seconds / 3600
        minute = (seconds - hour * 3600) / 60
        s = seconds % 60
        resultstr = ""
        if hour != 0:
            resultstr += "%dh" % hour
        if minute != 0:
            resultstr += "%dm" % minute
        return "%s%ds" % (resultstr, s)

    def CopyCerts(self):
        """
        function: copy certs
        input  : NA
        output : NA
        """
        self.context.logger.log("copy certs from %s to %s." % (
            self.context.oldClusterAppPath, self.context.newClusterAppPath))
        try:
            cmd = "%s -t %s -U %s --old_cluster_app_path=%s " \
                  "--new_cluster_app_path=%s -l %s" % \
                  (OMCommand.getLocalScript("Local_Upgrade_Utility"),
                   Const.ACTION_COPY_CERTS,
                   self.context.user,
                   self.context.oldClusterAppPath,
                   self.context.newClusterAppPath,
                   self.context.localLog)
            self.context.logger.debug("Command for copy certs: '%s'." % cmd)
            DefaultValue.execCommandWithMode(cmd,
                                             "Command for copy certs",
                                             self.context.sshTool,
                                             self.context.isSingle,
                                             self.context.mpprcFile)

        except Exception as e:
            self.context.logger.log("Failed to copy certs from %s to %s." %
                                    (self.context.oldClusterAppPath,
                                     self.context.newClusterAppPath))
            raise Exception(str(e))
        time.sleep(10)
        self.context.logger.log("Successfully copy certs from %s to %s." %
                                (self.context.oldClusterAppPath,
                                 self.context.newClusterAppPath),
                                "constant")

    def switchBin(self, switchTo=Const.OLD):
        """
        function: switch bin
        input  : switchTo
        output : NA
        """
        self.context.logger.log("Switch symbolic link to %s binary directory."
                                % switchTo, "addStep")
        try:
            cmd = "%s -t %s -U %s -l %s" % \
                  (OMCommand.getLocalScript("Local_Upgrade_Utility"),
                   Const.ACTION_SWITCH_BIN,
                   self.context.user,
                   self.context.localLog)
            if switchTo == Const.NEW:
                cmd += " -R '%s'" % self.context.newClusterAppPath
            else:
                cmd += " -R '%s'" % self.context.oldClusterAppPath
            if self.context.forceRollback:
                cmd += " --force"
            self.context.logger.debug("Command for switching binary directory:"
                                      " '%s'." % cmd)
            DefaultValue.execCommandWithMode(cmd,
                                             "Switch the binary directory",
                                             self.context.sshTool,
                                             self.context.isSingle,
                                             self.context.mpprcFile)

        except Exception as e:
            self.context.logger.log("Failed to switch symbolic link to %s "
                                    "binary directory." % switchTo)
            raise Exception(str(e))
        time.sleep(10)
        self.context.logger.log("Successfully switch symbolic link to %s "
                                "binary directory." % switchTo, "constant")

    def clearOtherToolPackage(self, action=""):
        """
        function: clear other tool package
        input  : action
        output : NA
        """
        if action == Const.ACTION_AUTO_ROLLBACK:
            self.context.logger.debug("clean other tool package files.")
        else:
            self.context.logger.debug(
                "clean other tool package files.", "addStep")
        try:
            commonPart = DefaultValue.get_package_back_name().rsplit("_", 1)[0]
            gphomePath = os.listdir(DefaultValue.getClusterToolPath())
            commitId = self.newCommitId
            if action == Const.ACTION_AUTO_ROLLBACK:
                commitId = self.oldCommitId
            for filePath in gphomePath:
                if commonPart in filePath and commitId not in filePath:
                    toDeleteFilePath = os.path.join(
                        DefaultValue.getClusterToolPath(), filePath)
                    deleteCmd = "(if [ -f '%s' ]; then rm -rf '%s'; fi) " % \
                                  (toDeleteFilePath, toDeleteFilePath)
                    DefaultValue.execCommandWithMode(
                        deleteCmd,
                        "clean tool package files",
                        self.context.sshTool,
                        self.context.isSingle,
                        self.context.mpprcFile)
        except Exception as e:
            self.context.logger.log(
                "Failed to clean other tool package files.")
            raise Exception(str(e))
        if action == Const.ACTION_AUTO_ROLLBACK:
            self.context.logger.debug(
                "Success to clean other tool package files.")
        else:
            self.context.logger.debug(
                "Success to clean other tool package files.", "constant")

    def createGphomePack(self):
        """
        function: create Gphome pack
        input  : NA
        output : NA
        """
        try:
            cmd = "(if [ ! -d '%s' ]; then mkdir -p '%s'; fi)" % \
                  (DefaultValue.getClusterToolPath(),
                   DefaultValue.getClusterToolPath())
            cmd += " && (chmod %d -R %s)" % \
                   (DefaultValue.KEY_DIRECTORY_MODE,
                    DefaultValue.getClusterToolPath())
            self.context.logger.debug(
                "Command for creating directory: %s" % cmd)
            DefaultValue.execCommandWithMode(cmd,
                                             "create gphome path",
                                             self.context.sshTool,
                                             self.context.isSingle,
                                             self.context.mpprcFile)
            oldPackName = "%s-Package-bak_%s.tar.gz" % \
                          (VersionInfo.PRODUCT_NAME_PACKAGE, self.oldCommitId)
            packFilePath = "%s/%s" % (DefaultValue.getClusterToolPath(),
                                      oldPackName)
            copyNode = ""
            cmd = "if [ -f '%s' ]; then echo 'GetFile'; " \
                  "else echo 'NoThisFile'; fi" % packFilePath
            self.context.logger.debug("Command for checking file: %s" % cmd)
            (status, output) = self.context.sshTool.getSshStatusOutput(
                cmd, self.context.clusterNodes, self.context.mpprcFile)
            outputMap = self.context.sshTool.parseSshOutput(
                self.context.clusterNodes)
            self.context.logger.debug("Output: %s" % output)
            for node in self.context.clusterNodes:
                if status[node] == DefaultValue.SUCCESS:
                    if 'GetFile' in outputMap[node]:
                        copyNode = node
                        break
            if copyNode:
                self.context.logger.debug("Copy the file %s from node %s." %
                                          (packFilePath, copyNode))
                for node in self.context.clusterNodes:
                    if status[node] == DefaultValue.SUCCESS:
                        if 'NoThisFile' in outputMap[node]:
                            cmd = g_Platform.getRemoteCopyCmd(
                                packFilePath,
                                DefaultValue.getClusterToolPath(),
                                str(copyNode), False, 'directory', node)
                            self.context.logger.debug(
                                "Command for copying directory: %s" % cmd)
                            DefaultValue.execCommandLocally(cmd)
            else:
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50210"] %
                                packFilePath)
        except Exception as e:
            raise Exception(str(e))
