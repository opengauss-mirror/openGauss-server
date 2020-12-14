# -*- coding:utf-8 -*-
#############################################################################
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
# Description  : gs_install is a utility to deploy a Gauss200 server.
#############################################################################
import subprocess
import os
import sys

sys.path.append(sys.path[0] + "/../../../")
from gspylib.common.Common import DefaultValue, ClusterCommand
from gspylib.common.OMCommand import OMCommand
from gspylib.common.ErrorCode import ErrorCode
from gspylib.common.VersionInfo import VersionInfo
from gspylib.os.gsfile import g_file
from impl.install.InstallImpl import InstallImpl

ROLLBACK_FAILED = 3


class InstallImplOLAP(InstallImpl):
    """
    The class is used to do perform installation
    """
    """
    init the command options
    save command line parameter values
    """

    def __init__(self, install):
        """
        function: constructor
        """
        super(InstallImplOLAP, self).__init__(install)

    def checkTimeout(self):
        """
        function: check timeout
        input: NA
        output: NA
        """
        if (self.context.time_out is None):
            # if --time-out is null
            self.context.time_out = DefaultValue.TIMEOUT_CLUSTER_START
        else:
            if (not str(self.context.time_out).isdigit()):
                # --time-out is not a digit
                raise Exception(ErrorCode.GAUSS_500["GAUSS_50003"] % (
                    "-time-out", "a nonnegative integer"))
            self.context.time_out = int(self.context.time_out)
            if (
                    self.context.time_out <= 0
                    or self.context.time_out >= 2147483647):
                # --time-out is not a int
                raise Exception(
                    ErrorCode.GAUSS_500["GAUSS_50004"] % "-time-out")

    def deleteTempFileForUninstall(self):
        """
        function: Rollback install ,delete temporary file
        input : NA
        output: NA
        """
        # Deleting temporary file
        self.context.logger.debug("Deleting temporary file.")
        tmpFile = "/tmp/temp.%s" % self.context.user
        cmd = g_file.SHELL_CMD_DICT["deleteFile"] % (tmpFile, tmpFile)
        DefaultValue.execCommandWithMode(cmd,
                                         "delete temporary file",
                                         self.context.sshTool,
                                         self.context.isSingle)
        self.context.logger.debug("Successfully deleted temporary file.")

    def prepareInstallCluster(self):
        """
         function: prepared install cluster
                 AP: distribute package
                 and Check installation environment on all nodes
                 TP: skip
        """
        if (not self.context.dws_mode and not self.context.isSingle):
            # distribute package to every host
            self.context.distributeFiles()
        self.checkNodeInstall()

    def getCommandOptions(self):
        """
        function: get command options
        input: NA
        output: NA
        """
        opts = ""
        if self.context.alarm_component != "":
            opts += " --alarm=%s " % self.context.alarm_component
        if self.context.time_out is not None:
            opts += " --time_out=%d " % self.context.time_out
        return opts

    def prepareConfigCluster(self):
        """
        function: install cluster instance
        input : NA
        output: NA
        """
        self.context.cleanNodeConfig()
        self.checkNodeConfig()
        self.distributeEncryptFiles()

    def checkNodeConfig(self):
        """
        function: Check node config on all nodes
        input : NA
        output: NA
        """
        self.context.logger.log("Checking node configuration on all nodes.")
        # Check node config on all nodes
        cmdParam = ""
        for param in self.context.dataGucParam:
            cmdParam += " -D \\\"%s\\\"" % param

        cmd = "source %s;" % self.context.mpprcFile
        cmd += "%s -U %s -l %s %s" % (
            OMCommand.getLocalScript("Local_Check_Config"), self.context.user,
            self.context.localLog, cmdParam)
        self.context.logger.debug(
            "Command for checking node configuration: %s." % cmd)

        cmd = self.singleCmd(cmd)

        DefaultValue.execCommandWithMode(cmd,
                                         "check node configuration",
                                         self.context.sshTool,
                                         self.context.isSingle)
        self.context.logger.debug("Successfully checked node configuration.")

    def checkNodeInstall(self):
        """
        function: check node install
        input: NA
        output: NA
        """
        self.context.logger.debug("Checking node's installation.", "constant")
        # Checking node's installation
        self.context.logger.log(
            "Checking the installation environment on all nodes.", "constant")
        # Checking the installation environment
        cmd = "source %s;" % self.context.mpprcFile
        cmd += "%s -U %s -R %s -l %s -X %s" % (
            OMCommand.getLocalScript("Local_Check_Install"),
            self.context.user + ":" + self.context.group,
            self.context.clusterInfo.appPath,
            self.context.localLog, self.context.xmlFile)
        self.context.logger.debug(
            "Command for checking installation: %s." % cmd)

        cmd = self.singleCmd(cmd)

        DefaultValue.execCommandWithMode(cmd,
                                         "check installation environment",
                                         self.context.sshTool,
                                         self.context.isSingle)
        self.context.logger.debug("Successfully checked node's installation.",
                                  "constant")

    def distributeEncryptFiles(self):
        """
        function: distribute encrypt files
        input: NA
        output: NA
        """
        # distribute encrypt files to remote host
        # get local hostname
        localHostName = DefaultValue.GetHostIpOrName()
        # get all node names
        hostList = self.context.clusterInfo.getClusterNodeNames()
        # remove the local hostname from hostList
        hostList.remove(localHostName)
        DefaultValue.distributeEncryptFiles(self.context.clusterInfo.appPath,
                                            hostList)

    def initNodeInstance(self):
        """
        function: init instance applications
        input : NA
        output: NA
        """
        self.context.logger.log("Initializing instances on all nodes.")
        # init instance applications
        cmdParam = ""
        # get the --gsinit-parameter parameter values
        for param in self.context.dbInitParam:
            cmdParam += " -P \\\"%s\\\"" % param

        cmd = "source %s;" % self.context.mpprcFile
        # init instances on all nodes
        cmd += "%s -U %s %s -l %s" % (
            OMCommand.getLocalScript("Local_Init_Instance"), self.context.user,
            cmdParam, self.context.localLog)
        self.context.logger.debug(
            "Command for initializing instances: %s" % cmd)

        cmd = self.singleCmd(cmd)

        DefaultValue.execCommandWithMode(cmd,
                                         "initialize instances",
                                         self.context.sshTool,
                                         self.context.isSingle)
        self.context.logger.debug("Successfully initialized node instance.")

    def configInstance(self):
        """
        function: config instance
        input : NA
        output: NA
        """
        # config instance applications
        self.updateInstanceConfig()
        self.updateHbaConfig()

    def checkMemAndCores(self):
        """
        function: memCheck and coresCheck
        input  : NA
        output : False/True
        """
        self.context.logger.log(
            "Check consistence of memCheck and coresCheck on database nodes.")
        self.context.logger.debug(
            "Check whether the memory "
            "and CPU cores of database nodes meet the requirements.")
        self.context.logger.debug("If all database nodes meet follows : ")
        self.context.logger.debug("memory=128G and CPU logic_cores=16")
        self.context.logger.debug(
            "Then we don't use default guc set xmlFile : guc_list.xml")
        checkConsistence = False
        data_check_info = {}
        if self.context.isSingle:
            return False
        all_dn = []
        for dataNode in self.context.clusterInfo.dbNodes:
            if len(dataNode.datanodes) > 0:
                all_dn.append(dataNode)
        self.context.logger.debug(
            "Check consistence of memCheck and coresCheck on database node: %s"
            % [node.name for node in all_dn])
        for dbNode in all_dn:
            memCheck = "cat /proc/cpuinfo | grep processor | wc -l"
            coresCheck = "free -g --si | grep 'Mem' | awk -F ' ' '{print \$2}'"
            cmd = "pssh -s -H %s \"%s & %s\"" % (
                dbNode.name, memCheck, coresCheck)
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0 or len(output.strip().split()) != 2:
                self.context.logger.debug(
                    ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                    + " Error: \n%s" % str(
                        output))
                raise Exception(
                    ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                    + " Error: \n%s" % str(
                        output))
            data_check_info[dbNode.name] = str(output).strip().split()
        self.context.logger.debug(
            "The check info on each node. \nNode : Info(MemSize | CPUCores)")
        for each_node, check_info in data_check_info.items():
            self.context.logger.debug("%s : %s" % (each_node, check_info))
        try:
            if len(set([",".join(value) for value in
                        list(data_check_info.values())])) == 1:
                coresNum = int(list(data_check_info.values())[0][0])
                memSize = int(list(data_check_info.values())[0][1])
                if (coresNum == 16 and memSize >= 124 and memSize <= 132):
                    checkConsistence = True
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53023"] % str(e))
        self.context.logger.log(
            "Successful check consistence of memCheck "
            "and coresCheck on all nodes.")
        return checkConsistence

    def updateInstanceConfig(self):
        """
        function: Update instances config on all nodes
        input : NA
        output: NA
        """
        self.context.logger.log(
            "Updating instance configuration on all nodes.")
        # update instances config on all nodes
        cmdParam = ""
        paralistdn = [param.split('=')[0].strip() for param in
                      self.context.dataGucParam]
        if ("autovacuum" not in paralistdn):
            self.context.dataGucParam.append("autovacuum=on")

        # get the --dn-guc parameter values
        for param in self.context.dataGucParam:
            cmdParam += "*==SYMBOL==*-D*==SYMBOL==*%s" % param
        # check the --alarm-component parameter
        if (self.context.alarm_component != ""):
            cmdParam += "*==SYMBOL==*--alarm=%s" % self.context.alarm_component

        # create tmp file for guc parameters
        # comm_max_datanode and max_process_memory
        self.context.logger.debug("create tmp_guc file.")
        tmpGucPath = DefaultValue.getTmpDirFromEnv(self.context.user)
        tmpGucFile = "%s/tmp_guc" % tmpGucPath
        cmd = g_file.SHELL_CMD_DICT["createFile"] % (
            tmpGucFile, DefaultValue.MAX_DIRECTORY_MODE, tmpGucFile)
        DefaultValue.execCommandWithMode(cmd, "Install applications",
                                         self.context.sshTool,
                                         self.context.isSingle,
                                         self.context.mpprcFile)
        self.context.logger.debug("Create tmp_guc file successfully.")

        # get the master datanode number
        primaryDnNum = DefaultValue.getPrimaryDnNum(self.context.clusterInfo)
        self.context.logger.debug(
            "get master datanode number : %s" % primaryDnNum)
        # get the physic memory of all node and choose the min one
        physicMemo = DefaultValue.getPhysicMemo(self.context.sshTool,
                                                self.context.isSingle)
        self.context.logger.debug("get physic memory value : %s" % physicMemo)
        # get the datanode number in all nodes and choose the max one
        dataNodeNum = DefaultValue.getDataNodeNum(self.context.clusterInfo)
        self.context.logger.debug("get min datanode number : %s" % dataNodeNum)

        # write the value in tmp file
        self.context.logger.debug("Write value in tmp_guc file.")
        gucValueContent = str(primaryDnNum) + "," + str(
            physicMemo) + "," + str(dataNodeNum)
        cmd = g_file.SHELL_CMD_DICT["overWriteFile"] % (
            gucValueContent, tmpGucFile)
        DefaultValue.execCommandWithMode(cmd, "Install applications",
                                         self.context.sshTool,
                                         self.context.isSingle,
                                         self.context.mpprcFile)
        self.context.logger.debug("Write tmp_guc file successfully.")

        # update instances config
        cmd = "source %s;" % self.context.mpprcFile
        cmd += "%s " % (OMCommand.getLocalScript("Local_Config_Instance"))
        paraLine = \
            "*==SYMBOL==*-U*==SYMBOL==*%s%s*==SYMBOL==*-l*==SYMBOL==*%s" % (
                self.context.user, cmdParam, self.context.localLog)
        if (self.context.dws_mode):
            paraLine += "*==SYMBOL==*--dws-mode"
        # get the --gucXml parameter
        if (self.checkMemAndCores()):
            paraLine += "*==SYMBOL==*--gucXml"
        paraLine += "*==SYMBOL==*-X*==SYMBOL==*%s" % self.context.xmlFile
        cmd += DefaultValue.encodeParaline(paraLine, DefaultValue.BASE_ENCODE)

        self.context.logger.debug(
            "Command for updating instances configuration: %s" % cmd)
        DefaultValue.execCommandWithMode(cmd,
                                         "update instances configuration",
                                         self.context.sshTool,
                                         self.context.isSingle)
        self.context.logger.debug("Successfully configured node instance.")

    def updateHbaConfig(self):
        """
        function: config Hba instance
        input : NA
        output: NA
        """
        self.context.logger.log("Configuring pg_hba on all nodes.")

        # Configuring pg_hba
        cmd = "source %s;" % self.context.mpprcFile
        cmd += "%s -U %s -X '%s' -l '%s' " % (
            OMCommand.getLocalScript("Local_Config_Hba"), self.context.user,
            self.context.xmlFile, self.context.localLog)
        self.context.logger.debug(
            "Command for configuring Hba instance: %s" % cmd)
        DefaultValue.execCommandWithMode(cmd,
                                         "config Hba instance",
                                         self.context.sshTool,
                                         self.context.isSingle)
        self.context.logger.debug("Successfully configured HBA.")

    def rollbackInstall(self):
        """
        function: Rollback install
        input : NA
        output: NA
        0 succeed
        1 failed
        2 rollback succeed
        3 rollback failed
        """
        # Rollback install
        self.context.logger.log("Rolling back.")
        try:
            self.deleteTempFileForUninstall()
            # Rollback install
            cmd = "source %s;" % self.context.mpprcFile
            cmd += "%s -U %s -R '%s' -l '%s' -T" % (
                OMCommand.getLocalScript("Local_Uninstall"), self.context.user,
                os.path.realpath(self.context.clusterInfo.appPath),
                self.context.localLog)
            self.context.logger.debug("Command for rolling back: %s." % cmd)
            # exec the cmd for rollback
            (status, output) = self.context.sshTool.getSshStatusOutput(cmd)
            for ret in list(status.values()):
                if (ret != DefaultValue.SUCCESS):
                    raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] %
                                    cmd + "Error:\n%s" % str(output))
            self.context.logger.debug(output)
        except Exception as e:
            # failed to roll back
            self.context.logger.error(str(e))
            sys.exit(ROLLBACK_FAILED)
        # Rollback succeeded
        self.context.logger.log("Rollback succeeded.")

    def checkPgLogFileMode(self):
        """
        function: change pg_log file mode
        input : NA
        output: NA
        """
        try:
            userDir = "%s/%s" % (
                self.context.clusterInfo.logPath, self.context.user)
            # change log file mode
            ClusterCommand.getchangeFileModeCmd(userDir)
        except Exception as e:
            raise Exception(str(e))

    def checkClusterStatus(self):
        """
        function: Check if cluster is running
        input : NA
        output: NA
        """
        # Check if cluster is running
        self.context.logger.debug("Checking the cluster status.", "addStep")
        try:
            cmd = ClusterCommand.getQueryStatusCmd(self.context.user, "", "",
                                                   False)
            (status, output) = subprocess.getstatusoutput(cmd)
            if status == 0:
                # You can find the cluster status,
                # indicating that the cluster is installed, and exit the error.
                self.context.logger.debug("The cmd is %s " % cmd)
                raise Exception(ErrorCode.GAUSS_516["GAUSS_51625"]
                                + " Can not do install now.")
            else:
                self.context.logger.debug(
                    "Successfully checked the cluster status.", "constant")
        except Exception as e:
            self.context.logger.debug("Failed to check cluster status. "
                                      "and the cluster may be not installed.")

    def singleCmd(self, cmd):
        """
        function: remove symbol \ if in single mode.
        input : cmd
        output: str
        """
        # remove symbol \ if in single mode.
        if (self.context.isSingle):
            cmd = cmd.replace("\\", "")
        return cmd

    def distributeRackInfo(self):
        """
        function: Distributing the rack Information File
        input : NA
        output: NA
        """
        node_names = self.context.clusterInfo.getClusterNodeNames()
        DefaultValue.distributeRackFile(self.context.sshTool, node_names)

    def deleteSymbolicAppPath(self):
        """
        function: delete symbolic app path
        input  : NA
        output : NA
        """
        self.context.logger.debug("Delete symbolic link $GAUSSHOME.")
        versionFile = VersionInfo.get_version_file()
        commitid = VersionInfo.get_version_info(versionFile)[2]
        cmd = "rm -rf %s" % self.context.clusterInfo.appPath
        self.context.clusterInfo.appPath = \
            self.context.clusterInfo.appPath + "_" + commitid
        DefaultValue.execCommandWithMode(cmd, "Delete symbolic link",
                                         self.context.sshTool,
                                         self.context.isSingle,
                                         self.context.mpprcFile)
        self.context.logger.debug(
            "Successfully delete symbolic link $GAUSSHOME, cmd: %s." % cmd)
