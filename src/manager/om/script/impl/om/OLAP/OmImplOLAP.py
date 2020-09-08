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
# Description : omManagerImplOLAP.py is a utility to manage a Gauss200 cluster.
#############################################################################
import subprocess
import sys
import re
import time

sys.path.append(sys.path[0] + "/../../../../")
from gspylib.common.DbClusterInfo import dbClusterInfo, queryCmd
from gspylib.threads.SshTool import SshTool
from gspylib.common.ErrorCode import ErrorCode
from gspylib.common.Common import DefaultValue
from gspylib.common.OMCommand import OMCommand
from impl.om.OmImpl import OmImpl
from gspylib.os.gsfile import g_file

# action type
ACTION_CHANGEIP = "changeip"

# tmp file that storage change io information
CHANGEIP_BACKUP_DIR = "%s/change_ip_bak" % DefaultValue.getTmpDirFromEnv()
# cluster_static_config file
CHANGEIP_BAK_STATIC = "%s/cluster_static_config" % CHANGEIP_BACKUP_DIR
# daily alarm timeout waiting for other nodes to complete
DAILY_ALARM_TIME_OUT = 300
# daily alarm result file validity time
DAILY_ALARM_FILE_VALIDITY_TIME = 60 * 60 * 1
DAILY_ALARM_OUT_FILE = ""
# The shell script with check remote result file change time
DAILY_ALARM_SHELL_FILE = "/tmp/om_dailyAlarm_%s.sh" % \
                         DefaultValue.GetHostIpOrName()
# The tmp file with cluster status
DAILY_ALARM_STATUS_FILE = "/tmp/gauss_cluster_status_dailyAlarm.dat"

ISOLATE_TIMEOUT = 180
KILL_SESSION = "select pg_terminate_backend(pid) " \
               "from pg_stat_activity where state " \
               "in ('active', 'fastpath function call', 'retrying') and " \
               "query not like '%terminate%' " \
               "and application_name not " \
               "in('JobScheduler','WorkloadMonitor'," \
               "'workload','WLMArbiter','cm_agent');"
QUERY_SESSION = "select pid from pg_stat_activity" \
                " where state " \
                "in ('active', 'fastpath function call', 'retrying') and  " \
                "query not like '%terminate%' " \
                "and application_name " \
                "not in('JobScheduler','WorkloadMonitor'" \
                ",'workload','WLMArbiter','cm_agent');"


###########################################
class OmImplOLAP(OmImpl):
    """
    class: OmImplOLAP
    """

    def __init__(self, OperationManager=None):
        """
        function:class init
        input:OperationManager
        output:NA
        """
        OmImpl.__init__(self, OperationManager)

    def checkNode(self):
        """
        function: check if the current node is to be uninstalled
        input : NA
        output: NA
        """
        if (len(
                self.context.g_opts.nodeInfo) != 0
                and self.context.g_opts.hostname ==
                DefaultValue.GetHostIpOrName()):
            raise Exception(
                ErrorCode.GAUSS_516["GAUSS_51631"] % "coordinate"
                + "\nPlease perform this operation on other nodes "
                  "because this node will be deleted.")

    # AP
    def stopCluster(self):
        """
        function:Stop cluster
        input:NA
        output:NA
        """
        self.logger.log("Stopping the cluster.")
        # Stop cluster in 300 seconds
        cmd = "source %s; %s -t %d" % (
            self.context.g_opts.mpprcFile, OMCommand.getLocalScript("Gs_Stop"),
            DefaultValue.TIMEOUT_CLUSTER_STOP)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            self.logger.log(
                "Warning: Failed to stop cluster within 300 seconds,"
                "stopping cluster again at immediate mode.")
            cmd = "source %s; %s -m immediate -t %d" % (
                self.context.g_opts.mpprcFile,
                OMCommand.getLocalScript("Gs_Stop"),
                DefaultValue.TIMEOUT_CLUSTER_STOP)
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                self.logger.log("The cmd is %s " % cmd)
                raise Exception(
                    ErrorCode.GAUSS_516["GAUSS_51610"]
                    % "the cluster at immediate mode"
                    + " Error: \n%s" % output)

        self.logger.log("Successfully stopped the cluster.")

    # AP
    def startCluster(self):
        """
        function:Start cluster
        input:NA
        output:NA
        """
        self.logger.log("Starting the cluster.", "addStep")
        # Delete cluster dynamic config if it is exist on all nodes
        clusterDynamicConf = "%s/bin/cluster_dynamic_config" \
                             % self.oldClusterInfo.appPath
        cmd = g_file.SHELL_CMD_DICT["deleteFile"] % (
            clusterDynamicConf, clusterDynamicConf)
        self.logger.debug(
            "Command for removing the cluster dynamic configuration: %s."
            % cmd)
        self.sshTool.executeCommand(cmd, "remove dynamic configuration")
        # Start cluster in 300 seconds
        cmd = "source %s; %s -t %s" % (
            self.context.g_opts.mpprcFile,
            OMCommand.getLocalScript("Gs_Start"),
            DefaultValue.TIMEOUT_CLUSTER_START)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            self.logger.debug("The cmd is %s " % cmd)
            raise Exception(
                ErrorCode.GAUSS_516["GAUSS_51607"]
                % "the cluster" + " Error: \n%s" % output)

        self.logger.log("Successfully started the cluster.", "constant")

    ##########################################################################
    # Start Flow
    ##########################################################################
    def getNodeId(self):
        """
        function: get node Id
        input: NA
        output: NA
        """
        clusterType = "cluster"
        nodeId = 0
        if (self.context.g_opts.nodeName != ""):
            clusterType = "node"
            dbNode = self.context.clusterInfo.getDbNodeByName(
                self.context.g_opts.nodeName)
            if not dbNode:
                raise Exception(
                    ErrorCode.GAUSS_516["GAUSS_51619"]
                    % self.context.g_opts.nodeName)
            nodeId = dbNode.id
        elif (self.context.g_opts.azName != ""):
            clusterType = self.context.g_opts.azName
            # check whether the given azName is in the cluster
            if (
                    self.context.g_opts.azName
                    not in self.context.clusterInfo.getazNames()):
                raise Exception(
                    ErrorCode.GAUSS_500["GAUSS_50004"]
                    % '-az' + " The az name [%s] is not in the cluster."
                    % self.context.g_opts.azName)
        return nodeId, clusterType

    def doStartCluster(self):
        """
        function: do start cluster
        input: NA
        output: NA
        """
        self.logger.debug("Operating: Starting.")
        # Specifies the stop node
        # Gets the specified node id
        startType = "node" if self.context.g_opts.nodeName != "" else "cluster"
        # Perform a start operation
        self.logger.log("Starting %s." % startType)
        self.logger.log("=========================================")
        hostName = DefaultValue.GetHostIpOrName()
        #get the newest dynaminc config and send to other node
        self.clusterInfo.checkClusterDynamicConfig(self.context.user, hostName)
        if self.context.g_opts.nodeName == "":
            hostList = self.clusterInfo.getClusterNodeNames()
        else:
            hostList = []
            hostList.append(self.context.g_opts.nodeName)
        self.sshTool = SshTool(self.clusterInfo.getClusterNodeNames(), None,
                               DefaultValue.TIMEOUT_CLUSTER_START)
        if self.time_out is None:
            time_out = DefaultValue.TIMEOUT_CLUSTER_START
        else:
            time_out = self.time_out
        cmd = "source %s; %s -U %s -R %s -t %s --security-mode=%s" % (
        self.context.g_opts.mpprcFile,
        OMCommand.getLocalScript("Local_StartInstance"),
        self.context.user, self.context.clusterInfo.appPath, time_out,
        self.context.g_opts.security_mode)
        if self.dataDir != "":
            cmd += " -D %s" % self.dataDir
        (statusMap, output) = self.sshTool.getSshStatusOutput(cmd, hostList)
        for nodeName in hostList:
            if statusMap[nodeName] != 'Success':
                raise Exception(
                    ErrorCode.GAUSS_536["GAUSS_53600"] % (cmd, output))
        if re.search("another server might be running", output):
            self.logger.log(output)
        if startType == "cluster":
            starttime = time.time()
            cluster_state = ""
            cmd = "source %s; gs_om -t status|grep cluster_state" \
                  % self.context.g_opts.mpprcFile
            while time.time() <= 30 + starttime:
                status, output = subprocess.getstatusoutput(cmd)
                if status != 0:
                    raise Exception(
                        ErrorCode.GAUSS_516["GAUSS_51607"] % "cluster" +
                        " After startup, check cluster_state failed")
                else:
                    cluster_state = output.split()[-1]
                    if cluster_state != "Normal":
                        self.logger.log("Waiting for check cluster state...")
                        time.sleep(5)
                    else:
                        break
            if cluster_state != "Normal":
                raise Exception(ErrorCode.GAUSS_516["GAUSS_51607"] % "cluster"
                                + " After startup, the last check results were"
                                  " %s. Please check manually."
                                % cluster_state)
        self.logger.log("=========================================")
        self.logger.log("Successfully started.")
        self.logger.debug("Operation succeeded: Start.")

    def doStopCluster(self):
        """
        function: do stop cluster
        input: NA
        output: NA
        """
        self.logger.debug("Operating: Stopping.")
        # Specifies the stop node
        # Gets the specified node id
        stopType = "node" if self.context.g_opts.nodeName != "" else "cluster"
        # Perform a stop operation
        self.logger.log("Stopping %s." % stopType)
        self.logger.log("=========================================")
        if self.context.g_opts.nodeName == "":
            hostList = self.clusterInfo.getClusterNodeNames()
        else:
            hostList = []
            hostList.append(self.context.g_opts.nodeName)
        self.sshTool = SshTool(self.clusterInfo.getClusterNodeNames(), None,
                               DefaultValue.TIMEOUT_CLUSTER_START)
        if self.time_out is None:
            time_out = DefaultValue.TIMEOUT_CLUSTER_STOP
        else:
            time_out = self.time_out
        cmd = "source %s; %s -U %s -R %s -t %s" % (
            self.context.g_opts.mpprcFile,
            OMCommand.getLocalScript("Local_StopInstance"),
            self.context.user, self.context.clusterInfo.appPath, time_out)
        if self.dataDir != "":
            cmd += " -D %s" % self.dataDir
        if self.mode != "":
            cmd += " -m %s" % self.mode
        (statusMap, output) = self.sshTool.getSshStatusOutput(cmd, hostList)
        for nodeName in hostList:
            if statusMap[nodeName] != 'Success':
                raise Exception(
                    ErrorCode.GAUSS_536["GAUSS_53606"] % (cmd, output))
        self.logger.log("Successfully stopped %s." % stopType)

        self.logger.log("=========================================")
        self.logger.log("End stop %s." % stopType)
        self.logger.debug("Operation succeeded: Stop.")

    def doView(self):
        """
        function:get cluster node info
        input:NA
        output:NA
        """
        # view static_config_file
        self.context.clusterInfo.printStaticConfig(self.context.user,
                                                   self.context.g_opts.outFile)

    def doQuery(self):
        """
        function: do query
        input  : NA
        output : NA
        """
        hostName = DefaultValue.GetHostIpOrName()
        sshtool = SshTool(self.context.clusterInfo.getClusterNodeNames())
        cmd = queryCmd()
        if (self.context.g_opts.outFile != ""):
            cmd.outputFile = self.context.g_opts.outFile
        self.context.clusterInfo.queryClsInfo(hostName, sshtool,
                                              self.context.mpprcFile, cmd)

    def doRefreshConf(self):
        """
        function: do refresh conf
        input  : NA
        output : NA
        """
        if self.context.clusterInfo.isSingleNode():
            self.logger.log(
                "No need to generate dynamic configuration file for one node.")
            return
        self.logger.log("Generating dynamic configuration file for all nodes.")
        hostName = DefaultValue.GetHostIpOrName()
        sshtool = SshTool(self.context.clusterInfo.getClusterNodeNames())
        self.context.clusterInfo.createDynamicConfig(self.context.user,
                                                     hostName, sshtool)
        self.logger.log("Successfully generated dynamic configuration file.")
