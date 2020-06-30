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
import time
import signal
import copy
import subprocess
import re
import getpass
from datetime import datetime, timedelta
from multiprocessing.dummy import Pool as ThreadPool

sys.path.append(sys.path[0] + "/../../")
from gspylib.common.GaussLog import GaussLog
from gspylib.common.DbClusterInfo import dbClusterInfo
from gspylib.common.Common import DefaultValue, ClusterCommand, \
    TempfileManagement
from gspylib.common.DbClusterStatus import DbClusterStatus
from gspylib.common.OMCommand import OMCommand
from gspylib.os.gsfile import g_file
from gspylib.os.gsplatform import g_Platform
from gspylib.threads.SshTool import SshTool
from gspylib.common.ErrorCode import ErrorCode
from gspylib.component.Kernel.DN_OLAP.DN_OLAP import DN_OLAP

SPACE_USAGE_DBUSER = 80


class ParallelBaseOM(object):
    """
    Base class of parallel command
    """
    ACTION_INSTALL = "install"
    ACTION_CONFIG = "config"
    ACTION_START = "start"
    ACTION_REDISTRIBUTE = "redistribute"
    ACTION_HEALTHCHECK = "healthcheck"

    HEALTH_CHECK_BEFORE = "before"
    HEALTH_CHECK_AFTER = "after"
    """
    Base class for parallel command
    """

    def __init__(self):
        '''
        Constructor
        '''
        self.logger = None
        self.clusterInfo = None
        self.oldClusterInfo = None
        self.sshTool = None
        self.action = ""

        # Cluster config file.
        self.xmlFile = ""
        self.oldXmlFile = ""

        self.logType = DefaultValue.LOCAL_LOG_FILE
        self.logFile = ""
        self.localLog = ""
        self.user = ""
        self.group = ""
        self.mpprcFile = ""
        # Temporary catalog for install
        self.operateStepDir = TempfileManagement.getTempDir(
            "%s_step" % self.__class__.__name__.lower())
        # Temporary files for install step
        self.operateStepFile = "%s/%s_step.dat" % (
            self.operateStepDir, self.__class__.__name__.lower())
        self.initStep = ""
        self.dws_mode = False
        self.rollbackCommands = []
        self.etcdCons = []
        self.cmCons = []
        self.gtmCons = []
        self.cnCons = []
        self.dnCons = []
        # localMode is same as isSingle in all OM script, expect for
        # gs_preinstall.
        # in gs_preinstall, localMode means local mode for master-standby
        # cluster.
        # in gs_preinstall, localMode also means local mode for single
        # cluster(will not create os user).
        # in gs_preinstall, isSingle means single cluster, it will create
        # os user.
        # not isSingle and not localMode : master-standby cluster global
        # mode(will create os user).
        # not isSingle and localMode : master-standby cluster local
        # mode(will not create os user).
        # isSingle and not localMode : single cluster(will create os user).
        # isSingle and localMode : single cluster(will not create os user).
        self.localMode = False
        self.isSingle = False
        # Indicates whether there is a logical cluster.
        # If elastic_group exists, the current cluster is a logical cluster.
        # Otherwise, it is a large physical cluster.
        self.isElasticGroup = False
        self.isAddElasticGroup = False
        self.lcGroup_name = ""
        # Lock the cluster mode, there are two modes: exclusive lock and
        # wait lock mode,
        # the default exclusive lock
        self.lockMode = "exclusiveLock"

        # SinglePrimaryMultiStandby support binary upgrade, inplace upgrade
        self.isSinglePrimaryMultiStandby = False

        # Adapt to 200 and 300
        self.productVersion = None

    def initComponent(self):
        """
        function: Init component
        input : NA
        output: NA
        """
        for nodeInfo in self.clusterInfo.dbNodes:
            self.initKernelComponent(nodeInfo)

    def initComponentAttributes(self, component):
        """
        function: Init  component attributes on current node
        input : Object component
        output: NA
        """
        component.logger = self.logger
        component.binPath = "%s/bin" % self.clusterInfo.appPath
        component.dwsMode = self.dws_mode

    def initKernelComponent(self, nodeInfo):
        """
        function: Init kernel component
        input : Object nodeInfo
        output: NA
        """
        for inst in nodeInfo.datanodes:
            component = DN_OLAP()
            # init component cluster type
            component.clusterType = self.clusterInfo.clusterType
            component.instInfo = inst
            self.initComponentAttributes(component)
            self.dnCons.append(component)

    def initLogger(self, module=""):
        """
        function: Init logger
        input : module
        output: NA
        """
        # log level
        LOG_DEBUG = 1
        self.logger = GaussLog(self.logFile, module, LOG_DEBUG)

        dirName = os.path.dirname(self.logFile)
        self.localLog = os.path.join(dirName, DefaultValue.LOCAL_LOG_FILE)

    def initClusterInfo(self, refreshCN=True):
        """
        function: Init cluster info
        input : NA
        output: NA
        """
        try:
            self.clusterInfo = dbClusterInfo()
            if (refreshCN):
                static_config_file = "%s/bin/cluster_static_config" % \
                                     DefaultValue.getInstallDir(self.user)
                self.clusterInfo.initFromXml(self.xmlFile, static_config_file)
            else:
                self.clusterInfo.initFromXml(self.xmlFile)
        except Exception as e:
            raise Exception(str(e))
        self.logger.debug("Instance information of cluster:\n%s." %
                          str(self.clusterInfo))

    def initClusterInfoFromStaticFile(self, user, flag=True):
        """
        function: Function to init clusterInfo from static file
        input : user
        output: NA
        """
        try:
            self.clusterInfo = dbClusterInfo()
            self.clusterInfo.initFromStaticConfig(user)
        except Exception as e:
            raise Exception(str(e))
        if flag:
            self.logger.debug("Instance information of cluster:\n%s." %
                              str(self.clusterInfo))

    def initSshTool(self, nodeNames, timeout=0):
        """
        function: Init ssh tool
        input : nodeNames, timeout
        output: NA
        """
        self.sshTool = SshTool(nodeNames, self.logger.logFile, timeout)

    def check_cluster_version_consistency(self, clusterNodes, newNodes=None):
        """
        """
        self.logger.log("Check cluster version consistency.")
        if newNodes is None:
            newNodes = []
        dic_version_info = {}
        # check version.cfg on every node.
        gp_home = DefaultValue.getEnv("GPHOME")
        gauss_home = DefaultValue.getEnv("GAUSSHOME")
        if not (os.path.exists(gp_home) and os.path.exists(gauss_home)):
            GaussLog.exitWithError(ErrorCode.GAUSS_502["GAUSS_50201"] %
                                   ("%s", "or %s") % (gp_home, gauss_home))
        for ip in clusterNodes:
            if ip in newNodes:
                cmd = "pssh -s -H %s 'cat %s/version.cfg'" % \
                      (ip, DefaultValue.getEnv("GPHOME"))
            else:
                cmd = "pssh -s -H %s 'cat %s/bin/upgrade_version'" % \
                      (ip, DefaultValue.getEnv("GAUSSHOME"))
            status, output = subprocess.getstatusoutput(cmd)
            if (status != 0):
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                                " Error:\n%s" % str(output))
            if len(output.strip().split()) < 3:
                raise Exception(ErrorCode.GAUSS_516["GAUSS_51623"])
            dic_version_info[ip] = ",".join(output.strip().split()[1:])

        self.logger.debug("The cluster version on every node.")
        for check_ip, version_info in dic_version_info.items():
            self.logger.debug("%s : %s" % (check_ip, version_info))
        if len(set(dic_version_info.values())) != 1:
            L_inconsistent = list(set(dic_version_info.values()))
            self.logger.debug("The package version on some nodes are "
                              "inconsistent\n%s" % str(L_inconsistent))
            raise Exception("The package version on some nodes are "
                            "inconsistent,%s" % str(L_inconsistent))
        self.logger.log("Successfully checked cluster version.")

    def checkBaseFile(self, checkXml=True):
        """
        function: Check xml file and log file
        input : checkXml
        output: NA
        """
        if (checkXml):
            if (self.xmlFile == ""):
                raise Exception(ErrorCode.GAUSS_500["GAUSS_50001"] % 'X' + ".")

            if (not os.path.exists(self.xmlFile)):
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] %
                                ("configuration file [%s]" % self.xmlFile))

            if (not os.path.isabs(self.xmlFile)):
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50213"] %
                                ("configuration file [%s]" % self.xmlFile))
        else:
            self.xmlFile = ""

        if (self.logFile == ""):
            self.logFile = DefaultValue.getOMLogPath(self.logType,
                                                     self.user, "",
                                                     self.xmlFile)

        if (not os.path.isabs(self.logFile)):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50213"] % "log")

    def initSignalHandler(self):
        """
        function: Function to init signal handler
        input : NA
        output: NA
        """
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGQUIT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGALRM, signal.SIG_IGN)
        signal.signal(signal.SIGHUP, signal.SIG_IGN)
        signal.signal(signal.SIGUSR1, signal.SIG_IGN)
        signal.signal(signal.SIGUSR2, signal.SIG_IGN)

    def print_signal_stack(self, frame):
        """
        function: Function to print signal stack
        input : frame
        output: NA
        """
        if (self.logger is None):
            return
        try:
            import inspect
            stacks = inspect.getouterframes(frame)
            for curr in range(len(stacks)):
                stack = stacks[curr]
                self.logger.debug("Stack level: %d. File: %s. Function: "
                                  "%s. LineNo: %d." % (
                                      curr, stack[1], stack[3],
                                      stack[2]))
                self.logger.debug("Code: %s." % (
                    stack[4][0].strip().strip("\n")))
        except Exception as e:
            self.logger.debug("Failed to print signal stack. Error: \n%s" %
                              str(e))

    def raise_handler(self, signal_num, frame):
        """
        function: Function to raise handler
        input : signal_num, frame
        output: NA
        """
        if (self.logger is not None):
            self.logger.debug("Received signal[%d]." % (signal_num))
            self.print_signal_stack(frame)
        raise Exception(ErrorCode.GAUSS_516["GAUSS_51614"] % (signal_num))

    def setupTimeoutHandler(self):
        """
        function: Function to set up time out handler
        input : NA
        output: NA
        """
        signal.signal(signal.SIGALRM, self.timeout_handler)

    def setTimer(self, timeout):
        """
        function: Function to set timer
        input : timeout
        output: NA
        """
        self.logger.debug("Set timer. The timeout: %d." % timeout)
        signal.signal(signal.SIGALRM, self.timeout_handler)
        signal.alarm(timeout)

    def resetTimer(self):
        """
        function: Reset timer
        input : NA
        output: NA
        """
        signal.signal(signal.SIGALRM, signal.SIG_IGN)
        self.logger.debug("Reset timer. Left time: %d." % signal.alarm(0))

    def timeout_handler(self, signal_num, frame):
        """
        function: Received the timeout signal
        input : signal_num, frame
        output: NA
        """
        if (self.logger is not None):
            self.logger.debug("Received the timeout signal: [%d]." %
                              (signal_num))
            self.print_signal_stack(frame)
        raise Timeout("Time out.")

    def waitProcessStop(self, processKeywords, hostname):
        """
        function: Wait the process stop
        input : process name 
        output: NA
        """
        count = 0
        while (True):
            psCmd = "ps ux|grep -v grep |awk '{print \$11}'|grep '%s' " % \
                    processKeywords.strip()
            (status, output) = self.sshTool.getSshStatusOutput(
                psCmd, [hostname])
            # Determine whether the process can be found.
            if (status[hostname] != DefaultValue.SUCCESS):
                self.logger.debug("The %s process stopped." % processKeywords)
                break

            count += 1
            if (count % 20 == 0):
                self.logger.debug("The %s process exists." % processKeywords)
            time.sleep(3)

    def managerOperateStepDir(self, action='create', nodes=None):
        """
        function: manager operate step directory 
        input : NA
        output: currentStep
        """
        if nodes is None:
            nodes = []
        try:
            # Creating the backup directory
            if (action == "create"):
                cmd = "(if [ ! -d '%s' ];then mkdir -p '%s' -m %s;fi)" % (
                    self.operateStepDir, self.operateStepDir,
                    DefaultValue.KEY_DIRECTORY_MODE)
            else:
                cmd = "(if [ -d '%s' ];then rm -rf '%s';fi)" % (
                    self.operateStepDir, self.operateStepDir)
            DefaultValue.execCommandWithMode(cmd,
                                             "%s temporary directory" % action,
                                             self.sshTool,
                                             self.localMode or self.isSingle,
                                             "",
                                             nodes)
        except Exception as e:
            raise Exception(str(e))

    def readOperateStep(self):
        """
        function: read operate step signal 
        input : NA
        output: currentStep
        """
        currentStep = self.initStep

        if not os.path.exists(self.operateStepFile):
            self.logger.debug("The %s does not exits." % self.operateStepFile)
            return currentStep

        if not os.path.isfile(self.operateStepFile):
            self.logger.debug("The %s must be a file." % self.operateStepFile)
            return currentStep

        with open(self.operateStepFile, "r") as fp:
            line = fp.readline().strip()
            if line is not None and line != "":
                currentStep = line

        return currentStep

    def writeOperateStep(self, stepName, nodes=None):
        """
        function: write operate step signal 
        input : step
        output: NA
        """
        if nodes is None:
            nodes = []
        try:
            # write the step into INSTALL_STEP
            # open the INSTALL_STEP
            with open(self.operateStepFile, "w") as g_DB:
                # write the INSTALL_STEP
                g_DB.write(stepName)
                g_DB.write(os.linesep)
                g_DB.flush()
            # change the INSTALL_STEP permissions
            g_file.changeMode(DefaultValue.KEY_FILE_MODE, self.operateStepFile)

            # distribute file to all nodes
            cmd = "mkdir -p -m %s '%s'" % (DefaultValue.KEY_DIRECTORY_MODE,
                                           self.operateStepDir)
            DefaultValue.execCommandWithMode(cmd,
                                             "create backup directory "
                                             "on all nodes",
                                             self.sshTool,
                                             self.localMode or self.isSingle,
                                             "",
                                             nodes)

            if not self.localMode and not self.isSingle:
                self.sshTool.scpFiles(self.operateStepFile,
                                      self.operateStepDir, nodes)
        except Exception as e:
            # failed to write the step into INSTALL_STEP
            raise Exception(str(e))

    def distributeFiles(self):
        """
        function: distribute package to every host
        input : NA
        output: NA
        """
        self.logger.debug("Distributing files.")
        try:
            # get the all nodes
            hosts = self.clusterInfo.getClusterNodeNames()
            if DefaultValue.GetHostIpOrName() not in hosts:
                raise Exception(ErrorCode.GAUSS_516["GAUSS_51619"] %
                                DefaultValue.GetHostIpOrName())
            hosts.remove(DefaultValue.GetHostIpOrName())
            # Send xml file to every host
            DefaultValue.distributeXmlConfFile(self.sshTool, self.xmlFile,
                                               hosts, self.mpprcFile)
            # Successfully distributed files
            self.logger.debug("Successfully distributed files.")
        except Exception as e:
            # failed to distribute package to every host
            raise Exception(str(e))

    def checkPreInstall(self, user, flag, nodes=None):
        """
        function: check if have done preinstall on given nodes
        input : user, nodes
        output: NA
        """
        if nodes is None:
            nodes = []
        try:
            cmd = "%s -U %s -t %s" % (
                OMCommand.getLocalScript("Local_Check_PreInstall"), user, flag)
            DefaultValue.execCommandWithMode(
                cmd, "check preinstall", self.sshTool,
                self.localMode or self.isSingle, "", nodes)
        except Exception as e:
            raise Exception(str(e))

    def checkNodeInstall(self, nodes=None, checkParams=None,
                         strictUserCheck=True):
        """
        function: Check node install
        input : nodes, checkParams, strictUserCheck
        output: NA
        """
        if nodes is None:
            nodes = []
        if checkParams is None:
            checkParams = []
        validParam = ["shared_buffers", "max_connections"]
        cooGucParam = ""
        for param in checkParams:
            entry = param.split("=")
            if (len(entry) != 2):
                raise Exception(ErrorCode.GAUSS_500["GAUSS_50009"])
            if (entry[0].strip() in validParam):
                cooGucParam += " -C \\\"%s\\\"" % param
        self.logger.log("Checking installation environment on all nodes.")
        cmd = "%s -U %s:%s -R %s %s -l %s -X '%s'" % (
            OMCommand.getLocalScript("Local_Check_Install"), self.user,
            self.group, self.clusterInfo.appPath, cooGucParam, self.localLog,
            self.xmlFile)
        if (not strictUserCheck):
            cmd += " -O"
        self.logger.debug("Checking the install command: %s." % cmd)
        DefaultValue.execCommandWithMode(cmd,
                                         "check installation environment",
                                         self.sshTool,
                                         self.localMode or self.isSingle,
                                         "",
                                         nodes)

    def cleanNodeConfig(self, nodes=None, datadirs=None):
        """
        function: Clean instance
        input : nodes, datadirs
        output: NA
        """
        self.logger.log("Deleting instances from all nodes.")
        if nodes is None:
            nodes = []
        if datadirs is None:
            datadirs = []
        cmdParam = ""
        for datadir in datadirs:
            cmdParam += " -D %s " % datadir
        cmd = "%s -U %s %s -l %s" % (
            OMCommand.getLocalScript("Local_Clean_Instance"),
            self.user, cmdParam, self.localLog)
        DefaultValue.execCommandWithMode(
            cmd, "clean instance", self.sshTool,
            self.localMode or self.isSingle, "", nodes)
        self.logger.log("Successfully deleted instances from all nodes.")

    @staticmethod
    def getPrepareKeysCmd(key_file, user, confFile, destPath, logfile,
                          userProfile="", localMode=False):
        """
        function: get  etcd communication keys command
        input: key_file, user, confFile, destPath, localMode:do not scp keys
        output: NA
        """
        if (not os.path.exists(key_file)):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % key_file)
        if (not userProfile):
            userProfile = DefaultValue.getMpprcFile()
        # create the directory on all nodes
        cmd = "source %s; %s -U %s -X %s --src-file=%s --dest-path=%s -l %s" \
              % (userProfile, OMCommand.getLocalScript("Local_PrepareKeys"),
               user, confFile, key_file, destPath, logfile)
        # if local mode, only prepare keys, do not scp keys to cluster nodes
        if (localMode):
            cmd += " -L"
        return cmd

    def getClusterRings(self, clusterInfo):
        """
        function: get clusterRings from cluster info
        input: DbclusterInfo() instance
        output: list
        """
        hostPerNodeList = self.getDNHostnamesPerNode(clusterInfo)
        # Loop the hostname list on each node where the master and slave
        # of the DB instance.
        for i in range(len(hostPerNodeList)):
            # Loop the list after the i-th list
            for perNodelist in hostPerNodeList[i + 1:len(hostPerNodeList)]:
                # Define a tag
                flag = 0
                # Loop the elements of each perNodelist
                for hostNameElement in perNodelist:
                    # If elements on the i-th node, each element of the
                    # list are joined in hostPerNodeList[i
                    if hostNameElement in hostPerNodeList[i]:
                        flag = 1
                        for element in perNodelist:
                            if element not in hostPerNodeList[i]:
                                hostPerNodeList[i].append(element)
                if (flag == 1):
                    hostPerNodeList.remove(perNodelist)

        return hostPerNodeList

    def getDNHostnamesPerNode(self, clusterInfo):
        """
        function: get DB hostnames per node
        input: DbclusterInfo() instance
        output: list
        """
        hostPerNodeList = []
        for dbNode in clusterInfo.dbNodes:
            nodeDnlist = []
            # loop per node
            for dnInst in dbNode.datanodes:
                if (dnInst.instanceType == DefaultValue.MASTER_INSTANCE):
                    if dnInst.hostname not in nodeDnlist:
                        nodeDnlist.append(dnInst.hostname)
                    # get other standby and dummy hostname
                    instances = clusterInfo.getPeerInstance(dnInst)
                    for inst in instances:
                        if inst.hostname not in nodeDnlist:
                            nodeDnlist.append(inst.hostname)
            if nodeDnlist != []:
                hostPerNodeList.append(nodeDnlist)
        return hostPerNodeList

    # for olap function
    def checkIsElasticGroupExist(self, dbNodes):
        """
        function: Check if elastic_group exists.
        input : NA
        output: NA
        """
        self.logger.debug("Checking if elastic group exists.")

        self.isElasticGroup = False
        coorNode = []
        # traverse old nodes
        for dbNode in dbNodes:
            if (len(dbNode.coordinators) >= 1):
                coorNode.append(dbNode.coordinators[0])
                break

        # check elastic group
        CHECK_GROUP_SQL = "SELECT count(*) FROM pg_catalog.pgxc_group " \
                          "WHERE group_name='elastic_group' " \
                          "and group_kind='e'; "
        (checkstatus, checkoutput) = ClusterCommand.remoteSQLCommand(
            CHECK_GROUP_SQL, self.user, coorNode[0].hostname, coorNode[0].port)
        if (checkstatus != 0 or not checkoutput.isdigit()):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50219"] %
                            "node group" + " Error:\n%s" % str(checkoutput))
        elif (checkoutput.strip() == '1'):
            self.isElasticGroup = True
        elif (checkoutput.strip() == '0'):
            self.isElasticGroup = False
        else:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50219"] %
                            "the number of node group")

        self.logger.debug("Successfully checked if elastic group exists.")

    def checkHostnameIsLoop(self, nodenameList):
        """
        function: check if hostname is looped
        input : NA
        output: NA
        """
        isRing = True
        # 1.get ring information in the cluster
        clusterRings = self.getClusterRings(self.clusterInfo)
        nodeRing = ""
        nodenameRings = []
        # 2.Check if the node is in the ring
        for num in iter(clusterRings):
            ringNodeList = []
            for nodename in nodenameList:
                if (nodename in num):
                    ringNodeList.append(nodename)
            if (len(ringNodeList) != 0 and len(ringNodeList) ==
                    len(num)):
                nodenameRings.append(ringNodeList)
            if (len(ringNodeList) != 0 and len(ringNodeList) !=
                    len(num)):
                isRing = False
                break
            else:
                continue
        if not isRing:
            raise Exception(ErrorCode.GAUSS_500["GAUSS_50004"] % "h" +
                            " The hostname (%s) specified by the -h parameter "
                            "must be looped." % nodeRing)
        return (clusterRings, nodenameRings)

    def getDNinstanceByNodeName(self, hostname, isMaster=True):
        """
        function: Get the DB instance of the node based on the node name.
        input : hostname
                isMaster: get master DB instance
        output: NA
        """
        masterdnInsts = []
        standbydnInsts = []
        # notice
        for dbNode in self.clusterInfo.dbNodes:
            if (dbNode.name == hostname):
                for dbInst in dbNode.datanodes:
                    # get master DB instance
                    if (dbInst.instanceType == DefaultValue.MASTER_INSTANCE):
                        masterdnInsts.append(dbInst)
                    # get standby or dummy DB instance
                    else:
                        standbydnInsts.append(dbInst)

        if (isMaster):
            return masterdnInsts
        else:
            return standbydnInsts

    def getSQLResultList(self, sql, user, hostname, port,
                         database="postgres"):
        """
        """
        (status, output) = ClusterCommand.remoteSQLCommand(sql, user,
                                                           hostname, port,
                                                           False, database)
        if status != 0 or ClusterCommand.findErrorInSql(output):
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % sql +
                            " Error:\n%s" % str(output))
        # split the output string with '\n'
        resultList = output.split("\n")
        return resultList

    def getCooInst(self):
        """
        function: get CN instance
        input : NA
        output: CN instance
        """
        coorInst = []
        # get CN on nodes
        for dbNode in self.clusterInfo.dbNodes:
            if (len(dbNode.coordinators) >= 1):
                coorInst.append(dbNode.coordinators[0])
        # check if contain CN on nodes
        if (len(coorInst) == 0):
            raise Exception(ErrorCode.GAUSS_526["GAUSS_52602"])
        else:
            return coorInst

    def getGroupName(self, fieldName, fieldVaule):
        """
        function: Get nodegroup name by field name and field vaule.
        input : field name and field vaule
        output: node group name
        """
        # 1.get CN instance info from cluster
        cooInst = self.getCooInst()

        # 2.obtain the node group
        OBTAIN_SQL = "select group_name from pgxc_group where %s = %s; " % \
                     (fieldName, fieldVaule)
        # execute the sql command
        (status, output) = ClusterCommand.remoteSQLCommand(OBTAIN_SQL,
                                                           self.user,
                                                           cooInst[0].hostname,
                                                           cooInst[0].port,
                                                           ignoreError=False)
        if (status != 0):
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] %
                            OBTAIN_SQL + " Error:\n%s" % str(output))

        return output.strip()

    def killKernalSnapshotThread(self, coorInst):
        """
        function: kill snapshot thread in Kernel,
                avoid dead lock with redistribution)
        input : NA
        output: NA
        """
        self.logger.debug("Stopping snapshot thread in database node Kernel.")
        killSnapshotSQL = "select * from kill_snapshot();"

        (status, output) = ClusterCommand.remoteSQLCommand(
            killSnapshotSQL, self.user, coorInst.hostname, coorInst.port,
            False, DefaultValue.DEFAULT_DB_NAME)
        if (status != 0):
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] %
                            killSnapshotSQL + " Error:\n%s" % str(output))
        self.logger.debug("Successfully stopped snapshot "
                          "thread in database node Kernel.")

    def createServerCa(self, hostList=None):
        """
        function: create grpc ca file
        input : NA
        output: NA
        """
        self.logger.debug("Generating CA files.")
        if hostList is None:
            hostList = []
        appPath = DefaultValue.getInstallDir(self.user)
        caPath = os.path.join(appPath, "share/sslcert/om")
        self.logger.debug("The ca file dir is: %s." % caPath)
        if (len(hostList) == 0):
            for dbNode in self.clusterInfo.dbNodes:
                hostList.append(dbNode.name)
        # Create CA dir and prepare files for using.
        self.logger.debug("Create CA file directory.")
        try:
            DefaultValue.createCADir(self.sshTool, caPath, hostList)
            self.logger.debug("Add hostname to config file.")
            DefaultValue.createServerCA(DefaultValue.SERVER_CA, caPath,
                                        self.logger)
            # Clean useless files, and change permission of ca file to 600.
            DefaultValue.cleanServerCaDir(caPath)
            self.logger.debug("Scp CA files to all nodes.")
        except Exception as e:
            certFile = caPath + "/demoCA/cacert.pem"
            if os.path.exists(certFile):
                g_file.removeFile(certFile)
            DefaultValue.cleanServerCaDir(caPath)
            raise Exception(str(e))
        for certFile in DefaultValue.SERVER_CERT_LIST:
            scpFile = os.path.join(caPath, "%s" % certFile)
            self.sshTool.scpFiles(scpFile, caPath, hostList)
        self.logger.debug("Successfully generated server CA files.")

    def createGrpcCa(self, hostList=None):
        """
        function: create grpc ca file
        input : NA
        output: NA
        """
        self.logger.debug("Generating grpc CA files.")
        if hostList is None:
            hostList = []
        appPath = DefaultValue.getInstallDir(self.user)
        caPath = os.path.join(appPath, "share/sslcert/grpc")
        self.logger.debug("The ca file dir is: %s." % caPath)
        if (len(hostList) == 0):
            for dbNode in self.clusterInfo.dbNodes:
                hostList.append(dbNode.name)
        # Create CA dir and prepare files for using.
        self.logger.debug("Create CA file directory.")
        try:
            DefaultValue.createCADir(self.sshTool, caPath, hostList)
            self.logger.debug("Add hostname to config file.")
            configPath = os.path.join(appPath,
                                      "share/sslcert/grpc/openssl.cnf")
            self.logger.debug("The ca file dir is: %s." % caPath)
            # Add hostname to openssl.cnf file.
            DefaultValue.changeOpenSslConf(configPath, hostList)
            self.logger.debug("Generate CA files.")
            DefaultValue.createCA(DefaultValue.GRPC_CA, caPath)
            # Clean useless files, and change permission of ca file to 600.
            DefaultValue.cleanCaDir(caPath)
            self.logger.debug("Scp CA files to all nodes.")
        except Exception as e:
            certFile = caPath + "/demoCA/cacertnew.pem"
            if os.path.exists(certFile):
                g_file.removeFile(certFile)
            DefaultValue.cleanCaDir(caPath)
            raise Exception(str(e))
        for certFile in DefaultValue.GRPC_CERT_LIST:
            scpFile = os.path.join(caPath, "%s" % certFile)
            self.sshTool.scpFiles(scpFile, caPath, hostList)
        self.logger.debug("Successfully generated grpc CA files.")

    def genCipherAndRandFile(self, hostList=None):
        self.logger.debug("Encrypting cipher and rand files.")
        if hostList is None:
            hostList = []
        appPath = DefaultValue.getInstallDir(self.user)
        binPath = os.path.join(appPath, "bin")
        retry = 0
        while True:
            sshpwd = getpass.getpass("Please enter password for database:")
            sshpwd_check = getpass.getpass("Please repeat for database:")
            if sshpwd_check != sshpwd:
                sshpwd = ""
                sshpwd_check = ""
                self.logger.error(
                    ErrorCode.GAUSS_503["GAUSS_50306"] % "database"
                    + "The two passwords are different, "
                      "please enter password again.")
            else:
                cmd = "%s/gs_guc encrypt -M server -K %s -D %s " % (binPath,
                                                                    sshpwd,
                                                                    binPath)
                (status, output) = subprocess.getstatusoutput(cmd)
                sshpwd = ""
                sshpwd_check = ""
                if status != 0:
                    self.logger.error(
                        ErrorCode.GAUSS_503["GAUSS_50322"] % "database"
                        + "Error:\n %s" % output)
                else:
                    break
            if retry >= 2:
                raise Exception(
                    ErrorCode.GAUSS_503["GAUSS_50322"] % "database")
            retry += 1
        g_file.changeMode(DefaultValue.KEY_FILE_MODE,
                          "'%s'/server.key.cipher" % binPath)
        g_file.changeMode(DefaultValue.KEY_FILE_MODE,
                          "'%s'/server.key.rand" % binPath)
        if len(hostList) == 0:
            for dbNode in self.clusterInfo.dbNodes:
                hostList.append(dbNode.name)
        for certFile in DefaultValue.BIN_CERT_LIST:
            scpFile = os.path.join(binPath, "%s" % certFile)
            self.sshTool.scpFiles(scpFile, binPath, hostList)
        self.logger.debug("Successfully encrypted cipher and rand files.")


class Timeout(Exception):
    pass
