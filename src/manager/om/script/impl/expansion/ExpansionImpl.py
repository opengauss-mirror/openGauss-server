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
# Description  : ExpansionImpl.py
#############################################################################

import subprocess
import sys
import re
import os
import getpass
import pwd
import datetime
import weakref
from random import sample
import time
import grp
import socket
import stat
from multiprocessing import Process, Value

sys.path.append(sys.path[0] + "/../../../../")
from gspylib.common.DbClusterInfo import dbClusterInfo, queryCmd
from gspylib.threads.SshTool import SshTool
from gspylib.common.DbClusterStatus import DbClusterStatus
from gspylib.common.ErrorCode import ErrorCode
from gspylib.common.Common import DefaultValue
from gspylib.common.GaussLog import GaussLog

sys.path.append(sys.path[0] + "/../../../lib/")
DefaultValue.doConfigForParamiko()
import paramiko


#mode
MODE_PRIMARY = "primary"
MODE_STANDBY = "standby"
MODE_NORMAL = "normal"

#db state
STAT_NORMAL = "normal"

# master 
MASTER_INSTANCE = 0
# standby 
STANDBY_INSTANCE = 1

# statu failed
STATUS_FAIL = "Failure"

class ExpansionImpl():
    """
    class for expansion standby node.
    step:
        1. preinstall database on new standby node
        2. install as single-node database
        3. establish primary-standby relationship of all node
    """

    def __init__(self, expansion):
        """
        """
        self.context = expansion

        self.user = self.context.user
        self.group = self.context.group

        self.logger = self.context.logger

        envFile = DefaultValue.getEnv("MPPDB_ENV_SEPARATE_PATH")
        if envFile:
            self.envFile = envFile
        else:
            self.envFile = "/etc/profile"

        currentTime = str(datetime.datetime.now()).replace(" ", "_").replace(
            ".", "_")

        self.commonGsCtl = GsCtlCommon(expansion)
        self.tempFileDir = "/tmp/gs_expansion_%s" % (currentTime)
        self.logger.debug("tmp expansion dir is %s ." % self.tempFileDir)

        self._finalizer = weakref.finalize(self, self.clearTmpFile)

    def sendSoftToHosts(self):
        """
        create software dir and send it on each nodes
        """
        self.logger.debug("Start to send soft to each standby nodes.\n")
        hostNames = self.context.newHostList
        hostList = hostNames

        sshTool = SshTool(hostNames)

        srcFile = self.context.packagepath
        targetDir = os.path.realpath(
            os.path.join(srcFile, "../"))

        ## mkdir package dir and send package to remote nodes.
        sshTool.executeCommand("mkdir -p %s" % srcFile , "", DefaultValue.SUCCESS,
                       hostList)
        sshTool.scpFiles(srcFile, targetDir, hostList)

        ## change mode of package dir to set privileges for users
        tPathList = os.path.split(targetDir)
        path2ChangeMode = targetDir
        if len(tPathList) > 2:
            path2ChangeMode = os.path.join(tPathList[0],tPathList[1])
        changeModCmd =  "chmod -R a+x {srcFile}".format(user=self.user,
            group=self.group,srcFile=path2ChangeMode)
        sshTool.executeCommand(changeModCmd, "", DefaultValue.SUCCESS,
                       hostList)
        self.logger.debug("End to send soft to each standby nodes.\n")
        self.cleanSshToolFile(sshTool)

    def generateAndSendXmlFile(self):
        """
        """
        self.logger.debug("Start to generateAndSend XML file.\n")

        tempXmlFile = "%s/clusterconfig.xml" % self.tempFileDir
        cmd = "mkdir -p %s; touch %s; cat /dev/null > %s" % \
        (self.tempFileDir, tempXmlFile, tempXmlFile)
        (status, output) = subprocess.getstatusoutput(cmd)

        cmd = "chown -R %s:%s %s" % (self.user, self.group, self.tempFileDir)
        (status, output) = subprocess.getstatusoutput(cmd)
        
        newHosts = self.context.newHostList
        for host in newHosts:
            # create single deploy xml file for each standby node
            xmlContent = self.__generateXml(host)
            with os.fdopen(os.open("%s" % tempXmlFile, os.O_WRONLY | os.O_CREAT,
             stat.S_IWUSR | stat.S_IRUSR),'w') as fo:
                fo.write( xmlContent )
                fo.close()
            # send single deploy xml file to each standby node
            sshTool = SshTool(host)
            retmap, output = sshTool.getSshStatusOutput("mkdir -p %s" % 
            self.tempFileDir , [host], self.envFile)
            retmap, output = sshTool.getSshStatusOutput("chown %s:%s %s" % 
            (self.user, self.group, self.tempFileDir), [host], self.envFile)
            sshTool.scpFiles("%s" % tempXmlFile, "%s" % 
            tempXmlFile, [host], self.envFile)
            self.cleanSshToolFile(sshTool)
        
        self.logger.debug("End to generateAndSend XML file.\n")

    def __generateXml(self, backIp):
        """
        """
        nodeName = self.context.backIpNameMap[backIp]
        nodeInfo = self.context.clusterInfoDict[nodeName]

        backIp = nodeInfo["backIp"]
        sshIp = nodeInfo["sshIp"]
        port = nodeInfo["port"]
        dataNode = nodeInfo["dataNode"]

        appPath = self.context.clusterInfoDict["appPath"]
        logPath = self.context.clusterInfoDict["logPath"]
        corePath = self.context.clusterInfoDict["corePath"]
        toolPath = self.context.clusterInfoDict["toolPath"]
        tmpMppdbPath = DefaultValue.getEnv("PGHOST")
        if not tmpMppdbPath:
            tmpMppdbPath = toolPath

        xmlConfig = """\
<?xml version="1.0" encoding="UTF-8"?>
<ROOT>
    <CLUSTER>
        <PARAM name="clusterName" value="dbCluster" />
        <PARAM name="nodeNames" value="{nodeName}" />
        <PARAM name="backIp1s" value="{backIp}"/>
        <PARAM name="gaussdbAppPath" value="{appPath}" />
        <PARAM name="gaussdbLogPath" value="{logPath}" />
        <PARAM name="gaussdbToolPath" value="{toolPath}" />
        <PARAM name="tmpMppdbPath" value="{mppdbPath}" />
        <PARAM name="corePath" value="{corePath}"/>
        <PARAM name="clusterType" value="single-inst"/>
    </CLUSTER>
    <DEVICELIST>
        <DEVICE sn="1000001">
            <PARAM name="name" value="{nodeName}"/>
            <PARAM name="azName" value="{azName}"/>
            <PARAM name="azPriority" value="1"/>
            <PARAM name="backIp1" value="{backIp}"/>
            <PARAM name="sshIp1" value="{sshIp}"/>
            <!--dbnode-->
            <PARAM name="dataNum" value="1"/>
            <PARAM name="dataPortBase" value="{port}"/>
            <PARAM name="dataNode1" value="{dataNode}"/>
        </DEVICE>
    </DEVICELIST>
</ROOT>
        """.format(nodeName=nodeName,backIp=backIp,appPath=appPath,
        logPath=logPath,toolPath=toolPath,corePath=corePath,
        sshIp=sshIp,port=port,dataNode=dataNode,azName=self.context.azName,
        mppdbPath=tmpMppdbPath)
        return xmlConfig

    def changeUser(self):
        user = self.user
        try:
            pw_record = pwd.getpwnam(user)
        except Exception:
            GaussLog.exitWithError(ErrorCode.GAUSS_503["GAUSS_50300"] % user)

        user_name = pw_record.pw_name
        user_uid       = pw_record.pw_uid
        user_gid       = pw_record.pw_gid
        env = os.environ.copy()
        os.setgid(user_gid)
        os.setuid(user_uid)

    def initSshConnect(self, host, user='root'):
        
        try:
            getPwdStr = "Please enter the password of user [%s] on node [%s]: " \
             % (user, host)
            passwd = getpass.getpass(getPwdStr)
            self.sshClient = paramiko.SSHClient()
            self.sshClient.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.sshClient.connect(host, 22, user, passwd)
        except paramiko.ssh_exception.AuthenticationException as e :
            self.logger.log("Authentication failed.")
            self.initSshConnect(host, user)

    def installDatabaseOnHosts(self):
        """
        install database on each standby node
        """
        hostList = self.context.newHostList
        envfile = self.envFile
        tempXmlFile = "%s/clusterconfig.xml" % self.tempFileDir
        installCmd = "source {envfile} ; gs_install -X {xmlfile} \
            2>&1".format(envfile=envfile,xmlfile=tempXmlFile)

        statusArr = []

        for newHost in hostList:

            self.logger.log("\ninstalling database on node %s:" % newHost)
            self.logger.debug(installCmd)

            hostName = self.context.backIpNameMap[newHost]
            sshIp = self.context.clusterInfoDict[hostName]["sshIp"]
            self.initSshConnect(sshIp, self.user)

            stdin, stdout, stderr = self.sshClient.exec_command(installCmd, 
            get_pty=True)
            channel = stdout.channel
            echannel = stderr.channel

            while not channel.exit_status_ready():
                try:
                    recvOut = channel.recv(1024)
                    outDecode = recvOut.decode("utf-8");
                    outStr = outDecode.strip()
                    if(len(outStr) == 0):
                        continue
                    if(outDecode.endswith("\r\n")):
                        self.logger.log(outStr)
                    else:
                        value = ""
                        if re.match(r".*yes.*no.*", outStr):
                            value = input(outStr)
                            while True:
                                # check the input
                                if (
                                    value.upper() != "YES"
                                    and value.upper() != "NO"
                                    and value.upper() != "Y"
                                    and value.upper() != "N"):
                                    value = input("Please type 'yes' or 'no': ")
                                    continue
                                break
                        else:
                            value = getpass.getpass(outStr)
                        stdin.channel.send("%s\r\n" %value)
                        stdin.flush()
                    stdout.flush()
                except Exception as e:
                    sys.exit(1)
                    pass
                if channel.exit_status_ready() and  \
                    not channel.recv_stderr_ready() and \
                    not channel.recv_ready(): 
                    channel.close()
                    break
            
            stdout.close()
            stderr.close()
            status = channel.recv_exit_status()
            statusArr.append(status)
        
        isBothSuccess = True
        for status in statusArr:
            if status != 0:
                isBothSuccess = False
                break
        if isBothSuccess:
            self.logger.log("\nSuccessfully install database on node %s" %
             hostList)
        else:
            sys.exit(1)
    
    def preInstallOnHosts(self):
        """
        execute preinstall step
        """
        self.logger.debug("Start to preinstall database step.\n")
        newBackIps = self.context.newHostList
        newHostNames = []
        for host in newBackIps:
            newHostNames.append(self.context.backIpNameMap[host])
        envfile = self.envFile
        tempXmlFile = "%s/clusterconfig.xml" % self.tempFileDir

        if envfile == "/etc/profile":
            preinstallCmd = "{softpath}/script/gs_preinstall -U {user} -G {group} \
            -X {xmlfile} --non-interactive 2>&1\
                    ".format(softpath=self.context.packagepath,user=self.user,
                    group=self.group,xmlfile=tempXmlFile)
        else:
            preinstallCmd = "{softpath}/script/gs_preinstall -U {user} -G {group} \
                -X {xmlfile} --sep-env-file={envfile} \
                --non-interactive 2>&1\
                    ".format(softpath=self.context.packagepath,user=self.user,
                    group=self.group,xmlfile=tempXmlFile,envfile=envfile)

        sshTool = SshTool(newHostNames)
        
        status, output = sshTool.getSshStatusOutput(preinstallCmd , [], envfile)
        statusValues = status.values()
        if STATUS_FAIL in statusValues:
            GaussLog.exitWithError(output)
        
        self.logger.debug("End to preinstall database step.\n")
        self.cleanSshToolFile(sshTool)

    
    def buildStandbyRelation(self):
        """
        func: after install single database on standby nodes. 
        build the relation with primary and standby nodes.
        step:
        1. restart primary node with Primary Mode 
        (only used to Single-Node instance)
        2. set guc config to primary node
        3. restart standby node with Standby Mode
        4. set guc config to standby node
        5. generate cluster static file and send to each node.
        """
        self.queryPrimaryClusterDetail()
        self.setPrimaryGUCConfig()
        self.setStandbyGUCConfig()
        self.addTrustOnExistNodes()
        self.restartSingleDbWithPrimaryMode()
        self.buildStandbyHosts()
        self.generateClusterStaticFile()

    def queryPrimaryClusterDetail(self):
        """
        get current cluster type. 
        single-node or primary-standby
        """
        self.logger.debug("Query primary database instance mode.\n")
        self.isSingleNodeInstance = True
        primaryHost = self.getPrimaryHostName()
        result = self.commonGsCtl.queryOmCluster(primaryHost, self.envFile)
        instance = re.findall(r"node\s+node_ip\s+instance\s+state", result)
        if len(instance) > 1:
            self.isSingleNodeInstance = False
        self.logger.debug("Original instance mode is %s" % 
        self.isSingleNodeInstance)

    def setPrimaryGUCConfig(self):
        """
        """
        self.logger.debug("Start to set primary node GUC config.\n")
        primaryHost = self.getPrimaryHostName()

        self.setGUCOnClusterHosts([primaryHost])
        self.addStandbyIpInPrimaryConf()
        
        
    def setStandbyGUCConfig(self):
        """
        set the expansion standby node db guc config
        """
        self.logger.debug("Stat to set standby node GUC config.\n")
        nodeList = self.context.nodeNameList
        primaryHost = self.getPrimaryHostName()
        standbyHostNames = list(set(nodeList).difference(set([primaryHost])))
        self.setGUCOnClusterHosts(standbyHostNames)

    def addTrustOnExistNodes(self):
        """
        add host trust in pg_hba.conf on existing standby node. 
        """ 
        self.logger.debug("Start to set host trust on existing node.")
        allNodeNames = self.context.nodeNameList
        newNodeIps = self.context.newHostList
        newNodeNames = []
        trustCmd = []
        for node in newNodeIps:
            nodeName = self.context.backIpNameMap[node]
            newNodeNames.append(nodeName)
            cmd = 'host    all    all    %s/32    trust' % node
            trustCmd.append(cmd)
        existNodes = list(set(allNodeNames).difference(set(newNodeNames)))
        for node in existNodes:
            dataNode = self.context.clusterInfoDict[node]["dataNode"]
            cmd = ""
            for trust in trustCmd:
                cmd += "source %s; gs_guc set -D %s -h '%s';" % \
                    (self.envFile, dataNode, trust)
            sshTool = SshTool([node])
            resultMap, outputCollect = sshTool.getSshStatusOutput(cmd, 
            [node], self.envFile)
            self.cleanSshToolFile(sshTool)
        self.logger.debug("End to set host trust on existing node.")
    
    def restartSingleDbWithPrimaryMode(self):
        """
        """
        primaryHost = self.getPrimaryHostName()
        dataNode = self.context.clusterInfoDict[primaryHost]["dataNode"]

        insType, dbStat = self.commonGsCtl.queryInstanceStatus(primaryHost,
        dataNode,self.envFile)
        if insType != MODE_PRIMARY:
            self.commonGsCtl.stopInstance(primaryHost, dataNode, self.envFile)
            self.commonGsCtl.startInstanceWithMode(primaryHost, dataNode,
            MODE_PRIMARY,self.envFile)
        
        # start db to primary state for three times max
        start_retry_num = 1
        while start_retry_num <= 3:
            insType, dbStat = self.commonGsCtl.queryInstanceStatus(primaryHost,
            dataNode, self.envFile)
            if insType == MODE_PRIMARY:
                break
            self.logger.debug("Start database as Primary mode failed, \
retry for %s times" % start_retry_num)
            self.commonGsCtl.startInstanceWithMode(primaryHost, dataNode,
            MODE_PRIMARY, self.envFile)
            start_retry_num = start_retry_num + 1

    def addStandbyIpInPrimaryConf(self):
        """
        add standby hosts ip in primary node pg_hba.conf
        """

        standbyHosts = self.context.newHostList
        primaryHost = self.getPrimaryHostName()
        command = ''
        for host in standbyHosts:
            hostName = self.context.backIpNameMap[host]
            dataNode = self.context.clusterInfoDict[hostName]["dataNode"]
            command += ("source %s; gs_guc set -D %s -h 'host    all    all    %s/32    " + \
                "trust';") % (self.envFile, dataNode, host)
        self.logger.debug(command)
        sshTool = SshTool([primaryHost])
        resultMap, outputCollect = sshTool.getSshStatusOutput(command, 
        [primaryHost], self.envFile)
        self.logger.debug(outputCollect)
        self.cleanSshToolFile(sshTool)

    def reloadPrimaryConf(self):
        """
        """
        primaryHost = self.getPrimaryHostName()
        dataNode = self.context.clusterInfoDict[primaryHost]["dataNode"]
        command = "gs_ctl reload -D %s " % dataNode
        sshTool = SshTool([primaryHost])
        self.logger.debug(command)
        resultMap, outputCollect = sshTool.getSshStatusOutput(command, 
        [primaryHost], self.envFile)
        self.logger.debug(outputCollect)
        self.cleanSshToolFile(sshTool)

    def getPrimaryHostName(self):
        """
        """
        primaryHost = ""
        for nodeName in self.context.nodeNameList:
            if self.context.clusterInfoDict[nodeName]["instanceType"] \
                == MASTER_INSTANCE:
                primaryHost = nodeName
                break
        return primaryHost


    def buildStandbyHosts(self):
        """
        stop the new standby host`s database and build it as standby mode
        """
        self.logger.debug("start to build standby node...\n")
        
        standbyHosts = self.context.newHostList

        for host in standbyHosts:
            hostName = self.context.backIpNameMap[host]
            dataNode = self.context.clusterInfoDict[hostName]["dataNode"]

            self.checkTmpDir(hostName)

            self.commonGsCtl.stopInstance(hostName, dataNode, self.envFile)
            self.commonGsCtl.startInstanceWithMode(hostName, dataNode, 
            MODE_STANDBY, self.envFile)
            
            # start standby as standby mode for three times max.
            start_retry_num = 1
            while start_retry_num <= 3:
                insType, dbStat = self.commonGsCtl.queryInstanceStatus(hostName,
                 dataNode, self.envFile)
                if insType != MODE_STANDBY:
                    self.logger.debug("Start database as Standby mode failed, \
retry for %s times" % start_retry_num)
                    self.commonGsCtl.startInstanceWithMode(hostName, dataNode, 
                    MODE_STANDBY, self.envFile)
                    start_retry_num = start_retry_num + 1
                else:
                    break

            # build standby node
            self.addStandbyIpInPrimaryConf()
            self.reloadPrimaryConf()
            self.commonGsCtl.buildInstance(hostName, dataNode, MODE_STANDBY, 
            self.envFile)

            # if build failed first time. retry for three times.
            start_retry_num = 1
            while start_retry_num <= 3:
                insType, dbStat = self.commonGsCtl.queryInstanceStatus(hostName,
                 dataNode, self.envFile)
                if dbStat != STAT_NORMAL:
                    self.logger.debug("Build standby instance failed, \
retry for %s times" % start_retry_num)
                    self.addStandbyIpInPrimaryConf()
                    self.reloadPrimaryConf()
                    self.commonGsCtl.buildInstance(hostName, dataNode, 
                    MODE_STANDBY, self.envFile)
                    start_retry_num = start_retry_num + 1
                else:
                    break
                
    def checkTmpDir(self, hostName):
        """
        if the tmp dir id not exist, create it.
        """
        tmpDir = os.path.realpath(DefaultValue.getTmpDirFromEnv())
        checkCmd = 'if [ ! -d "%s" ]; then exit 1;fi;' % (tmpDir)
        sshTool = SshTool([hostName])
        resultMap, outputCollect = sshTool.getSshStatusOutput(checkCmd, 
        [hostName], self.envFile)
        ret = resultMap[hostName]
        if ret == STATUS_FAIL:
            self.logger.debug("Node [%s] does not have tmp dir. need to fix.")
            fixCmd = "mkdir -p %s" % (tmpDir)
            sshTool.getSshStatusOutput(fixCmd, [hostName], self.envFile)
        self.cleanSshToolFile(sshTool)

    def generateClusterStaticFile(self):
        """
        generate static_config_files and send to all hosts
        """
        self.logger.debug("Start to generate and send cluster static file.\n")

        primaryHosts = self.getPrimaryHostName()
        command = "gs_om -t generateconf -X %s --distribute" % self.context.xmlFile
        sshTool = SshTool([primaryHosts])
        resultMap, outputCollect = sshTool.getSshStatusOutput(command, 
        [primaryHosts], self.envFile)
        self.logger.debug(outputCollect)
        self.cleanSshToolFile(sshTool)

        nodeNameList = self.context.nodeNameList

        for hostName in nodeNameList:
            hostSsh = SshTool([hostName])
            toolPath = self.context.clusterInfoDict["toolPath"]
            appPath = self.context.clusterInfoDict["appPath"]
            srcFile = "%s/script/static_config_files/cluster_static_config_%s" \
                % (toolPath, hostName)
            if not os.path.exists(srcFile):
                GaussLog.exitWithError("Generate static file [%s] not found." \
                    % srcFile)
            targetFile = "%s/bin/cluster_static_config" % appPath
            hostSsh.scpFiles(srcFile, targetFile, [hostName], self.envFile)
            self.cleanSshToolFile(hostSsh)

        self.logger.debug("End to generate and send cluster static file.\n")
        time.sleep(10)

        # Single-node database need start cluster after expansion
        if self.isSingleNodeInstance:
            self.logger.debug("Single-Node instance need restart.\n")
            self.commonGsCtl.queryOmCluster(primaryHosts, self.envFile)

            # if primary database not normal, restart it
            primaryHost = self.getPrimaryHostName()
            dataNode = self.context.clusterInfoDict[primaryHost]["dataNode"]
            insType, dbStat = self.commonGsCtl.queryInstanceStatus(primaryHost,
            dataNode, self.envFile)
            if insType != MODE_PRIMARY:
                self.commonGsCtl.startInstanceWithMode(primaryHost, dataNode, 
                MODE_PRIMARY, self.envFile)
            # if stat if not normal,rebuild standby database
            standbyHosts = self.context.newHostList
            for host in standbyHosts:
                hostName = self.context.backIpNameMap[host]
                dataNode = self.context.clusterInfoDict[hostName]["dataNode"]
                insType, dbStat = self.commonGsCtl.queryInstanceStatus(hostName,
                dataNode, self.envFile)
                if dbStat != STAT_NORMAL:
                    self.commonGsCtl.startInstanceWithMode(hostName, dataNode, 
                    MODE_STANDBY, self.envFile)

            self.commonGsCtl.startOmCluster(primaryHosts, self.envFile)

    def setGUCOnClusterHosts(self, hostNames=[]):
        """
        guc config on all hosts 
        """

        gucDict = self.getGUCConfig()
        
        tempShFile = "%s/guc.sh" % self.tempFileDir

        if len(hostNames) == 0:
            hostNames = self.context.nodeNameList

        for host in hostNames:
            
            command = "source %s ; " % self.envFile + gucDict[host]

            self.logger.debug(command)

            sshTool = SshTool([host])

            # create temporary dir to save guc command bashfile.
            mkdirCmd = "mkdir -m a+x -p %s; chown %s:%s %s" % \
                (self.tempFileDir,self.user,self.group,self.tempFileDir)
            retmap, output = sshTool.getSshStatusOutput(mkdirCmd, [host], self.envFile)

            subprocess.getstatusoutput("mkdir -m a+x -p %s; touch %s; \
                cat /dev/null > %s" % \
                    (self.tempFileDir, tempShFile, tempShFile))
            with os.fdopen(os.open("%s" % tempShFile, os.O_WRONLY | os.O_CREAT,
             stat.S_IWUSR | stat.S_IRUSR),'w') as fo:
                fo.write("#bash\n")
                fo.write( command )
                fo.close()

            # send guc command bashfile to each host and execute it.
            sshTool.scpFiles("%s" % tempShFile, "%s" % tempShFile, [host], 
            self.envFile)
            
            resultMap, outputCollect = sshTool.getSshStatusOutput("sh %s" % \
                tempShFile, [host], self.envFile)

            self.logger.debug(outputCollect)
            self.cleanSshToolFile(sshTool)

    def getGUCConfig(self):
        """
        get guc config of each node:
            replconninfo[index]
            remote_read_mode
            replication_type
        """
        nodeDict = self.context.clusterInfoDict
        hostNames = self.context.nodeNameList
        
        gucDict = {}

        for hostName in hostNames:
            
            localeHostInfo = nodeDict[hostName]
            index = 1
            guc_tempate_str = "source %s; " % self.envFile
            for remoteHost in hostNames:
                if(remoteHost == hostName):
                    continue
                remoteHostInfo = nodeDict[remoteHost]
      
                guc_repl_template = """\
gs_guc set -D {dn} -c "replconninfo{index}=\
'localhost={localhost} localport={localport} \
localheartbeatport={localeHeartPort} \
localservice={localservice} \
remotehost={remoteNode} \
remoteport={remotePort} \
remoteheartbeatport={remoteHeartPort} \
remoteservice={remoteservice}'"
                """.format(dn=localeHostInfo["dataNode"],
                index=index,
                localhost=localeHostInfo["sshIp"],
                localport=localeHostInfo["localport"],
                localeHeartPort=localeHostInfo["heartBeatPort"],
                localservice=localeHostInfo["localservice"],
                remoteNode=remoteHostInfo["sshIp"],
                remotePort=remoteHostInfo["localport"],
                remoteHeartPort=remoteHostInfo["heartBeatPort"],
                remoteservice=remoteHostInfo["localservice"])

                guc_tempate_str += guc_repl_template

                index += 1

            guc_mode_type = """
            gs_guc set -D {dn} -c 'remote_read_mode=off';
            gs_guc set -D {dn} -c 'replication_type=1';
            """.format(dn=localeHostInfo["dataNode"])
            guc_tempate_str += guc_mode_type

            gucDict[hostName] = guc_tempate_str
        return gucDict

    def checkLocalModeOnStandbyHosts(self):
        """
        expansion the installed standby node. check standby database.
        1. if the database is normal
        2. if the databases version are same before existing and new 
        """
        standbyHosts = self.context.newHostList
        envfile = self.envFile
        
        self.logger.log("Checking the database with locale mode.")
        for host in standbyHosts:
            hostName = self.context.backIpNameMap[host]
            dataNode = self.context.clusterInfoDict[hostName]["dataNode"]
            insType, dbStat = self.commonGsCtl.queryInstanceStatus(hostName, 
            dataNode, self.envFile)
            if insType not in (MODE_PRIMARY, MODE_STANDBY, MODE_NORMAL):
                GaussLog.exitWithError(ErrorCode.GAUSS_357["GAUSS_35703"] % 
                (hostName, self.user, dataNode, dataNode))
        
        allHostIp = []
        allHostIp.append(self.context.localIp)
        versionDic = {}

        for hostip in standbyHosts:
            allHostIp.append(hostip)
        sshTool= SshTool(allHostIp)
        #get version in the nodes 
        getversioncmd = "gaussdb --version"
        resultMap, outputCollect = sshTool.getSshStatusOutput(getversioncmd,
                                                               [], envfile)
        self.cleanSshToolFile(sshTool)
        versionLines = outputCollect.splitlines()
        for verline in versionLines:
            if verline[0:9] == '[SUCCESS]':
               ipKey = verline[10:-1]
               continue
            else:
                versionStr = "".join(verline)
                preVersion = versionStr.split(' ')
                versionInfo = preVersion[4]
                versionDic[ipKey] = versionInfo[:-2]
        for hostip in versionDic:
            if hostip == self.context.localIp:
               versionCompare = ""
               versionCompare = versionDic[hostip]
            else:
                if versionDic[hostip] == versionCompare:
                    continue
                else:
                    GaussLog.exitWithError(ErrorCode.GAUSS_357["GAUSS_35705"] \
                       %(hostip, versionDic[hostip]))
        
        self.logger.log("Successfully checked the database with locale mode.")

    def preInstall(self):
        """
        preinstall on new hosts.
        """
        self.logger.log("Start to preinstall database on the new \
standby nodes.")
        self.sendSoftToHosts()
        self.generateAndSendXmlFile()
        self.preInstallOnHosts()
        self.logger.log("Successfully preinstall database on the new \
standby nodes.")


    def clearTmpFile(self):
        """
        clear temporary file after expansion success
        """
        self.logger.debug("start to delete temporary file %s" % self.tempFileDir)
        clearCmd = "if [ -d '%s' ];then rm -rf %s;fi" % \
            (self.tempFileDir, self.tempFileDir)
        hostNames = self.context.nodeNameList
        for host in hostNames:
            try:
                sshTool = SshTool(hostNames)
                result, output = sshTool.getSshStatusOutput(clearCmd, 
                hostNames, self.envFile)
                self.logger.debug(output)
                self.cleanSshToolFile(sshTool)
            except Exception as e:
                self.logger.debug(str(e))
                self.cleanSshToolFile(sshTool)
        

    def cleanSshToolFile(self, sshTool):
        """
        """
        try:
            sshTool.clenSshResultFiles()
        except Exception as e:
            self.logger.debug(str(e))

    
    def checkNodesDetail(self):
        """
        """
        self.checkUserAndGroupExists()
        self.checkXmlFileAccessToUser()
    
    def checkXmlFileAccessToUser(self):
        """
        Check if the xml config file has readable access to user.
        """
        userInfo = pwd.getpwnam(self.user)
        uid = userInfo.pw_uid
        gid = userInfo.pw_gid

        xmlFile = self.context.xmlFile
        fstat = os.stat(xmlFile)
        mode = fstat[stat.ST_MODE]
        if (fstat[stat.ST_UID] == uid and (mode & stat.S_IRUSR > 0)) or \
           (fstat[stat.ST_GID] == gid and (mode & stat.S_IRGRP > 0)):
            pass
        else:
            self.logger.debug("User %s has no access right for file %s" \
                 % (self.user, xmlFile))
            os.chown(xmlFile, uid, gid)
            os.chmod(xmlFile, stat.S_IRUSR)

    def checkUserAndGroupExists(self):
        """
        check system user and group exists and be same 
        on primary and standby nodes
        """
        inputUser = self.user
        inputGroup = self.group
        
        user_group_id = ""
        isUserExits = False
        localHost = socket.gethostname()
        for user in pwd.getpwall():
            if user.pw_name == self.user:
                user_group_id = user.pw_gid
                isUserExits = True
                break
        if not isUserExits:
            GaussLog.exitWithError(ErrorCode.GAUSS_357["GAUSS_35704"] \
                % ("User", self.user, localHost))

        isGroupExits = False
        group_id = ""
        for group in grp.getgrall():
            if group.gr_name == self.group:
                group_id = group.gr_gid
                isGroupExits = True
        if not isGroupExits:
            GaussLog.exitWithError(ErrorCode.GAUSS_357["GAUSS_35704"] \
                % ("Group", self.group, localHost))
        if user_group_id != group_id:
            GaussLog.exitWithError("User [%s] is not in the group [%s]."\
                 % (self.user, self.group))
        
        hostNames = self.context.newHostList
        envfile = self.envFile
        sshTool = SshTool(hostNames)

        #get username in the other standy nodes 
        getUserNameCmd = "cat /etc/passwd | grep -w %s" % inputUser
        resultMap, outputCollect = sshTool.getSshStatusOutput(getUserNameCmd, 
        [], envfile)
        
        for hostKey in resultMap:
            if resultMap[hostKey] == STATUS_FAIL:
                self.cleanSshToolFile(sshTool)
                GaussLog.exitWithError(ErrorCode.GAUSS_357["GAUSS_35704"] \
                       % ("User", self.user, hostKey))
        
        #get groupname in the other standy nodes   
        getGroupNameCmd = "cat /etc/group | grep -w %s" % inputGroup
        resultMap, outputCollect = sshTool.getSshStatusOutput(getGroupNameCmd, 
        [], envfile)
        for hostKey in resultMap:
            if resultMap[hostKey] == STATUS_FAIL:
                self.cleanSshToolFile(sshTool)
                GaussLog.exitWithError(ErrorCode.GAUSS_357["GAUSS_35704"] \
                       % ("Group", self.group, hostKey))
        self.cleanSshToolFile(sshTool)

    
    def installAndExpansion(self):
        """
        install database and expansion standby node with db om user
        """
        pvalue = Value('i', 0)
        proc = Process(target=self.installProcess, args=(pvalue,)) 
        proc.start()
        proc.join()
        if not pvalue.value:
            sys.exit(1)
        else:
            proc.terminate()

    def installProcess(self, pvalue):
        # change to db manager user. the below steps run with db manager user.
        self.changeUser()

        if not self.context.standbyLocalMode:
            self.logger.log("\nStart to install database on the new \
standby nodes.")
            self.installDatabaseOnHosts()
        else:
            self.logger.log("\nStandby nodes is installed with locale mode.")
            self.checkLocalModeOnStandbyHosts()

        self.logger.log("\nDatabase on standby nodes installed finished. \
Start to establish the primary-standby relationship.") 
        self.buildStandbyRelation()
        # process success
        pvalue.value = 1

    def run(self):
        """
        start expansion
        """
        self.checkNodesDetail()
        # preinstall on standby nodes with root user.
        if not self.context.standbyLocalMode:
            self.preInstall()

        self.installAndExpansion()

        self.logger.log("\nSuccess to expansion standby nodes.")


class GsCtlCommon:

    def __init__(self, expansion):
        """
        """
        self.logger = expansion.logger
        self.user = expansion.user
    
    def queryInstanceStatus(self, host, datanode, env):
        """
        """
        command = "source %s ; gs_ctl query -D %s" % (env, datanode)
        sshTool = SshTool([datanode])
        resultMap, outputCollect = sshTool.getSshStatusOutput(command, 
        [host], env)
        self.logger.debug(outputCollect)
        localRole = re.findall(r"local_role.*: (.*?)\n", outputCollect)
        db_state = re.findall(r"db_state.*: (.*?)\n", outputCollect)

        insType = ""

        if(len(localRole)) == 0:
            insType = ""
        else:
            insType = localRole[0]
        
        dbStatus = ""
        if(len(db_state)) == 0:
            dbStatus = ""
        else:
            dbStatus = db_state[0]
        self.cleanSshToolTmpFile(sshTool)
        return insType.strip().lower(), dbStatus.strip().lower()

    def stopInstance(self, host, datanode, env):
        """
        """
        command = "source %s ; gs_ctl stop -D %s" % (env, datanode)
        sshTool = SshTool([host])
        resultMap, outputCollect = sshTool.getSshStatusOutput(command, 
        [host], env)
        self.logger.debug(host)
        self.logger.debug(outputCollect)
        self.cleanSshToolTmpFile(sshTool)
    
    def startInstanceWithMode(self, host, datanode, mode, env):
        """
        """
        command = "source %s ; gs_ctl start -D %s -M %s" % (env, datanode, mode)
        self.logger.debug(command)
        sshTool = SshTool([host])
        resultMap, outputCollect = sshTool.getSshStatusOutput(command, 
        [host], env)
        self.logger.debug(host)
        self.logger.debug(outputCollect)
        self.cleanSshToolTmpFile(sshTool)

    def buildInstance(self, host, datanode, mode, env):
        command = "source %s ; gs_ctl build -D %s -M %s" % (env, datanode, mode)
        self.logger.debug(command)
        sshTool = SshTool([host])
        resultMap, outputCollect = sshTool.getSshStatusOutput(command, 
        [host], env)
        self.logger.debug(host)
        self.logger.debug(outputCollect)
        self.cleanSshToolTmpFile(sshTool)

    def startOmCluster(self, host, env):
        """
        om tool start cluster
        """
        command = "source %s ; gs_om -t start" % env
        self.logger.debug(command)
        sshTool = SshTool([host])
        resultMap, outputCollect = sshTool.getSshStatusOutput(command, 
        [host], env)
        self.logger.debug(host)
        self.logger.debug(outputCollect)
        self.cleanSshToolTmpFile(sshTool)
    
    def queryOmCluster(self, host, env):
        """
        query om cluster detail with command:
        gs_om -t status --detail
        """
        command = "source %s ; gs_om -t status --detail" % env
        sshTool = SshTool([host])
        resultMap, outputCollect = sshTool.getSshStatusOutput(command, 
        [host], env)
        self.logger.debug(host)
        self.logger.debug(outputCollect)
        if resultMap[host] == STATUS_FAIL:
            GaussLog.exitWithError("Query cluster failed. Please check " \
                "the cluster status or " \
                "source the environmental variables of user [%s]." % self.user)
        self.cleanSshToolTmpFile(sshTool)
        return outputCollect

    def cleanSshToolTmpFile(self, sshTool):
        """
        """
        try:
            sshTool.clenSshResultFiles()
        except Exception as e:
            self.logger.debug(str(e))



