# coding: UTF-8
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
import time
import subprocess
import time
import base64
import json

sys.path.append(sys.path[0] + "/../../../")
from gspylib.common.Common import DefaultValue
from gspylib.common.OMCommand import OMCommand
from gspylib.common.ErrorCode import ErrorCode
from gspylib.os.gsfile import g_file
from gspylib.os.gsplatform import g_Platform
from gspylib.os.gsOSlib import g_OSlib
from impl.collect.CollectImpl import CollectImpl


class CollectImplOLAP(CollectImpl):
    """
    The class is used to do perform collect log files.
    """

    def __init__(self, collectObj):
        """
        function: Constructor
        input : collectObj
        output: NA
        """
        self.jobInfo = {}
        self.nodeJobInfo = {}
        super(CollectImplOLAP, self).__init__(collectObj)

    def parseConfigFile(self):
        """
        function: Parsing configuration files
        input : NA
        output: NA
        """
        try:
            # Init the cluster information
            self.context.initClusterInfoFromStaticFile(self.context.user)
            self.context.appPath = self.context.clusterInfo.appPath

            # Obtain the cluster installation directory owner and group
            (self.context.user, self.context.group) = g_OSlib.getPathOwner(
                self.context.appPath)
            if (self.context.user == "" or self.context.group == ""):
                self.context.logger.logExit(ErrorCode.GAUSS_503["GAUSS_50308"])

            # Match the corresponding node
            for nodename in self.context.nodeName:
                if not self.context.clusterInfo.getDbNodeByName(nodename):
                    self.context.logger.logExit(
                        ErrorCode.GAUSS_516["GAUSS_51619"] % nodename)

            if (len(self.context.nodeName) == 0):
                self.context.nodeName = \
                    self.context.clusterInfo.getClusterNodeNames()

            self.context.initSshTool(self.context.nodeName,
                                     DefaultValue.TIMEOUT_PSSH_COLLECTOR)
            if (len(self.context.nodeName) == 1 and self.context.nodeName[
                0] == DefaultValue.GetHostIpOrName()):
                self.context.localMode = True
        except Exception as e:
            raise Exception(str(e))
        self.context.logger.log("Successfully parsed the configuration file.")

    # python will remove Single quotes
    # and double quote when we pass parameter from outside
    # we use # to replace double quote
    def formatJsonString(self, check):
        """
        function: format sonString
        input : string
        output: json string
        """
        if (self.context.isSingle or self.context.localMode):
            return "\'" + json.dumps(check).replace("\"", "#") + "\'"
        else:
            return "\'" \
                   + \
                   json.dumps(check).replace("$", "\$").replace("\"", "#") \
                   + "\'"

    def checkTmpDir(self):
        """
        function: Check tmp dir, if tmp dir not exist, create it
        input : TmpDirFromEnv
        output: NA
        """
        try:
            # Create a temporary file
            tmpDir = DefaultValue.getTmpDirFromEnv()
            cmd = "(if [ ! -d '%s' ];then mkdir -p '%s' -m %s;fi)" \
                  % (tmpDir, tmpDir, DefaultValue.KEY_DIRECTORY_MODE)
            DefaultValue.execCommandWithMode(
                cmd,
                "Check temporary directory",
                self.context.sshTool,
                self.context.isSingle or self.context.localMode,
                self.context.mpprcFile)
        except Exception as e:
            self.context.logger.logExit(str(e))

    def checkCommand(self):
        """
        function: check command
        output: Successfully command exists
        """
        self.context.logger.log("check rsync command.")
        # Check the system information on each node
        cmd = "source %s; %s -t check_command -U %s -S %d -l %s" % (
            self.context.mpprcFile,
            OMCommand.getLocalScript("Local_Collect"),
            self.context.user,
            self.context.speedLimitFlag,
            self.context.localLog)
        flag = 0
        failedNodeList = []
        if (self.context.isSingle or self.context.localMode):
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                flag = 1
                self.context.logger.log("The cmd is %s " % cmd)
                self.context.logger.logExit(
                    "rsync command not found on %s. "
                    "Error:\n%s\n--speed-limit parameters cannot be used" % \
                    (self.context.nodeName[0], output))
        else:
            (status, output) = self.context.sshTool.getSshStatusOutput(
                cmd, self.context.nodeName)
            self.context.sshTool.parseSshOutput(self.context.nodeName)
            # Gets the execution result
            for node in self.context.nodeName:
                if (status[node] != DefaultValue.SUCCESS):
                    flag = 1
                    failedNodeList.append(node)
        if flag == 0:
            self.context.logger.log("Successfully check rsync command.")
        else:
            self.context.logger.logExit(
                "rsync command not found on hosts: %s.\n "
                "--speed-limit parameters cannot be used "
                % str(failedNodeList))

    def createStoreDir(self):
        """
        :return:
        """
        resultdir = ""
        # Gets the current time
        currentTime = time.strftime("%Y%m%d_%H%M%S")
        if (self.context.outFile is not None and self.context.outFile != ""):
            # rm the tmpdir
            resultdir = self.context.outFile
        else:
            # rm the tmpdir
            resultdir = DefaultValue.getTmpDirFromEnv()

        cmd = \
            "if [ -d '%s'/collector_tmp_* ];" \
            "then rm -rf '%s'/collector_tmp_*; fi" % (
                resultdir, resultdir)
        (status, output) = DefaultValue.retryGetstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                            + "Error:\n%s" % output)
        # Get the default path
        targetdir = "%s/collector_tmp_%s" % (resultdir, currentTime)
        self.context.outFile = "%s/collector_%s" % (targetdir, currentTime)
        # Create a folder to store log information
        g_file.createDirectory(self.context.outFile)
        g_file.changeMode(DefaultValue.KEY_DIRECTORY_MODE,
                          self.context.outFile, True)
        return (currentTime, targetdir, resultdir)

    def createDir(self):
        """
        function: create Dir
        output: Successfully create dir
        """
        self.context.logger.log("create Dir.")
        # Check the system information on each node
        cmd = "source %s; %s -t create_dir -U %s -l %s" % (
            self.context.mpprcFile,
            OMCommand.getLocalScript("Local_Collect"),
            self.context.user,
            self.context.localLog)
        flag = 0
        if (self.context.isSingle or self.context.localMode):
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                flag = 1
                self.context.logger.log("The cmd is %s " % cmd)
                self.context.logger.log(
                    "Failed to create dir on %s. Error:\n%s" % \
                    (self.context.nodeName[0], output))
        else:
            (status, output) = self.context.sshTool.getSshStatusOutput(
                cmd,
                self.context.nodeName)
            outputMap = self.context.sshTool.parseSshOutput(
                self.context.nodeName)
            # Gets the execution result
            for node in self.context.nodeName:
                if (status[node] != DefaultValue.SUCCESS):
                    flag = 1
                    self.context.logger.log(
                        "Failed to create dir on %s. Error:\n%s" % \
                        (node, str(outputMap[node])))
        if (flag == 0):
            self.context.logger.log("Successfully create dir.")

    def printSummaryInfo(self, resultdir, currentTime):
        maxNamelen = len("SUCCESS HOSTNAME")
        maxJoblen = len("success")
        jobCount = 0
        jobNameList = []
        tag = ""
        info = ""

        for host in self.context.nodeName:
            maxNamelen = max(maxNamelen, len(host))
        for jobName, jobInfo in self.jobInfo.items():
            subJobCount = len(jobInfo)
            jobCount += subJobCount
            while subJobCount > 0:
                job = "%s-%s" % (jobName, str(subJobCount))
                maxJoblen = max(maxJoblen, len(job))
                jobNameList.append(job)
                subJobCount -= 1
        maxJoblen = maxJoblen + 4
        maxNamelen = maxNamelen + 4

        title = "%s%s%s%s%s%s%s" % ("|", "TASK NAME".center(maxJoblen), "|",
                                    "SUCCESS HOSTNAME".center(maxNamelen), "|",
                                    "FAILED HOSTNAME".center(maxNamelen), "|")
        index = len(title)
        while index > 0:
            tag = "%s%s" % (tag, "-")
            index -= 1
        info = "%s%s%s" % (info, tag, "\n")
        info = "%s%s%s%s%s%s%s%s" % (
            info, "|", "".center(maxJoblen), "|", "".center(maxNamelen), "|",
            "".center(maxNamelen), "|\n")
        info = "%s%s%s" % (info, title, "\n")
        info = "%s%s%s%s%s%s%s%s" % (
            info, "|", "".center(maxJoblen), "|", "".center(maxNamelen), "|",
            "".center(maxNamelen), "|\n")
        info = "%s%s%s" % (info, tag, "\n")
        for job in jobNameList:
            jobName = str(job.split("-")[0])
            i = int(job.split("-")[1])
            len_s = len(self.jobInfo[jobName][i - 1]["successNodes"])
            len_f = len(self.jobInfo[jobName][i - 1]["failedNodes"])
            if len_s >= len_f:
                self.jobInfo[jobName][i - 1]["failedNodes"] += [None] * (
                            len_s - len_f)
            else:
                self.jobInfo[jobName][i - 1]["successNodes"] += [None] * (
                            len_f - len_s)

            isInitTitle = 0
            for s, f in zip(self.jobInfo[jobName][i - 1]["successNodes"],
                            self.jobInfo[jobName][i - 1]["failedNodes"]):
                if isInitTitle == 1:
                    job = ""
                if str(s) == "None":
                    s = ""
                if str(f) == "None":
                    f = ""
                info = "%s%s%s%s%s%s%s%s%s" % (
                    info, "|", job.ljust(maxJoblen), "|",
                    str(s).center(maxNamelen), "|", str(f).center(maxNamelen),
                    "|",
                    "\n")
                isInitTitle = 1
            info = "%s%s%s" % (info, tag, "\n")

        cmd = " echo '%s\n' >> %s/collector_tmp_%s/collector_%s/Summary.log" \
              % (
                  info, resultdir, currentTime, currentTime)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            print("Generate Summary Info Failed.")
            self.context.logger.debug("The cmd is %s " % cmd)
            self.context.logger.debug(
                "Generate Summary Info Failed %s." % output)

    def printDetailSummaryInfo(self, resultdir, currentTime):
        statusFiedLen = len("SuccessfulTask") + 4
        for node, jobList in self.nodeJobInfo.items():
            for job in jobList:
                jsonJob = json.loads(job)
                successLen = 0
                jobName = jsonJob["jobName"]
                successInfoList = []
                failedInfolist = []
                Info = ""
                tag = []
                successTask = jsonJob["successTask"]

                for i in range(0, len(successTask), 5):
                    Task = "; ".join(successTask[i: i + 5])
                    successLen = max(successLen, len(Task))
                    successInfoList.append(Task)

                failedLen = 0
                for failedJob, reason in jsonJob["failedTask"].items():
                    failedInfo = failedJob + ": " + reason
                    failedLen = max(len(failedInfo), failedLen)
                    failedInfolist.append(failedInfo)

                title = "%s - %s - %s" % (
                    node, jobName, "Success" if failedLen == 0 else "Failed")
                taskMaxLen = max(failedLen + 4 + 2, successLen + 4 + 2)
                maxLen = max(taskMaxLen, len(title))
                titleLen = maxLen + statusFiedLen + 1
                totalLen = titleLen + 2
                i = 0
                while i < totalLen:
                    tag.append("-")
                    i += 1
                Info = "%s%s%s" % (Info, "".join(tag), "\n")
                Info = "%s%s%s%s%s" % (
                    Info, "|", " ".center(titleLen), "|", "\n")
                Info = "%s%s%s%s%s" % (
                    Info, "|", title.center(titleLen), "|", "\n")
                Info = "%s%s%s%s%s" % (
                    Info, "|", " ".center(titleLen), "|", "\n")
                Info = "%s%s%s" % (Info, "".join(tag), "\n")
                for s in successInfoList:
                    Info = "%s%s%s%s%s%s%s" % (
                        Info, "|", "SuccessfulTask".center(statusFiedLen), "|",
                        s.center(maxLen), "|", "\n")
                    Info = "%s%s%s" % (Info, "".join(tag), "\n")
                for f in failedInfolist:
                    Info = "%s%s%s%s%s%s%s" % (
                        Info, "|", "FailedTask".center(statusFiedLen), "|",
                        f.center(maxLen), "|", "\n")
                    Info = "%s%s%s" % (Info, "".join(tag), "\n")
                Info = "%s%s" % (Info, "\n\n")
                cmd = \
                    " echo '%s' " \
                    ">> %s/collector_tmp_%s/collector_%s/Detail.log" % (
                        Info, resultdir, currentTime, currentTime)
                (status, output) = subprocess.getstatusoutput(cmd)
                if status != 0:
                    print("Generate Detail Summary Info Failed")
                    self.context.logger.debug("The cmd is %s " % cmd)
                    self.context.logger.debug(
                        "Generate Detail Summary Info Failed %s." % output)

    def generalJobInfo(self, jobName, nodeList):
        if self.jobInfo.__contains__(jobName):
            self.jobInfo[jobName].append(nodeList)
        else:
            nodes = [nodeList]
            self.jobInfo[jobName] = nodes

    def generalDetailInfo(self, nodeName, job):
        if self.nodeJobInfo.__contains__(nodeName):
            self.nodeJobInfo[nodeName].append(job)
        else:
            jobList = [job]
            self.nodeJobInfo[nodeName] = jobList

    def generalSummary(self, resultdir, currentTime):
        self.printSummaryInfo(resultdir, currentTime)
        self.printDetailSummaryInfo(resultdir, currentTime)

    def resultCheck(self, output):
        isFailed = 0
        nodeList = {}
        successNodeList = []
        failedNodeList = []
        jobName = "UNKNOWN"
        try:
            if self.context.isSingle or self.context.localMode:
                if len(json.loads(output)["failedTask"]) > 0:
                    isFailed = 1
                    failedNodeList.append(self.context.nodeName[0])
                else:
                    successNodeList.append(self.context.nodeName[0])
                self.generalDetailInfo(self.context.nodeName[0], output)
                jobName = json.loads(output)["jobName"]
            else:
                for node in self.context.nodeName:
                    if len(json.loads(str(output[node]))["failedTask"]) > 0:
                        isFailed = 1
                        failedNodeList.append(node)
                    else:
                        successNodeList.append(node)
                    self.generalDetailInfo(node, str(output[node]))
                    jobName = json.loads(output[node])["jobName"]
            nodeList["successNodes"] = successNodeList
            nodeList["failedNodes"] = failedNodeList

            self.generalJobInfo(jobName, nodeList)
            return isFailed
        except Exception as e:
            self.context.logger.debug("check result failed %s." % str(e))
            return 1

    def planResultCheck(self, output):
        isFailed = 1
        nodeList = {}
        successNodeList = []
        failedNodeList = []
        jobName = "UNKNOWN"
        try:
            if self.context.isSingle or self.context.localMode:
                if len(json.loads(output)["failedTask"]) == 0:
                    isFailed = 0
                    successNodeList.append(self.context.nodeName[0])
                else:
                    failedNodeList.append(self.context.nodeName[0])

                self.generalDetailInfo(self.context.nodeName[0], output)
                jobName = json.loads(output)["jobName"]
            else:
                for node in self.context.nodeName:
                    if len(json.loads(str(output[node]))["failedTask"]) == 0:
                        isFailed = 0
                        successNodeList.append(node)
                    else:
                        failedNodeList.append(node)

                    self.generalDetailInfo(node, str(output[node]))
                    jobName = json.loads(output[node])["jobName"]
            nodeList["successNodes"] = successNodeList
            nodeList["failedNodes"] = failedNodeList
            self.generalJobInfo(jobName, nodeList)
            return isFailed
        except Exception as e:
            self.context.logger.debug("check plan result failed %s." % str(e))
            return 1

    def systemCheck(self, sysInfo):
        """
        function: collected OS information
        output: Successfully collected OS information
        """
        self.context.logger.log("Collecting OS information.")
        # Check the system information on each node
        cmd = "source %s; %s -t system_check -U %s -l %s -C %s" % (
            self.context.mpprcFile,
            OMCommand.getLocalScript("Local_Collect"),
            self.context.user,
            self.context.localLog,
            self.formatJsonString(sysInfo))

        if (self.context.isSingle or self.context.localMode):
            output = subprocess.getstatusoutput(cmd)[1]
            flag = self.resultCheck(output)
        else:
            self.context.sshTool.getSshStatusOutput(
                cmd,
                self.context.nodeName)
            outputMap = self.context.sshTool.parseSshOutput(
                self.context.nodeName)
            # Gets the execution result
            flag = self.resultCheck(outputMap)
        if (flag == 0):
            self.context.logger.log("Successfully collected OS information.")
        else:
            self.context.logger.log("The cmd is %s " % cmd)
            self.context.logger.log("Failed to collect OS information.")

    def databaseCheck(self, data):
        """
        function: collected catalog informatics
        output: Successfully collected catalog statistics.
        """
        self.context.logger.log("Collecting catalog statistics.")
        # Collect catalog statistics on each node
        cmd = "source %s; %s -t database_check -U %s -l %s -C %s" % (
            self.context.mpprcFile,
            OMCommand.getLocalScript("Local_Collect"),
            self.context.user,
            self.context.localLog,
            self.formatJsonString(data))

        if (self.context.isSingle or self.context.localMode):
            output = subprocess.getstatusoutput(cmd)[1]
            flag = self.resultCheck(output)
        else:
            self.context.sshTool.getSshStatusOutput(
                cmd,
                self.context.nodeName)
            outputMap = self.context.sshTool.parseSshOutput(
                self.context.nodeName)
            # Gets the execution result
            flag = self.resultCheck(outputMap)
        if (flag == 0):
            self.context.logger.log(
                "Successfully collected catalog statistics.")
        else:
            self.context.logger.log("The cmd is %s " % cmd)
            self.context.logger.log("Failed collected catalog statistics.")

    def logCopy(self, log, l):
        """
        function: collected log files
        output: Successfully collected log files
        """
        self.context.logger.log("Collecting %s files." % log)
        # Copy the log information on each node
        self.context.keyword = base64.b64encode(
            bytes(self.context.keyword, 'utf-8')).decode()

        cmd = \
            "source %s; " \
            "%s -t %s -U %s -b '%s' -e '%s' -k '%s' -l %s -s %d -S %d -C %s" \
            % (self.context.mpprcFile,
               OMCommand.getLocalScript("Local_Collect"),
               "log_copy" if log == "Log" else (
                   "xlog_copy" if log == "XLog" else "core_copy"),
               self.context.user,
               self.context.begintime,
               self.context.endtime,
               self.context.keyword,
               self.context.localLog,
               # For local collection,
               # use the max speed limit, as all nodes do it individually.
               self.context.speedLimit * 1024,
               self.context.speedLimitFlag,
               self.formatJsonString(l)
               )

        if (self.context.isSingle or self.context.localMode):
            output = subprocess.getstatusoutput(cmd)[1]
            flag = self.resultCheck(output)
        else:
            timeout = int(
                self.context.LOG_SIZE_PER_DAY_ONE_NODE
                * self.context.duration // self.context.speedLimit \
                + self.context.LOG_SIZE_PER_DAY_ONE_NODE
                * self.context.duration // self.context.TAR_SPEED)
            # The timeout value should be in [10 min, 1 hour]
            if (timeout < DefaultValue.TIMEOUT_PSSH_COLLECTOR):
                timeout = DefaultValue.TIMEOUT_PSSH_COLLECTOR
            elif (timeout > 3600):
                timeout = 3600
            self.context.sshTool.setTimeOut(timeout)
            self.context.logger.debug(
                "Collection will be timeout in %ds." % timeout)
            self.context.sshTool.getSshStatusOutput(
                cmd,
                self.context.nodeName)
            outputMap = self.context.sshTool.parseSshOutput(
                self.context.nodeName)
            # Gets the execution result
            flag = self.resultCheck(outputMap)
        if (flag == 0):
            self.context.logger.log("Successfully collected %s files." % log)
        else:
            self.context.logger.log("The cmd is %s " % cmd)
            self.context.logger.log("Failed collected %s files." % log)

    def confGstack(self, check, s):
        """
        function: collected configuration files and processed stack information
        output: Successfully collected configuration files
        and processed stack information.
        """
        self.context.logger.log("Collecting %s files." % s["TypeName"])
        # Collect configuration files
        # and process stack information on each node
        cmd = "source %s; %s -t %s -U %s -l %s -C %s" % (
            self.context.mpprcFile,
            OMCommand.getLocalScript("Local_Collect"),
            check,
            self.context.user,
            self.context.localLog,
            self.formatJsonString(s))

        if (self.context.isSingle or self.context.localMode):
            output = subprocess.getstatusoutput(cmd)[1]
            flag = self.resultCheck(output)
        else:
            self.context.sshTool.getSshStatusOutput(
                cmd,
                self.context.nodeName)
            outputMap = self.context.sshTool.parseSshOutput(
                self.context.nodeName)
            # Gets the execution result
            flag = self.resultCheck(outputMap)
        if (flag == 0):
            self.context.logger.log(
                "Successfully collected %s files." % s["TypeName"])
        else:
            self.context.logger.log("The cmd is %s " % cmd)
            self.context.logger.log(
                "Failed collected %s files." % s["TypeName"])

    def planSimulator(self, data):
        """
        function: collect plan simulator files
        output: Successfully collected files.
        """
        self.context.logger.log("Collecting plan simulator statistics.")
        # Collect plan simulator on each node
        cmd = "source %s; %s -t plan_simulator_check -U %s -l %s -C %s" % (
            self.context.mpprcFile,
            OMCommand.getLocalScript("Local_Collect"),
            self.context.user,
            self.context.localLog,
            self.formatJsonString(data))

        if (self.context.isSingle or self.context.localMode):
            output = subprocess.getstatusoutput(cmd)[1]
            flag = self.planResultCheck(output)
        else:
            (status, output) = self.context.sshTool.getSshStatusOutput(
                cmd,
                self.context.nodeName)
            outputMap = self.context.sshTool.parseSshOutput(
                self.context.nodeName)
            # Gets the execution result
            flag = self.planResultCheck(outputMap)
        if (flag == 0):
            self.context.logger.log("Successfully collected plan simulator.")
        else:
            self.context.logger.log("The cmd is %s " % cmd)
            self.context.logger.log("Failed collected plan simulator.")

    def copyFile(self):
        """
        function: collected result files
        output: Successfully collected files.
        """
        self.context.logger.log("Collecting files.")
        # Collect result files on each node
        cmd = "source %s; %s -t copy_file -U %s -o %s -h %s -l %s" % (
            self.context.mpprcFile,
            OMCommand.getLocalScript("Local_Collect"),
            self.context.user,
            self.context.outFile,
            DefaultValue.GetHostIpOrName(),
            self.context.localLog)

        flag = 0
        if (self.context.isSingle or self.context.localMode):
            cmd = cmd + (" -s %d" % self.context.speedLimit * 1024)
            cmd = cmd + (" -S %d" % self.context.speedLimitFlag)
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                flag = 0
                self.context.logger.log("The cmd is %s " % cmd)
                self.context.logger.log(
                    "Failed to collect files on %s. Error:\n%s" % \
                    (self.context.nodeName[0], output))
            else:
                flag = 1
        else:
            parallelNum = DefaultValue.getCpuSet()
            if (len(self.context.nodeName) < parallelNum):
                parallelNum = len(self.context.nodeName)

            speedLimitEachNodeKBs = int(
                self.context.speedLimit * 1024 // parallelNum)

            # In parallel mode,
            # set a bandwidth to collect log files from other nodes
            # to avoid too much IO for net card, which is risky for CM things.
            cmd = cmd + (" -s %d" % speedLimitEachNodeKBs)
            cmd = cmd + (" -S %d" % self.context.speedLimitFlag)

            # The timeout value to remote copy.
            timeout = self.context.LOG_SIZE_PER_DAY_ONE_NODE \
                      * self.context.duration * 1024 // speedLimitEachNodeKBs
            # The timeout value should be in [10 min, 1 hour]
            if (timeout < DefaultValue.TIMEOUT_PSSH_COLLECTOR):
                timeout = DefaultValue.TIMEOUT_PSSH_COLLECTOR
            elif (timeout > 3600):
                timeout = 3600
            self.context.sshTool.setTimeOut(timeout)
            self.context.logger.debug(
                "Copy logs will be timeout in %ds." % timeout)
            (status, output) = self.context.sshTool.getSshStatusOutput(
                cmd,
                self.context.nodeName,
                parallel_num=parallelNum)
            self.context.sshTool.parseSshOutput(
                self.context.nodeName)
            # Gets the execution result
            for node in self.context.nodeName:
                if (status[node] == DefaultValue.SUCCESS):
                    flag = 1

        if (flag == 0):
            self.context.logger.log(
                "Failed to collect files: All collection tasks failed")
        else:
            self.context.logger.log("Successfully collected files.")

    def tarResultFiles(self, currentTime, targetdir, resultdir):
        """
        :return:
        """
        # tar the result and delete directory
        try:
            # tar the result and delete directory
            tarFile = "collector_%s.tar.gz" % currentTime
            destDir = "collector_%s" % currentTime
            cmd = "%s && %s" % (g_Platform.getCdCmd(targetdir),
                                g_Platform.getCompressFilesCmd(tarFile,
                                                               destDir))
            cmd += " && %s" % g_Platform.getChmodCmd(
                str(DefaultValue.KEY_FILE_MODE), tarFile)
            cmd += " && %s" % g_Platform.getMoveFileCmd(tarFile, "../")
            cmd += " && %s '%s'" % (
                g_Platform.getRemoveCmd("directory"), targetdir)
            DefaultValue.execCommandLocally(cmd)
            self.context.logger.log(
                "All results are stored in %s/collector_%s.tar.gz." % (
                    resultdir, currentTime))
        except Exception as e:
            raise Exception(str(e))

    def getCycle(self, sysInfo):
        """
        function: parse interval and count
        input : sysInfo
        output: count, interval
        """
        interval = 0
        if sysInfo.__contains__('Interval'):
            interval = int(sysInfo['Interval'].replace(" ", ""))
        count = int(sysInfo['Count'].replace(" ", ""))
        return interval, count

    def doCollector(self):
        """
        function: collect information
        input : strftime
        output: Successfully collected catalog statistics
        """
        # Parsing configuration files
        self.parseConfigFile()

        # check rsync command
        if self.context.speedLimitFlag == 1:
            self.checkCommand()

        # check tmp directory
        self.checkTmpDir()

        self.createDir()
        # create store dir
        (currentTime, targetdir, resultdir) = self.createStoreDir()

        # collect OS information
        if self.context.config.__contains__('System'):
            sysList = self.context.config['System']
            for sysInfo in sysList:
                if sysInfo.__contains__('Count'):
                    (interval, count) = self.getCycle(sysInfo)
                    print("do system check interval %s : count %s" % (
                        str(interval), str(count)))
                    while count:
                        count -= 1
                        self.systemCheck(sysInfo)
                        if count > 0 and interval > 0:
                            time.sleep(interval)
                else:
                    self.systemCheck(sysInfo)

        # collect catalog statistics
        if self.context.config.__contains__('Database'):
            dataList = self.context.config['Database']
            for data in dataList:
                if data.__contains__('Count'):
                    (interval, count) = self.getCycle(data)
                    print("do database check interval %s : count %s" % (
                        str(interval), str(count)))
                    while count:
                        count -= 1
                        self.databaseCheck(data)
                        if count > 0 and interval > 0:
                            time.sleep(interval)
                else:
                    self.databaseCheck(data)

        # Collect log files
        if self.context.config.__contains__('Log'):
            logList = self.context.config['Log']
            for l in logList:
                if l.__contains__('Count'):
                    (interval, count) = self.getCycle(l)
                    print("do log check interval %s : count %s" % (
                        str(interval), str(count)))
                    if count > 1:
                        self.context.logger.log(
                            ErrorCode.GAUSS_512["GAUSS_51246"] % "Log")
                        count = 1
                    while count:
                        count -= 1
                        self.logCopy("Log", l)
                        if count > 0 and interval > 0:
                            time.sleep(interval)
                else:
                    self.logCopy("Log", l)

        # Collect xlog files
        if self.context.config.__contains__('XLog'):
            xloglist = self.context.config['XLog']
            for l in xloglist:
                if l.__contains__('Count'):
                    (interval, count) = self.getCycle(l)
                    print("do XLog check " + str(interval) + ":" + str(count))
                    if count > 1:
                        self.context.logger.log(
                            ErrorCode.GAUSS_512["GAUSS_51246"] % "XLog")
                        count = 1
                    while count:
                        count -= 1
                        self.logCopy("XLog", l)
                        if count > 0 and interval > 0:
                            time.sleep(interval)
                else:
                    self.logCopy("XLog", l)

        # CoreDump files
        if self.context.config.__contains__('CoreDump'):
            corelist = self.context.config['CoreDump']
            for l in corelist:
                if l.__contains__('Count'):
                    (interval, count) = self.getCycle(l)
                    print("do CoreDump check " + str(interval) + ":" + str(
                        count))
                    if count > 1:
                        self.context.logger.log(
                            ErrorCode.GAUSS_512["GAUSS_51246"] % "CoreDump")
                        count = 1
                    while count:
                        count -= 1
                        self.logCopy("CoreDump", l)
                        if count > 0 and interval > 0:
                            time.sleep(interval)
                else:
                    self.logCopy("CoreDump", l)

        # collect configuration files
        if self.context.config.__contains__('Config'):
            clist = self.context.config['Config']
            for c in clist:
                if c.__contains__('Count'):
                    (interval, count) = self.getCycle(c)
                    print("do Config check " + str(interval) + ":" + str(
                        count))
                    if count > 1:
                        self.context.logger.log(
                            ErrorCode.GAUSS_512["GAUSS_51246"] % "Config")
                        count = 1
                    while count:
                        count -= 1
                        self.confGstack("Config", c)
                        if count > 0 and interval > 0:
                            time.sleep(interval)
                else:
                    self.confGstack("Config", c)

        # process stack information
        if self.context.config.__contains__('Gstack'):
            stacklist = self.context.config['Gstack']
            for s in stacklist:
                if s.__contains__('Count'):
                    (interval, count) = self.getCycle(s)
                    print("do Gstack check " + str(interval) + ":" + str(
                        count))

                    while count:
                        count -= 1
                        self.confGstack("Gstack", s)
                        if count > 0 and interval > 0:
                            time.sleep(interval)
                else:
                    self.confGstack("Gstack", s)

        # collect configuration files and process stack information
        if self.context.config.__contains__('Trace'):
            print("do config check")

        # collect plan simulator files
        if self.context.config.__contains__('Plan'):
            dbList = self.context.config['Plan']
            for s in dbList:
                if s.__contains__('Count'):
                    (interval, count) = self.getCycle(s)
                    print("do Plan check " + str(interval) + ":" + str(count))

                    while count:
                        count -= 1
                        self.planSimulator(s)
                        if count > 0 and interval > 0:
                            time.sleep(interval)
                else:
                    self.planSimulator(s)

        # Collect result files
        self.copyFile()

        # generate summary info
        self.generalSummary(resultdir, currentTime)

        # tar the result and delete directory
        self.tarResultFiles(currentTime, targetdir, resultdir)
