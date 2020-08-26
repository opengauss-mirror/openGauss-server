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
# Description  : SshTool.py is utility to support ssh tools
#############################################################################
import subprocess
import os
import sys
import datetime
import weakref
import getpass
import time
import re
from random import sample

sys.path.append(sys.path[0] + "/../../")
from gspylib.common.ErrorCode import ErrorCode
from gspylib.common.Common import DefaultValue
from gspylib.os.gsfile import g_file
from gspylib.common.GaussLog import GaussLog


class SshTool():
    """
    Class for controling multi-hosts
    """

    def __init__(self, hostNames, logFile=None,
                 timeout=DefaultValue.TIMEOUT_PSSH_COMMON, key=""):
        '''
        Constructor
        '''
        self.hostNames = hostNames
        self.__logFile = logFile
        self.__pid = os.getpid()
        self.__timeout = timeout + 30
        self._finalizer = weakref.finalize(self, self.clenSshResultFiles)

        currentTime = str(datetime.datetime.now()).replace(" ", "_").replace(
            ".", "_")
        randomnum = ''.join(sample('0123456789', 3))
        # can tmp path always access?
        if key == "":
            self.__hostsFile = "/tmp/gauss_hosts_file_%d_%s_%s" % (
                self.__pid, currentTime, randomnum)
            self.__resultFile = "/tmp/gauss_result_%d_%s_%s.log" % (
                self.__pid, currentTime, randomnum)
            self.__outputPath = "/tmp/gauss_output_files_%d_%s_%s" % (
                self.__pid, currentTime, randomnum)
            self.__errorPath = "/tmp/gauss_error_files_%d_%s_%s" % (
                self.__pid, currentTime, randomnum)
        else:
            self.__hostsFile = "/tmp/gauss_hosts_file_%d_%s_%s_%s" % (
                self.__pid, key, currentTime, randomnum)
            self.__resultFile = "/tmp/gauss_result_%d_%s_%s_%s.log" % (
                self.__pid, key, currentTime, randomnum)
            self.__outputPath = "/tmp/gauss_output_files_%d_%s_%s_%s" % (
                self.__pid, key, currentTime, randomnum)
            self.__errorPath = "/tmp/gauss_error_files_%d_%s_%s_%s" % (
                self.__pid, key, currentTime, randomnum)

        self.__resultStatus = {}
        if logFile is None:
            self.__logFile = "/dev/null"

        # before using, clean the old ones
        g_file.removeFile(self.__hostsFile)
        g_file.removeFile(self.__resultFile)

        if os.path.exists(self.__outputPath):
            g_file.removeDirectory(self.__outputPath)

        if os.path.exists(self.__errorPath):
            g_file.removeDirectory(self.__errorPath)

        self.__writeHostFiles()

    def clenSshResultFiles(self):
        """
        function: Delete file
        input : NA
        output: NA
        """
        if os.path.exists(self.__hostsFile):
            g_file.removeFile(self.__hostsFile)

        if os.path.exists(self.__resultFile):
            g_file.removeFile(self.__resultFile)

        if os.path.exists(self.__outputPath):
            g_file.removeDirectory(self.__outputPath)

        if os.path.exists(self.__errorPath):
            g_file.removeDirectory(self.__errorPath)

    def __del__(self):
        """
        function: Delete file
        input : NA
        output: NA
        """
        self._finalizer()

    def exchangeHostnameSshKeys(self, user, pwd, mpprcFile=""):
        """
        function: Exchange ssh public keys for specified user, using hostname
        input : user, pwd, mpprcFile
        output: NA
        """
        if mpprcFile != "":
            exkeyCmd = "su - %s -c 'source %s&&mvxssh-exkeys -f %s -p" \
                       " %s' 2>>%s" % (user, mpprcFile, self.__hostsFile,
                                       pwd, self.__logFile)
        else:
            exkeyCmd = "su - %s -c 'source /etc/profile&&mvxssh-exkeys -f" \
                       " %s -p %s' 2>>%s" % (user, self.__hostsFile,
                                             pwd, self.__logFile)
        (status, output) = subprocess.getstatusoutput(exkeyCmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_511["GAUSS_51112"] % user +
                            "Error: \n%s" % output.replace(pwd, "******") +
                            "\nYou can comment Cipher 3des."
                            " Ciphers aes128-cbc and MACs in"
                            " /etc/ssh/ssh_config and try again.")

    def exchangeIpSshKeys(self, user, pwd, ips, mpprcFile=""):
        """
        function: Exchange ssh public keys for specified user,
                  using ip address
        input : user, pwd, ips, mpprcFile
        output: NA
        """
        if mpprcFile != "":
            exkeyCmd = "su - %s -c 'source %s&&mvxssh-exkeys " \
                       % (user, mpprcFile)
        else:
            exkeyCmd = "su - %s -c 'source /etc/profile&&mvxssh-exkeys " \
                       % user
        for ip in ips:
            exkeyCmd += " -h %s " % ip.strip()
        exkeyCmd += "-p %s' 2>>%s" % (pwd, self.__logFile)
        (status, output) = subprocess.getstatusoutput(exkeyCmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_511["GAUSS_51112"]
                            % user + "Error: \n%s"
                            % output.replace(pwd, "******") +
                            "\nYou can comment Cipher 3des, Ciphers"
                            " aes128-cbc and MACs in /etc/ssh/ssh_config"
                            " and try again.")

    def createTrust(self, user, pwd, ips=None, mpprcFile="",
                    skipHostnameSet=False, preMode=False):
        """
        function: create trust for specified user with both ip and hostname,
                  when using N9000 tool create trust failed
                  do not support using a normal user to create trust for
                  another user.
        input : user, pwd, ips, mpprcFile, skipHostnameSet
        output: NA
        """
        tmp_hosts = "/tmp/tmp_hosts_%d" % self.__pid
        cnt = 0
        status = 0
        output = ""
        if ips is None:
            ips = []
        try:
            g_file.removeFile(tmp_hosts)
            # 1.prepare hosts file
            for ip in ips:
                cmd = "echo %s >> %s 2>/dev/null" % (ip, tmp_hosts)
                (status, output) = subprocess.getstatusoutput(cmd)
                if status != 0:
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"]
                                    % tmp_hosts + " Error:\n%s." % output
                                    + "The cmd is %s" % cmd)
            g_file.changeMode(DefaultValue.KEY_HOSTS_FILE, tmp_hosts, False,
                              "python")

            # 2.call createtrust script
            create_trust_file = "gs_sshexkey"
            if pwd is None or len(str(pwd)) == 0:
                GaussLog.printMessage("Please enter password for current"
                                      " user[%s]." % user)
                pwd = getpass.getpass()
               
            if (mpprcFile != "" and
                    g_file.checkFilePermission(mpprcFile, True) and
                    self.checkMpprcfile(user, mpprcFile)):
                cmd = "source %s; %s -f %s -l '%s'" % (
                    mpprcFile,  create_trust_file, tmp_hosts,
                    self.__logFile)
            elif (mpprcFile == "" and g_file.checkFilePermission(
                    '/etc/profile', True)):
                cmd = "source /etc/profile;" \
                      " %s -f %s -l '%s'" % (create_trust_file,
                                             tmp_hosts, self.__logFile)

            if skipHostnameSet:
                cmd += " --skip-hostname-set"
            cmd += " 2>&1"
            
            tempcmd = ["su", "-", user, "-c"]
            tempcmd.append(cmd)
            cmd = tempcmd

            p = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                     stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if os.getuid() != 0:
                time.sleep(5)
                p.stdin.write((pwd+"\n").encode(encoding="utf-8"))
            time.sleep(10) 
            p.stdin.write((pwd+"\n").encode(encoding="utf-8"))           
            (output, err) = p.communicate()
            # 3.delete hosts file
            g_file.removeFile(tmp_hosts)
            if output is not None:
                output = str(output, encoding='utf-8')
                if re.search("\[GAUSS\-", output):
                    if re.search("Please enter password", output):
                        GaussLog.printMessage(
                            ErrorCode.GAUSS_503["GAUSS_50306"] % user)
                    else:
                        GaussLog.printMessage(output.strip())
                    sys.exit(1)
                else:
                    GaussLog.printMessage(output.strip())
            else:
                sys.exit(1)
        except Exception as e:
            g_file.removeFile(tmp_hosts)
            raise Exception(str(e))

    def checkMpprcfile(self, username, filePath):
        """
        function:
          check if given user has operation permission for Mpprcfile
        precondition:
          1.user should be exist---root/cluster user
          2.filePath should be an absolute path
        postcondition:
          1.return True or False
        input : username,filePath
        output: True/False
        """
        ownerPath = os.path.split(filePath)[0]
        cmd = "su - %s -c 'cd %s'" % (username, ownerPath)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_500["GAUSS_50004"]
                            % '-sep-env-file' + " Error:\n%s." % output
                            + "The cmd is %s" % cmd)

        return True

    def getUserOSProfile(self, env_file=""):
        """
        function: get user os profile
        input : env_file
        output: mpprcFile, userProfile, osProfile
        """
        if env_file != "":
            mpprcFile = env_file
        else:
            mpprcFile = DefaultValue.getEnv(DefaultValue.MPPRC_FILE_ENV)

        if mpprcFile != "" and mpprcFile is not None:
            userProfile = mpprcFile
        else:
            userProfile = "~/.bashrc"
        osProfile = "/etc/profile"
        return mpprcFile, userProfile, osProfile

    def getGPHOMEPath(self, osProfile):
        """
        function: get GPHOME path
        input : osProfile
        output: output
        """
        try:
            cmd = "source %s && echo $GPHOME" % osProfile
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0 or not output or output.strip() == "":
                raise Exception(ErrorCode.GAUSS_518["GAUSS_51802"] % "GPHOME"
                                + "The cmd is %s" % cmd)
            return output.strip()
        except Exception as e:
            raise Exception(str(e))

    def parseSshResult(self, hostList=None):
        """
        function: parse ssh result
        input : hostList
        output: resultMap, outputCollect
        """
        try:
            if hostList is None:
                hostList = []
            outputCollect = ""
            prefix = ""
            resultMap = self.__readCmdResult(self.__resultFile, len(hostList))
            for host in hostList:
                sshOutPutFile = "%s/%s" % (self.__outputPath, host)
                sshErrorPutFile = "%s/%s" % (self.__errorPath, host)
                if resultMap[host] == DefaultValue.SUCCESS:
                    prefix = "SUCCESS"
                else:
                    prefix = "FAILURE"
                outputCollect += "[%s] %s:\n" % (prefix, str(host))
                if os.path.isfile(sshOutPutFile):
                    context = ""
                    with open(sshOutPutFile, "r") as fp:
                        context = fp.read()
                    outputCollect += context
                if os.path.isfile(sshErrorPutFile):
                    context = ""
                    with open(sshErrorPutFile, "r") as fp:
                        context = fp.read()
                    outputCollect += context
        except Exception as e:
            raise Exception(str(e))
        return resultMap, outputCollect

    def timeOutClean(self, cmd, psshpre, hostList=None, env_file="",
                     parallel_num=300, signal=9):
        """
        function: timeout clean
        """
        if hostList is None:
            hostList = []
        pstree = "python3 %s -sc" % os.path.realpath(os.path.dirname(
            os.path.realpath(__file__)) + "/../../py_pstree.py")
        mpprcFile, userProfile, osProfile = self.getUserOSProfile(env_file)
        # kill the parent and child process. get all process by py_pstree.py
        timeOutCmd = "source %s && pidList=\`ps aux | grep \\\"%s\\\" |" \
                     " grep -v 'grep' | awk '{print \$2}' | xargs \`; " \
                     % (osProfile, cmd)
        timeOutCmd += "for pid in \$pidList; do %s \$pid | xargs -r -n 100" \
                      " kill -%s; done" % (pstree, str(signal))
        if len(hostList) == 0:
            if os.getuid() == 0 and (mpprcFile == "" or not mpprcFile):
                sshCmd = "source %s && %s -t %s -h %s -P -p %s -o %s -e" \
                         " %s \"source %s; %s\" 2>&1 | tee %s" % \
                         (
                             osProfile, psshpre, self.__timeout,
                             self.__hostsFile,
                             parallel_num, self.__outputPath,
                             self.__errorPath, osProfile, timeOutCmd,
                             self.__resultFile)
            else:
                sshCmd = "source %s && %s -t %s -h %s -P -p %s -o %s -e" \
                         " %s \"source %s;source %s;%s\" 2>&1 | tee %s" % \
                         (
                             osProfile, psshpre, self.__timeout,
                             self.__hostsFile,
                             parallel_num, self.__outputPath,
                             self.__errorPath, osProfile, userProfile,
                             timeOutCmd,
                             self.__resultFile)
        else:
            if os.getuid() == 0 and (mpprcFile == "" or not mpprcFile):
                sshCmd = "source %s && %s -t %s -H %s -P -p %s -o %s -e" \
                         " %s \"source %s; %s\" 2>&1 | tee %s" % \
                         (osProfile, psshpre, self.__timeout,
                          " -H ".join(hostList), parallel_num,
                          self.__outputPath,
                          self.__errorPath, osProfile, timeOutCmd,
                          self.__resultFile)
            else:
                sshCmd = "source %s && %s -t %s -H %s -P -p %s -o %s -e" \
                         " %s \"source %s;source %s;%s\" 2>&1 | tee %s" % \
                         (osProfile, psshpre, self.__timeout,
                          " -H ".join(hostList), parallel_num,
                          self.__outputPath,
                          self.__errorPath, osProfile, userProfile,
                          timeOutCmd, self.__resultFile)
        subprocess.getstatusoutput(sshCmd)

    def executeCommand(self, cmd, descript, cmdReturn=DefaultValue.SUCCESS,
                       hostList=None, env_file="", parallel_num=300,
                       checkenv=False):
        """
        function: Execute command on all hosts
        input : cmd, descript, cmdReturn, hostList, env_file, parallel_num
        output: NA
        """
        sshCmd = ""
        localMode = False
        resultMap = {}
        outputCollect = ""
        isTimeOut = False
        if hostList is None:
            hostList = []
        try:
            mpprcFile, userProfile, osProfile = self.getUserOSProfile(
                env_file)
            GPHOME = self.getGPHOMEPath(osProfile)
            psshpre = "python3 %s/script/gspylib/pssh/bin/pssh" % GPHOME

            # clean result file
            if os.path.exists(self.__resultFile):
                os.remove(self.__resultFile)

            if len(hostList) == 0:
                if os.getuid() == 0 and (mpprcFile == "" or not mpprcFile):
                    sshCmd = "source %s && %s -t %s -h %s -P -p %s -o %s -e" \
                             " %s \"source %s; %s\" 2>&1 | tee %s" \
                             % (osProfile, psshpre, self.__timeout,
                                self.__hostsFile, parallel_num,
                                self.__outputPath, self.__errorPath,
                                osProfile, cmd, self.__resultFile)
                else:
                    sshCmd = "source %s && %s -t %s -h %s -P -p %s -o %s -e" \
                             " %s \"source %s;source %s;%s\" 2>&1 | tee %s" \
                             % (osProfile, psshpre, self.__timeout,
                                self.__hostsFile, parallel_num,
                                self.__outputPath, self.__errorPath,
                                osProfile, userProfile, cmd,
                                self.__resultFile)
                hostList = self.hostNames
            else:
                if os.getuid() == 0 and (mpprcFile == "" or not mpprcFile):
                    sshCmd = "source %s && %s -t %s -H %s -P -p %s -o %s -e" \
                             " %s \"source %s; %s\" 2>&1 | tee %s" \
                             % (osProfile, psshpre, self.__timeout,
                                " -H ".join(hostList), parallel_num,
                                self.__outputPath, self.__errorPath,
                                osProfile, cmd, self.__resultFile)
                else:
                    sshCmd = "source %s && %s -t %s -H %s -P -p %s -o %s -e" \
                             " %s \"source %s;source %s;%s\" 2>&1 | tee %s" \
                             % (osProfile, psshpre, self.__timeout,
                                " -H ".join(hostList), parallel_num,
                                self.__outputPath, self.__errorPath,
                                osProfile, userProfile, cmd,
                                self.__resultFile)

            # single cluster or execute only in local node.
            if (len(hostList) == 1 and
                    hostList[0] == DefaultValue.GetHostIpOrName()
                    and cmd.find(" --lock-cluster ") < 0):
                localMode = True
                if os.getuid() == 0 and (mpprcFile == "" or not mpprcFile):
                    sshCmd = "source %s ; %s 2>&1" % (osProfile, cmd)
                else:
                    sshCmd = "source %s ; source %s; %s 2>&1" \
                             % (osProfile, userProfile, cmd)

            # if it is localMode, it means does not call pssh,
            # so there is no time out
            (status, output) = subprocess.getstatusoutput(sshCmd)
            # when the pssh is time out, kill parent and child process
            if not localMode:
                if output.find("Timed out, Killed by signal 9") > 0:
                    self.timeOutClean(cmd, psshpre, hostList, env_file,
                                      parallel_num)
                    isTimeOut = True
                    raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"]
                                    % sshCmd + " Error:\n%s" % output)
                if status != 0:
                    raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"]
                                    % sshCmd + " Error:\n%s" % output)

            if localMode:
                resultMap[hostList[0]] = DefaultValue.SUCCESS if status == 0 \
                    else DefaultValue.FAILURE
                outputCollect = "[%s] %s:\n%s" \
                                % ("SUCCESS" if status == 0 else "FAILURE",
                                   hostList[0], output)
            else:
                # ip and host name should match here
                resultMap, outputCollect = self.parseSshResult(hostList)
        except Exception as e:
            if not isTimeOut:
                self.clenSshResultFiles()
            raise Exception(str(e))

        for host in hostList:
            if resultMap.get(host) != cmdReturn:
                if outputCollect.find("GAUSS-5") == -1:
                    raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"]
                                    % cmd + " Result:%s.\nError:\n%s"
                                    % (resultMap, outputCollect))
                else:
                    raise Exception(outputCollect)
        if checkenv:
            for res in output.split("\n"):
                if res.find("[SUCCESS]") >= 0:
                    continue
                elif res == "":
                    continue
                else:
                    if mpprcFile != "" and mpprcFile is not None:
                        envfile = mpprcFile + " and /etc/profile"
                    else:
                        envfile = "/etc/profile and ~/.bashrc"
                    raise Exception(ErrorCode.GAUSS_518["GAUSS_51808"]
                                    % res + "Please check %s." % envfile)

    def getSshStatusOutput(self, cmd, hostList=None, env_file="",
                           gp_path="", parallel_num=300, ssh_config=""):
        """
        function: Get command status and output
        input : cmd, hostList, env_file, gp_path, parallel_num
        output: resultMap, outputCollect
        """
        sshCmd = ""
        localMode = False
        resultMap = {}
        outputCollect = ""
        isTimeOut = False
        need_replace_quotes = False

        if hostList is None:
            hostList = []

        if cmd.find("[need_replace_quotes]") != -1:
            cmd = cmd.replace("[need_replace_quotes]", "")
            need_replace_quotes = True
        fp = None

        try:
            mpprcFile, userProfile, osProfile = self.getUserOSProfile(
                env_file)
            # clean result file
            if os.path.exists(self.__resultFile):
                os.remove(self.__resultFile)

            if gp_path == "":
                GPHOME = self.getGPHOMEPath(osProfile)
            else:
                GPHOME = gp_path.strip()
            psshpre = "python3 %s/script/gspylib/pssh/bin/pssh" % GPHOME
            if ssh_config:
                if os.path.exists(ssh_config) and os.path.isfile(ssh_config):
                    psshpre += ' -x "-F %s" ' % ssh_config

            if len(hostList) == 0:
                if os.getuid() == 0 and (mpprcFile == "" or not mpprcFile):
                    sshCmd = "source %s && %s -t %s -h %s -P -p %s -o %s -e" \
                             " %s \"source %s; %s\" 2>&1 | tee %s" \
                             % (osProfile, psshpre, self.__timeout,
                                self.__hostsFile, parallel_num,
                                self.__outputPath, self.__errorPath,
                                osProfile, cmd, self.__resultFile)
                else:
                    sshCmd = "source %s && %s -t %s -h %s -P -p %s -o %s -e" \
                             " %s \"source %s;source %s;%s\" 2>&1 | tee %s" \
                             % (osProfile, psshpre, self.__timeout,
                                self.__hostsFile, parallel_num,
                                self.__outputPath, self.__errorPath,
                                osProfile, userProfile, cmd,
                                self.__resultFile)
                hostList = self.hostNames
            else:
                if need_replace_quotes:
                    remote_cmd = cmd.replace("\"", "\\\"")
                else:
                    remote_cmd = cmd
                if os.getuid() == 0 and (mpprcFile == "" or not mpprcFile):
                    sshCmd = "source %s && %s -t %s -H %s -P -p %s -o %s -e" \
                             " %s \"source %s; %s\" 2>&1 | tee %s" \
                             % (osProfile, psshpre, self.__timeout,
                                " -H ".join(hostList), parallel_num,
                                self.__outputPath, self.__errorPath,
                                osProfile, remote_cmd, self.__resultFile)
                else:
                    sshCmd = "source %s && %s -t %s -H %s -P -p %s -o %s -e" \
                             " %s \"source %s;source %s;%s\" 2>&1 | tee %s" \
                             % (osProfile, psshpre, self.__timeout,
                                " -H ".join(hostList), parallel_num,
                                self.__outputPath, self.__errorPath,
                                osProfile, userProfile, remote_cmd,
                                self.__resultFile)

            # single cluster or execute only in local node.
            if (len(hostList) == 1 and
                    hostList[0] == DefaultValue.GetHostIpOrName()):
                localMode = True
                if os.getuid() == 0 and (mpprcFile == "" or not mpprcFile):
                    sshCmd = "source %s ; %s 2>&1" % (osProfile, cmd)
                else:
                    sshCmd = "source %s ; source %s; %s 2>&1" % (osProfile,
                                                                 userProfile,
                                                                 cmd)

            (status, output) = subprocess.getstatusoutput(sshCmd)
            # when the pssh is time out, kill parent and child process
            if not localMode:
                if output.find("Timed out, Killed by signal 9") > 0:
                    isTimeOut = True
                    self.timeOutClean(cmd, psshpre, hostList, env_file,
                                      parallel_num)
                    raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"]
                                    % sshCmd + " Error:\n%s" % output)
                if status != 0:
                    raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"]
                                    % sshCmd + " Error:\n%s" % output)

            if localMode:
                dir_permission = 0o700
                if status == 0:
                    resultMap[hostList[0]] = DefaultValue.SUCCESS
                    outputCollect = "[%s] %s:\n%s" % ("SUCCESS", hostList[0],
                                                      output)

                    if not os.path.exists(self.__outputPath):
                        os.makedirs(self.__outputPath, mode=dir_permission)
                    file_path = os.path.join(self.__outputPath, hostList[0])
                    g_file.createFileInSafeMode(file_path)
                    with open(file_path, "w") as fp:
                        fp.write(output)
                        fp.flush()
                        fp.close()
                else:
                    resultMap[hostList[0]] = DefaultValue.FAILURE
                    outputCollect = "[%s] %s:\n%s" % ("FAILURE", hostList[0],
                                                      output)

                    if not os.path.exists(self.__errorPath):
                        os.makedirs(self.__errorPath, mode=dir_permission)
                    file_path = os.path.join(self.__errorPath, hostList[0])
                    g_file.createFileInSafeMode(file_path)
                    with open(file_path, "w") as fp:
                        fp.write(output)
                        fp.flush()
                        fp.close()
            else:
                resultMap, outputCollect = self.parseSshResult(hostList)
        except Exception as e:
            if fp:
                fp.close()
            if not isTimeOut:
                self.clenSshResultFiles()
            raise Exception(str(e))

        for host in hostList:
            if resultMap.get(host) != DefaultValue.SUCCESS:
                if outputCollect.find("GAUSS-5") == -1:
                    outputCollect = ErrorCode.GAUSS_514["GAUSS_51400"] \
                                    % cmd + " Error:\n%s." % outputCollect
                    break

        return resultMap, outputCollect

    def parseSshOutput(self, hostList):
        """
        function:
          parse ssh output on every host
        input:
          hostList: the hostname list of all hosts
        output:
          a dict, like this "hostname : info of this host"
        hiden info:
          the output info of all hosts
        ppp:
          for a host in hostList
            if outputfile exists
              open file with the same name
              read context into a str
              close file
              save info of this host
            else
              raise exception
          return host info list
        """
        resultMap = {}
        try:
            for host in hostList:
                context = ""
                sshOutPutFile = "%s/%s" % (self.__outputPath, host)
                sshErrorPutFile = "%s/%s" % (self.__errorPath, host)

                if os.path.isfile(sshOutPutFile):
                    with open(sshOutPutFile, "r") as fp:
                        context = fp.read()
                    resultMap[host] = context
                if os.path.isfile(sshErrorPutFile):
                    with open(sshErrorPutFile, "r") as fp:
                        context += fp.read()
                    resultMap[host] = context
                if (not os.path.isfile(sshOutPutFile) and
                        not os.path.isfile(sshErrorPutFile)):
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"]
                                    % "%s or %s"
                                    % (sshOutPutFile, sshErrorPutFile))
        except Exception as e:
            raise Exception(str(e))

        return resultMap

    def scpFiles(self, srcFile, targetDir, hostList=None, env_file="",
                 gp_path="", parallel_num=300):
        """
        function: copy files to other path
        input : srcFile, targetDir, hostList, env_file, gp_path, parallel_num
        output: NA
        """
        scpCmd = "source /etc/profile"
        outputCollect = ""
        if hostList is None:
            hostList = []
        try:
            if env_file != "":
                mpprcFile = env_file
            else:
                mpprcFile = DefaultValue.getEnv(DefaultValue.MPPRC_FILE_ENV)
            if mpprcFile != "" and mpprcFile is not None:
                scpCmd += " && source %s" % mpprcFile

            if gp_path == "":
                cmdpre = "%s && echo $GPHOME" % scpCmd
                (status, output) = subprocess.getstatusoutput(cmdpre)
                if status != 0 or not output or output.strip() == "":
                    raise Exception(ErrorCode.GAUSS_518["GAUSS_51802"]
                                    % "GPHOME" + "The cmd is %s" % cmdpre)
                GPHOME = output.strip()
            else:
                GPHOME = gp_path.strip()
            pscppre = "python3 %s/script/gspylib/pssh/bin/pscp" % GPHOME

            if len(hostList) == 0:
                scpCmd += " && %s -r -v -t %s -p %s -h %s -o %s -e %s %s %s" \
                          " 2>&1 | tee %s" % (pscppre, self.__timeout,
                                              parallel_num, self.__hostsFile,
                                              self.__outputPath,
                                              self.__errorPath, srcFile,
                                              targetDir, self.__resultFile)
                hostList = self.hostNames
            else:
                scpCmd += " && %s -r -v -t %s -p %s -H %s -o %s -e %s %s %s" \
                          " 2>&1 | tee %s" % (pscppre, self.__timeout,
                                              parallel_num,
                                              " -H ".join(hostList),
                                              self.__outputPath,
                                              self.__errorPath, srcFile,
                                              targetDir, self.__resultFile)
            (status, output) = subprocess.getstatusoutput(scpCmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50216"]
                                % ("file [%s]" % srcFile) +
                                " To directory: %s."
                                % targetDir + " Error:\n%s" % output)
            if output.find("Timed out") > 0:
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % scpCmd
                                + " Error:\n%s" % output)

            # ip and host name should match here
            resultMap, outputCollect = self.parseSshResult(hostList)
        except Exception as e:
            self.clenSshResultFiles()
            raise Exception(str(e))

        for host in hostList:
            if resultMap.get(host) != DefaultValue.SUCCESS:
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50216"]
                                % ("file [%s]" % srcFile) +
                                " To directory: %s." % targetDir +
                                " Command: %s.\nError:\n%s" % (scpCmd,
                                                               outputCollect))

    def checkRemoteFileExist(self, node, fileAbsPath, mpprcFile):
        """
        check remote node exist file
        this method depend on directory permisstion 'x'
        if exist return true,else retrun false
        """
        sshcmd = "if [ -e '%s' ];then echo 'exist tar file yes flag';" \
                 "else echo 'exist tar file no flag';fi" % fileAbsPath
        if node != DefaultValue.GetHostIpOrName():
            outputCollect = self.getSshStatusOutput(sshcmd,
                                                                 [node],
                                                                 mpprcFile)[1]
        else:
            outputCollect = subprocess.getstatusoutput(sshcmd)[1]
        if 'exist tar file yes flag' in outputCollect:
            return True
        elif 'exist tar file no flag' in outputCollect:
            return False
        else:
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % sshcmd
                            + "On node %s" % node)

    def __writeHostFiles(self):
        """
        function: Write all hostname to a file
        input : NA
        output: NA
        """
        try:
            g_file.createFileInSafeMode(self.__hostsFile)
            with open(self.__hostsFile, "w") as fp:
                for host in self.hostNames:
                    fp.write("%s\n" % host)
                fp.flush()
            subprocess.getstatusoutput("chmod %s '%s'"
                                       % (DefaultValue.FILE_MODE,
                                          self.__hostsFile))
        except Exception as e:
            g_file.removeFile(self.__hostsFile)
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50205"] % "host file"
                            + " Error: \n%s" % str(e))

        # change the mode
        # if it created by root user,and permission is 640, then
        # install user will have no permission to read it, so we should set
        # its permission 644.
        g_file.changeMode(DefaultValue.KEY_HOSTS_FILE, self.__hostsFile, False,
                          "python")

    def __readCmdResult(self, resultFile, hostNum):
        """
        function: Read command result
        input : resultFile, hostNum, cmd
        output: resultMap
        """
        resultMap = {}
        try:
            with open(resultFile, "r") as fp:
                lines = fp.readlines()
            context = "".join(lines)
            for line in lines:
                resultPair = line.strip().split(" ")
                if len(resultPair) >= 4 and resultPair[2] == "[FAILURE]":
                    resultMap[resultPair[3]] = "Failure"
                if len(resultPair) >= 4 and resultPair[2] == "[SUCCESS]":
                    resultMap[resultPair[3]] = "Success"

            if len(resultMap) != hostNum:
                raise Exception(ErrorCode.GAUSS_516["GAUSS_51637"]
                                % ("valid return item number [%d]"
                                   % len(resultMap), "host number[%d]"
                                   % hostNum) + " The return result:\n%s."
                                % context)
        except Exception as e:
            raise Exception(str(e))

        return resultMap

    def setTimeOut(self, timeout):
        """
        function: Set a new timeout value for ssh tool.
        :param timeout: The new timeout value in seconds.
        :return: void
        """
        self.__timeout = timeout
