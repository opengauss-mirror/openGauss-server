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
import pwd
import sys
import getpass

sys.path.append(sys.path[0] + "/../")

from gspylib.common.DbClusterInfo import dbClusterInfo
from gspylib.common.ErrorCode import ErrorCode
from gspylib.common.Common import ClusterCommand, DefaultValue
from gspylib.common.OMCommand import OMCommand
from gspylib.os.gsfile import g_file
from multiprocessing.dummy import Pool as ThreadPool

# action name
# prepare cluster tool package path
ACTION_PREPARE_PATH = "prepare_path"
# check the OS version
ACTION_CHECK_OS_VERSION = "check_os_Version"
# create os user
ACTION_CREATE_OS_USER = "create_os_user"
# check os user
ACTION_CHECK_OS_USER = "check_os_user"
# create cluster path
ACTION_CREATE_CLUSTER_PATHS = "create_cluster_paths"
# set the os parameters
ACTION_SET_OS_PARAMETER = "set_os_parameter"
# set finish flag
ACTION_SET_FINISH_FLAG = "set_finish_flag"
# set the user environment variable
ACTION_SET_USER_ENV = "set_user_env"
# set the tools environment variable
ACTION_SET_TOOL_ENV = "set_tool_env"
# prepare CRON service
ACTION_PREPARE_USER_CRON_SERVICE = "prepare_user_cron_service"
# prepare ssh service
ACTION_PREPARE_USER_SSHD_SERVICE = "prepare_user_sshd_service"
# set the dynamic link library
ACTION_SET_LIBRARY = "set_library"
# set sctp service
ACTION_SET_SCTP = "set_sctp"
# set virtual Ip
ACTION_SET_VIRTUALIP = "set_virtualIp"
# clean virtual Ip
ACTION_CLEAN_VIRTUALIP = "clean_virtualIp"
# check hostname on all nodes
ACTION_CHECK_HOSTNAME_MAPPING = "check_hostname_mapping"
# write /etc/hosts flag
HOSTS_MAPPING_FLAG = "#Gauss OM IP Hosts Mapping"
# init Gausslog
ACTION_INIT_GAUSSLOG = "init_gausslog"
# check envfile
ACTION_CHECK_ENVFILE = "check_envfile"
# check path owner
ACTION_CHECK_DIR_OWNER = "check_dir_owner"
# check os software
ACTION_CHECK_OS_SOFTWARE = "check_os_software"
#############################################################################
# Global variables
#   self.context.logger: globle logger
#   self.context.clusterInfo: global clueter information
#   self.context.sshTool: globle ssh tool interface
#   g_warningTpye: warning type
#############################################################################
iphostInfo = ""
topToolPath = ""
# create the tmp file for dist trust steps
g_stepTrustTmpFile = None
# the tmp file name
TRUST_TMP_FILE = "step_preinstall_file.dat"
# the tmp file path
TRUST_TMP_FILE_DIR = None
createTrustFlag = False


class PreinstallImpl:
    """
    init the command options
    save command line parameter values
    """

    def __init__(self, preinstall):
        """
        function: constructor
        """
        self.context = preinstall

    def installToolsPhase1(self):
        """
        function: install tools to local machine
        input: NA
        output: NA
        """
        pass

    def checkMpprcFile(self):
        """
        function: Check mpprc file path
        input : NA
        output: NA
        """
        clusterPath = []
        # get the all directorys list about cluster in the xml file
        dirs = self.context.clusterInfo.getClusterDirectorys()
        for checkdir in list(dirs.values()):
            # append directory to clusterPath
            clusterPath.extend(checkdir)
        # get tool path
        clusterPath.append(self.context.clusterToolPath)
        # get tmp path
        clusterPath.append(
            dbClusterInfo.readClusterTmpMppdbPath(self.context.user,
                                                  self.context.xmlFile))
        self.context.logger.debug("Cluster paths %s." % clusterPath,
                                  "constant")
        # check directory
        g_file.checkIsInDirectory(self.context.mpprcFile, clusterPath)

    def getUserName(self):
        """
        function: get the user name
        input: NA
        output: str
        """
        return os.environ.get('LOGNAME') or os.environ.get('USER')

    def getUserPasswd(self, name, point=""):
        """
        function:
            get user passwd
        input: name, point
        output: str
        """
        if point == "":
            self.context.logger.log("Please enter password for %s." % name,
                                    "constant")
        else:
            self.context.logger.log(
                "Please enter password for %s %s." % (name, point), "constant")
        passwdone = getpass.getpass()

        return passwdone

    def checkRootPasswd(self, ip):
        """
        function:check the root passwd is correct or not
        input:node ip
        output:NA

        """
        ssh = None
        try:
            import paramiko
        except ImportError as e:
            raise Exception(ErrorCode.GAUSS_522["GAUSS_52200"] % str(e))
        try:
            # ssh the ip
            ssh = paramiko.Transport((ip, 22))
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_506["GAUSS_50603"] + "IP: %s" % ip)
        try:
            ssh.connect(username="root", password=self.context.rootPasswd)
        except Exception as e:
            raise Exception(
                ErrorCode.GAUSS_503["GAUSS_50306"] % ip
                + " Maybe communication is exception, please check "
                  "the password and communication."
                + " Error: \nWrong password or communication is abnormal.")
        finally:
            if ssh is not None:
                ssh.close()

    def twoMoreChancesForRootPasswd(self):
        """
        function:for better user experience,
         if the root password is wrong, two more chances should be given
        input:ip list of all hosts
        output:NA
        """
        # save the sshIps
        Ips = []
        # get the user sshIps
        sshIps = self.context.clusterInfo.getClusterSshIps()
        # save the sshIps to Ips
        for ips in sshIps:
            Ips.extend(ips)
        times = 0
        while True:
            try:
                # get the number of concurrent processes
                pool = ThreadPool(DefaultValue.getCpuSet())
                # start the concurrent processes
                ipHostname = pool.map(self.checkRootPasswd, Ips)
                # close the pool
                pool.close()
                # wait the return from concurrent processes
                pool.join()
                break
            except Exception as e:
                if str(e).find("The IP address is invalid") != -1:
                    raise Exception(str(e))
                if times == 2:
                    raise Exception(str(e))
                self.context.logger.log(
                    "Password authentication failed, please try again.")
                self.context.rootPasswd = getpass.getpass()
                times += 1

    def createTrustForRoot(self):
        """
        function:
          create SSH trust for user who call this script with root privilege
        precondition:
          1.create SSH trust tool has been installed on local host
        postcondition:
          caller's SSH trust has been created
        input: NA
        output: NA
        hideninfo:NA
        """
        if self.context.localMode or self.context.isSingle:
            return

        try:
            # check the interactive mode
            # if interactive is True
            if not self.context.preMode:
                # Ask to create trust for root
                flag = input(
                    "Are you sure you want "
                    "to create trust for root (yes/no)? ")
                while True:
                    # If it is not yes or no, it has been imported
                    # if it is yes or no, it has been break
                    if (
                            flag.upper() != "YES"
                            and flag.upper() != "NO"
                            and flag.upper() != "Y" and flag.upper() != "N"):
                        flag = input("Please type 'yes' or 'no': ")
                        continue
                    break

                # Confirm that the user needs to be created trust for root
                # Receives the entered password
                if flag.upper() == "YES" or flag.upper() == "Y":
                    self.context.rootPasswd = self.getUserPasswd("root")
                    # check passwd, if wrong, then give two more chances
                    self.twoMoreChancesForRootPasswd()

            # save the distribute
            result = {}
            # save the sshIps
            Ips = []
            # create trust for root
            # get the user name
            username = pwd.getpwuid(os.getuid()).pw_name
            # get the user sshIps
            sshIps = self.context.clusterInfo.getClusterSshIps()
            # save the sshIps to Ips
            for ips in sshIps:
                Ips.extend(ips)

            Hosts = []
            # get the sshIps and node name
            for node in self.context.clusterInfo.dbNodes:
                Hosts.append(node.name)
                for ip in node.sshIps:
                    result[ip] = node.name
            # get the all hostname
            iphostnamedict = self.getAllHosts(Ips, self.context.rootPasswd)
            # check the hostname and node name
            checkResult = self.checkIpHostname(result, iphostnamedict)
            # if check failed, then exit
            if checkResult != DefaultValue.SUCCESS:
                raise Exception(checkResult)

            # write the /etc/hosts
            if not self.context.skipHostnameSet:
                # write the ip and hostname to /etc/hosts
                self.writeLocalHosts(result)
                if self.context.rootPasswd == "":
                    # write the /etc/hosts to remote node
                    self.writeRemoteHosts(result)

            # if not provide root passwd,
            # then do not create SSH trust for root user
            if not self.context.preMode:
                if self.context.rootPasswd != "":
                    self.context.logger.log(
                        "Creating SSH trust for the root permission user.")
                    self.context.sshTool.createTrust(
                        username,
                        self.context.rootPasswd,
                        Ips,
                        self.context.mpprcFile,
                        self.context.skipHostnameSet)
                    g_file.changeMode(DefaultValue.HOSTS_FILE, "/etc/hosts",
                                      False, "shell")

        except Exception as e:
            raise Exception(str(e))
        if self.context.rootPasswd != "":
            self.context.logger.log(
                "Successfully created SSH trust for the root permission user.")

    def getAllHostName(self, ip):
        """
        function:
          Connect to all nodes ,then get all hostaname by threading
        precondition:
          1.User's password is correct on each node
        postcondition:
           NA
        input: ip
        output:Dictionary ipHostname,key is IP  and value is hostname
        hideninfo:NA
        """
        # ip and hostname
        ipHostname = {}
        # user name
        username = pwd.getpwuid(os.getuid()).pw_name
        try:
            # load paramiko
            import paramiko
        except ImportError as e:
            raise Exception(ErrorCode.GAUSS_522["GAUSS_52200"] % str(e))
        try:
            # ssh the ip
            ssh = paramiko.Transport((ip, 22))
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_506["GAUSS_50603"] + "IP: %s" % ip)
        try:
            # connect
            ssh.connect(username=username, password=self.context.rootPasswd)
        except Exception as e:
            ssh.close()
            raise Exception(
                ErrorCode.GAUSS_503["GAUSS_50306"] % ip
                + " Maybe communication is exception, "
                  "please check the password and communication."
                + " Error: \nWrong password or communication is abnormal.")

        check_channel = ssh.open_session()
        cmd = "cd"
        check_channel.exec_command(cmd)
        channel_read = ""
        env_msg = check_channel.recv_stderr(9999).decode()
        while True:
            channel_read = check_channel.recv(9999).decode().strip()
            if len(channel_read) != 0:
                env_msg += str(channel_read)
            else:
                break
        if env_msg != "":
            ipHostname[
                "Node[%s]" % ip] = \
                "Output: [" \
                + env_msg \
                + " ] print by /etc/profile or ~/.bashrc, please check it."
            return ipHostname

        # get hostname
        cmd = "hostname"
        channel = ssh.open_session()
        # exec the hostname on remote node
        channel.exec_command(cmd)
        # recv the result from remote node
        hostname = channel.recv(9999).decode().strip()
        # save the hostname
        ipHostname[ip] = hostname
        # close ssh
        ssh.close()

        return ipHostname

    def getAllHosts(self, sshIps, passwd):
        """
        function:
          Connect to all nodes ,then get all hostaname
        precondition:
          1.User's password is correct on each node
        postcondition:
           NA
        input: sshIps,passwd
        output:Dictionary ipHostname,key is IP  and value is hostname
        hideninfo:NA
        """
        # ip and hostname
        # the result for return
        result = {}
        if passwd != "":
            try:
                # get the number of concurrent processes
                pool = ThreadPool(DefaultValue.getCpuSet())
                # start the concurrent processes
                ipHostname = pool.map(self.getAllHostName, sshIps)
                # close the pool
                pool.close()
                # wait the return from concurrent processes
                pool.join()
            except Exception as e:
                if str(e) == "":
                    raise Exception(
                        ErrorCode.GAUSS_511["GAUSS_51101"]
                        % "communication may be abnormal.")
                else:
                    raise Exception(str(e))

            # save the hostname to result
            err_msg = ""
            for i in ipHostname:
                for (key, value) in list(i.items()):
                    if key.find("Node") >= 0:
                        err_msg += str(i)
                    else:
                        result[key] = value
            if len(err_msg) > 0:
                raise Exception(ErrorCode.GAUSS_518["GAUSS_51808"] % err_msg)
        # if the passwd is null
        else:
            cmd = "source /etc/profile " \
                  "&& if [ -f  ~/.bashrc ]; then source ~/.bashrc; fi"
            if self.context.mpprcFile != "":
                cmd += "&& if [ -f '%s' ]; then source '%s'; fi" % (
                    self.context.mpprcFile, self.context.mpprcFile)
            # send the cmd to sshIps
            # check the trust and envfile
            self.context.sshTool.executeCommand(cmd,
                                                "check cluster trust",
                                                DefaultValue.SUCCESS,
                                                sshIps,
                                                self.context.mpprcFile,
                                                checkenv=True)

            pssh_path = os.path.join(os.path.dirname(__file__),
                                     "../../gspylib/pssh/bin/pssh")
            for sship in sshIps:
                # set the cmd
                cmd = "%s -s -H %s hostname 2>/dev/null" % (pssh_path, sship)
                # exec the command
                (status, output) = subprocess.getstatusoutput(cmd)
                # if cmd failed, then exit
                if status != 0:

                    raise Exception(ErrorCode.GAUSS_516["GAUSS_51618"]
                                    + "The cmd is %s " % cmd)
                result[sship] = output

        return result

    def checkIpHostname(self, srcList, tgtList):
        """
        function:
          Checking the hostname and IP is matched or not .
        precondition:
          NA
        postcondition:
           NA
        input: srcList,tgtList
        output: retValue ,if srclist and tgtlist is same ,
        then return Success  else return Warning message.
        hideninfo:NA
        """
        retValue = ""
        # Checking the hostname and IP is matched or not
        for (key, value) in list(srcList.items()):
            if srcList[key] != tgtList[key]:
                retValue = retValue + ErrorCode.GAUSS_524["GAUSS_52402"] % (
                    key, value)

        if retValue == "":
            # the result of check is OK
            retValue = DefaultValue.SUCCESS
        return retValue

    def writeLocalHosts(self, result):
        """
        function:
         Write hostname and Ip into /etc/hosts
         when there's not the same one in /etc/hosts file
        precondition:
          NA
        postcondition:
           NA
        input: Dictionary result,key is IP and value is hostname
        output: NA
        hideninfo:NA
        """
        writeResult = []
        hostIPList = []
        hostIPInfo = ""
        # the temporary Files for /etc/hosts
        tmp_hostipname = "./tmp_hostsiphostname_%d" % os.getpid()
        # Delete the line with 'HOSTS_MAPPING_FLAG' in the /etc/hosts
        cmd = "grep -v '%s' %s > %s ; cp %s %s && rm -rf '%s'" % \
              ("#Gauss.* IP Hosts Mapping", '/etc/hosts', tmp_hostipname,
               tmp_hostipname, '/etc/hosts', tmp_hostipname)
        (status, output) = DefaultValue.retryGetstatusoutput(cmd)
        # if cmd failed, append the output to writeResult
        if status != 0:
            g_file.removeFile(tmp_hostipname)
            writeResult.append(output)
        # cmd OK
        else:
            for (key, value) in list(result.items()):
                # set the string
                hostIPInfo = '%s  %s  %s' % (key, value, HOSTS_MAPPING_FLAG)
                hostIPList.append(hostIPInfo)
            # write the ip and hostname to /etc/hosts
            g_file.writeFile("/etc/hosts", hostIPList, mode="a+")

    def writeRemoteHosts(self, result):
        """
        function:
         Write hostname and Ip into /etc/hosts
         when there's not the same one in /etc/hosts file
        precondition:
          NA
        postcondition:
           NA
        input: Dictionary result,key is IP and value is hostname
                    rootPasswd
        output: NA
        hideninfo:NA
        """
        # IP and hostname
        global iphostInfo
        iphostInfo = ""
        # remote hosts
        remoteHosts = []

        # set the str for write into /etc/hosts
        for (key, value) in list(result.items()):
            iphostInfo += '%s  %s  %s\n' % (key, value, HOSTS_MAPPING_FLAG)
            if value != DefaultValue.GetHostIpOrName():
                remoteHosts.append(value)
        remoteHosts1 = list(set(remoteHosts))
        iphostInfo = iphostInfo[:-1]
        if len(remoteHosts1) == 0:
            return
        # the temporary Files for /etc/hosts
        tmp_hostipname = "./tmp_hostsiphostname_%d" % os.getpid()
        # Delete the line with 'HOSTS_MAPPING_FLAG' in the /etc/hosts
        cmd = "if [ -f '%s' ]; then grep -v '%s' %s > %s " \
              "; cp %s %s ; rm -rf '%s'; fi" % \
              ('/etc/hosts', "#Gauss.* IP Hosts Mapping", '/etc/hosts',
               tmp_hostipname, tmp_hostipname, '/etc/hosts', tmp_hostipname)
        # exec the cmd on all remote nodes
        self.context.sshTool.executeCommand(cmd,
                                            "grep /etc/hosts",
                                            DefaultValue.SUCCESS,
                                            remoteHosts1,
                                            self.context.mpprcFile)

        # write the iphostInfo into /etc/hosts on all remote nodes
        cmd = "echo '%s'  >> /etc/hosts" % iphostInfo
        self.context.sshTool.executeCommand(cmd,
                                            "write /etc/hosts",
                                            DefaultValue.SUCCESS,
                                            remoteHosts1,
                                            self.context.mpprcFile)

    def distributePackages(self):
        """
        function:
          distribute packages and xml to all nodes of cluster
        precondition:
          1.packages and xml exist on local host
          2.root SSH trust has been created
        postcondition:
          1.packages and xml exist on all hosts
          2.os user can access package and xml
        input:NA
        output:NA
        information hiding:
          1.the package and xml path
          2.node names
        ppp:
        check and create the server package path
        make compressed server package
        send server package
        Decompress package on every host
        change mode of packages
        check and create the xml path
        send xml
        change mode of xml file
        check and create the tool package path
        make compressed tool package
        send tool package
        change mode of packages
        """
        if self.context.localMode or self.context.isSingle:
            return

        self.context.logger.log("Distributing package.", "addStep")
        try:
            self.makeCompressedToolPackage(self.context.clusterToolPath)

            # get the all node names in xml file
            hosts = self.context.clusterInfo.getClusterNodeNames()
            # remove the local node name
            hosts.remove(DefaultValue.GetHostIpOrName())
            self.getTopToolPath(self.context.sshTool,
                                self.context.clusterToolPath, hosts,
                                self.context.mpprcFile)

            # Delete the old bak package in GPHOME before copy the new one.
            for bakPack in DefaultValue.PACKAGE_BACK_LIST:
                bakFile = os.path.join(self.context.clusterToolPath, bakPack)
                cmd = g_file.SHELL_CMD_DICT["deleteFile"] % (bakFile, bakFile)
                self.context.logger.debug(
                    "Command for deleting bak-package: %s." % cmd)
                (status, output) = self.context.sshTool.getSshStatusOutput(
                    cmd, hosts)
                for ret in list(status.values()):
                    if ret != DefaultValue.SUCCESS:
                        self.context.logger.debug(
                            "Failed delete bak-package, result: %s." % output)

            # Retry 3 times, if distribute failed.
            for i in range(3):
                try:
                    self.context.logger.log(
                        "Begin to distribute package to tool path.")
                    # Send compressed package to every host
                    DefaultValue.distributePackagesToRemote(
                        self.context.sshTool,
                        self.context.clusterToolPath,
                        self.context.clusterToolPath,
                        hosts,
                        self.context.mpprcFile,
                        self.context.clusterInfo)
                    # Decompress package on every host
                except Exception as e:
                    # loop 3 times, if still wrong, exit with error code.
                    if i == 2:
                        raise Exception(str(e))
                    # If error, continue loop.
                    self.context.logger.log(
                        "Distributing package failed, retry.")
                    continue
                # If scp success, exit loop.
                self.context.logger.log(
                    "Successfully distribute package to tool path.")
                break
            # 2.distribute gauss server package
            # Get the path to the server package
            dirName = os.path.dirname(os.path.realpath(__file__))
            packageDir = os.path.join(dirName, "./../../../")
            packageDir = os.path.normpath(packageDir)
            for i in range(3):
                try:
                    self.context.logger.log(
                        "Begin to distribute package to package path.")
                    # distribute the distribute package to all node names
                    DefaultValue.distributePackagesToRemote(
                        self.context.sshTool,
                        self.context.clusterToolPath,
                        packageDir,
                        hosts,
                        self.context.mpprcFile,
                        self.context.clusterInfo)
                except Exception as e:
                    # loop 3 times, if still wrong, exit with error code.
                    if i == 2:
                        raise Exception(str(e))
                    # If error, continue loop.
                    self.context.logger.log(
                        "Distributing package failed, retry.")
                    continue
                # If scp success, exit loop.
                self.context.logger.log(
                    "Successfully distribute package to package path.")
                break
            # 3.distribute xml file
            DefaultValue.distributeXmlConfFile(self.context.sshTool,
                                               self.context.xmlFile, hosts,
                                               self.context.mpprcFile)
        except Exception as e:
            raise Exception(str(e))

        self.context.logger.log("Successfully distributed package.",
                                "constant")

    def makeCompressedToolPackage(self, path):
        """
        function: make compressed tool package
        input  : path
        output : NA
        """
        pass

    def getTopToolPath(self, top_sshTool, clusterToolPath, hosts, mpprcFile):
        """
        function: find the top path of GPHOME in remote nodes.
        input: top_sshTool, clusterToolPath, hosts, mpprcFile
        output: NA
        """
        # get the String of each path & split it with space.
        global topToolPath
        topToolPath = {}
        pathList = clusterToolPath.split("/")
        pathStr = ""
        # get the string of GPHOME, split it by white spaces
        for path in pathList:
            if path == pathList[0]:
                pathStr = "/"
            elif path == pathList[1]:
                pathNext = "/" + path
                pathStr = pathNext
            else:
                pathNext = pathNext + "/" + path
                pathStr += " " + pathNext

        # use the shell command to get top path of gausstool
        cmd = "str='%s'; for item in \$str; " \
              "do if [ ! -d \$item ]; then TopPath=\$item; " \
              "break; fi; done; echo \$TopPath" % (
                  pathStr)
        top_sshTool.getSshStatusOutput(cmd, hosts, mpprcFile)
        outputMap = top_sshTool.parseSshOutput(hosts)
        for node in list(outputMap.keys()):
            topToolList = outputMap[node].split("\n")
            topToolPath[node] = topToolList[0]

    def fixServerPackageOwner(self):
        """
        function: when distribute server package,
        the os user has not been created, so we should fix
                  server package Owner here after user create.
        input: NA
        output: NA
        """
        pass

    def installToolsPhase2(self):
        """
        function: install the tools
        input: NA
        output: NA
        """
        # check if path have permission.
        if self.context.localMode or self.context.isSingle:
            # fix new created path's owner
            for onePath in self.context.needFixOwnerPaths:
                g_file.changeOwner(self.context.user, onePath, recursive=True,
                                   cmdType="shell")
            return

        self.context.logger.log("Installing the tools in the cluster.",
                                "addStep")
        try:
            self.context.logger.debug(
                "Paths need to be fixed owner:%s."
                % self.context.needFixOwnerPaths)
            # fix new created path's owner
            for onePath in self.context.needFixOwnerPaths:
                g_file.changeOwner(self.context.user, onePath, recursive=True,
                                   cmdType="shell")

            # fix remote toolpath's owner
            for node in list(topToolPath.keys()):
                nodelist = []
                nodelist.append(node)
                if os.path.exists(topToolPath[node]):
                    cmd = "chown -R %s:%s '%s'" % (
                        self.context.user, self.context.group,
                        topToolPath[node])
                    self.context.sshTool.executeCommand(
                        cmd,
                        "authorize top tool path",
                        DefaultValue.SUCCESS,
                        nodelist,
                        self.context.mpprcFile)

            # chown chmod top path file
            dirName = os.path.dirname(self.context.logFile)
            topDirFile = "%s/topDirPath.dat" % dirName
            cmd = "(if [ -f '%s' ];then cat '%s' " \
                  "| awk -F = '{print $1}' " \
                  "| xargs chown -R %s:%s; rm -rf '%s';fi)" % \
                  (topDirFile, topDirFile, self.context.user,
                   self.context.group, topDirFile)
            self.context.sshTool.executeCommand(cmd,
                                                "authorize top path",
                                                DefaultValue.SUCCESS,
                                                [],
                                                self.context.mpprcFile)

            # change owner of packages
            self.context.logger.debug("Changing package path permission.")
            dirName = os.path.dirname(os.path.realpath(__file__))
            packageDir = os.path.realpath(
                os.path.join(dirName, "./../../../")) + "/"

            list_dir = g_file.getDirectoryList(packageDir)
            for directory in list_dir:
                dirPath = packageDir + directory
                dirPath = os.path.normpath(dirPath)
                if directory.find('sudo') >= 0:
                    continue
                g_file.changeOwner(self.context.user, dirPath, recursive=True,
                                   cmdType="python")

            # check enter permission
            cmd = "su - %s -c 'cd '%s''" % (self.context.user, packageDir)
            (status, output) = subprocess.getstatusoutput(cmd)
            # if cmd failed, then exit
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                                + " Error: \n%s" % output)

            # change owner of GaussLog dir
            self.context.logger.debug("Changing the owner of Gauss log path.")
            user_dir = "%s/%s" % (
                self.context.clusterInfo.logPath, self.context.user)
            # the user_dir may not been created now,
            # so we need check its exists
            if os.path.exists(user_dir):

                g_file.changeOwner(self.context.user, user_dir, recursive=True,
                                   cmdType="shell", retryFlag=True,
                                   retryTime=15, waiteTime=1)

                # check enter permission
                cmd = "su - %s -c 'cd '%s''" % (self.context.user, user_dir)
                (status, output) = subprocess.getstatusoutput(cmd)
                # if cmd failed, then exit
                if status != 0:
                    raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                                    + " Error: \n%s" % output)
            # user can specify log file,
            # so we need change the owner of log file alonely
            g_file.changeOwner(self.context.user, self.context.logger.logFile,
                               recursive=False, cmdType="shell")
            g_file.changeMode(DefaultValue.FILE_MODE,
                              self.context.logger.logFile, recursive=False,
                              cmdType="shell")

            # check enter permission
            log_file_dir = os.path.dirname(self.context.logger.logFile)
            cmd = "su - %s -c 'cd '%s''" % (self.context.user, log_file_dir)
            (status, output) = subprocess.getstatusoutput(cmd)
            # if cmd failed, then exit
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                                + " Error: \n%s" % output)

            # set tool env on all hosts
            cmd = "%s -t %s -u %s -l %s -X '%s' -Q %s" % (
                OMCommand.getLocalScript("Local_PreInstall"),
                ACTION_SET_TOOL_ENV,
                self.context.user,
                self.context.localLog,
                self.context.xmlFile,
                self.context.clusterToolPath)
            if self.context.mpprcFile != "":
                cmd += " -s '%s' -g %s" % (
                    self.context.mpprcFile, self.context.group)
            self.context.sshTool.executeCommand(cmd,
                                                "set cluster tool ENV",
                                                DefaultValue.SUCCESS,
                                                [],
                                                self.context.mpprcFile)
            cmd = "%s -t %s -u %s -g %s -P %s -l %s" % (
                OMCommand.getLocalScript("Local_PreInstall"),
                ACTION_PREPARE_PATH,
                self.context.user,
                self.context.group,
                self.context.clusterToolPath,
                self.context.localLog)
            # prepare cluster tool package path
            self.context.sshTool.executeCommand(
                cmd,
                "prepare cluster tool package path",
                DefaultValue.SUCCESS,
                [],
                self.context.mpprcFile)
        except Exception as e:
            raise Exception(str(e))

        self.context.logger.log(
            "Successfully installed the tools in the cluster.", "constant")

    def checkMappingForHostName(self):
        """
        function: check mpping for hostname
        input: NA
        output: NA
        """
        if self.context.localMode or self.context.isSingle:
            return

        self.context.logger.log("Checking hostname mapping.", "addStep")
        try:
            # check hostname mapping
            cmd = "%s -t %s -u %s -X '%s' -l '%s'" % (
                OMCommand.getLocalScript("Local_PreInstall"),
                ACTION_CHECK_HOSTNAME_MAPPING,
                self.context.user,
                self.context.xmlFile,
                self.context.localLog)
            self.context.sshTool.executeCommand(cmd,
                                                "check hostname mapping",
                                                DefaultValue.SUCCESS,
                                                [],
                                                self.context.mpprcFile,
                                                DefaultValue.getCpuSet())
        except Exception as e:
            raise Exception(str(e))

        self.context.logger.log("Successfully checked hostname mapping.",
                                "constant")

    def createTrustForCommonUser(self):
        """
        function:
          create SSH trust for common user
        precodition:
          config file /etc/hosts has been modified correctly on local host
        input: NA
        output: NA
        """
        if self.context.localMode or self.context.isSingle:
            return

        if createTrustFlag:
            return
        self.context.logger.log(
            "Creating SSH trust for [%s] user." % self.context.user)
        try:
            # the IP for create trust
            allIps = []
            sshIps = self.context.clusterInfo.getClusterSshIps()
            # get all IPs
            for ips in sshIps:
                allIps.extend(ips)
            # create trust
            self.context.sshTool.createTrust(self.context.user,
                                             self.context.password, allIps,
                                             self.context.mpprcFile)
        except Exception as e:
            raise Exception(str(e))
        self.context.logger.log(
            "Successfully created SSH trust for [%s] user."
            % self.context.user)

    def checkOSVersion(self):
        """
        function:
          check if os version is support
        precondition:
        postcondition:
        input:NA
        output:NA
        hiden info:support os version
        ppp:
        """
        self.context.logger.log("Checking OS version.", "addStep")
        try:
            # Checking OS version
            cmd = "%s -t %s -u %s -l %s" % (
                OMCommand.getLocalScript("Local_PreInstall"),
                ACTION_CHECK_OS_VERSION,
                self.context.user,
                self.context.localLog)
            DefaultValue.execCommandWithMode(
                cmd,
                "check OS version",
                self.context.sshTool,
                self.context.localMode or self.context.isSingle,
                self.context.mpprcFile)
        except Exception as e:
            raise Exception(str(e))
        self.context.logger.log("Successfully checked OS version.", "constant")

    def createOSUser(self):
        """
        function:
          create os user and create trust for user
        precondition:
          1.user group passwd has been initialized
          2.create trust tool has been installed
        postcondition:
          1.user has been created
          2.user's trust has been created
        input:NA
        output:NA
        hiden:NA
        """
        # single cluster also need to create user without local mode
        self.context.logger.debug("Creating OS user and create trust for user")
        if self.context.localMode:
            return

        global createTrustFlag
        try:
            # check the interactive mode
            # if the interactive mode is True
            if not self.context.preMode:
                try:
                    # get the input
                    if self.context.localMode:
                        flag = input(
                            "Are you sure you want to "
                            "create the user[%s] (yes/no)? "
                            % self.context.user)
                    else:
                        flag = input(
                            "Are you sure you want to create "
                            "the user[%s] and create trust for it (yes/no)? "
                            % self.context.user)
                    while True:
                        # check the input
                        if (
                                flag.upper() != "YES"
                                and flag.upper() != "NO"
                                and flag.upper() != "Y"
                                and flag.upper() != "N"):
                            flag = input("Please type 'yes' or 'no': ")
                            continue
                        break

                    # set the flag for create user trust
                    self.context.logger.debug(
                        "Setting the flag for creating user's trust.")
                    if flag.upper() == "NO" or flag.upper() == "N":
                        createTrustFlag = True
                        cmd = "%s -t %s -u %s -l %s" % (
                            OMCommand.getLocalScript("Local_PreInstall"),
                            ACTION_INIT_GAUSSLOG,
                            self.context.user,
                            self.context.localLog)
                        DefaultValue.execCommandWithMode(
                            cmd,
                            "init gausslog",
                            self.context.sshTool,
                            self.context.isSingle,
                            self.context.mpprcFile)
                        return
                    # check the user is not exist on all nodes
                    cmd = "%s -t %s -u %s -g %s -l %s" % (
                        OMCommand.getLocalScript("Local_PreInstall"),
                        ACTION_CHECK_OS_USER,
                        self.context.user,
                        self.context.group,
                        self.context.localLog)
                    DefaultValue.execCommandWithMode(cmd,
                                                     "check OS user",
                                                     self.context.sshTool,
                                                     self.context.isSingle,
                                                     self.context.mpprcFile)
                    self.context.logger.debug(
                        "Successfully set the flag for creating user's trust")
                    return
                except Exception as e:
                    i = 0
                    # get the password
                    while i < 3:
                        self.context.password = self.getUserPasswd(
                            "cluster user")
                        DefaultValue.checkPasswordVaild(
                            self.context.password,
                            self.context.user,
                            self.context.clusterInfo)
                        self.context.passwordsec = self.getUserPasswd(
                            "cluster user", "again")

                        if self.context.password != self.context.passwordsec:
                            i = i + 1
                            self.context.logger.printMessage(
                                "Sorry. passwords do not match.")
                            continue
                        break

                    # check the password is not OK
                    if i == 3:
                        self.context.logger.printMessage(
                            "passwd: Have exhausted maximum number "
                            "of retries for service.")
                        sys.exit(1)
            else:
                createTrustFlag = True
                cmd = "%s -t %s -u %s -l %s" % (
                    OMCommand.getLocalScript("Local_PreInstall"),
                    ACTION_INIT_GAUSSLOG,
                    self.context.user,
                    self.context.localLog)
                DefaultValue.execCommandWithMode(cmd,
                                                 "init gausslog",
                                                 self.context.sshTool,
                                                 self.context.isSingle,
                                                 self.context.mpprcFile)
                return

            self.context.logger.debug(
                "Successfully created [%s] user on all nodes."
                % self.context.user)

            # create the user on all nodes
            # write the password into temporary file
            tmp_file = "/tmp/temp.%s" % self.context.user
            g_file.createFileInSafeMode(tmp_file)
            with open("/tmp/temp.%s" % self.context.user, "w") as fp:
                fp.write(self.context.password)
                fp.flush()
            # change the temporary file permissions
            g_file.changeMode(DefaultValue.KEY_FILE_MODE, tmp_file,
                              recursive=False, cmdType="shell")

            if not self.context.isSingle:
                # send the temporary file to all remote nodes
                try:
                    self.context.sshTool.scpFiles(
                        tmp_file, "/tmp/",
                        self.context.sshTool.hostNames)
                except Exception as e:
                    cmd = "(if [ -f '/tmp/temp.%s' ];" \
                          "then rm -f '/tmp/temp.%s';fi)" % (
                              self.context.user, self.context.user)
                    DefaultValue.execCommandWithMode(cmd,
                                                     "delete temporary files",
                                                     self.context.sshTool,
                                                     self.context.isSingle,
                                                     self.context.mpprcFile)
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50216"]
                                    % "temporary files")

            # create the user on all nodes
            cmd = "%s -t %s -u %s -g %s -l %s" % (
                OMCommand.getLocalScript("Local_PreInstall"),
                ACTION_CREATE_OS_USER,
                self.context.user,
                self.context.group,
                self.context.localLog)
            DefaultValue.execCommandWithMode(cmd,
                                             "create OS user",
                                             self.context.sshTool,
                                             self.context.isSingle,
                                             self.context.mpprcFile)

            # delete the temporary file on all nodes
            cmd = "(if [ -f '/tmp/temp.%s' ];then rm -f '/tmp/temp.%s';fi)" \
                  % (self.context.user, self.context.user)
            DefaultValue.execCommandWithMode(cmd,
                                             "delete temporary files",
                                             self.context.sshTool,
                                             self.context.isSingle,
                                             self.context.mpprcFile)

            # Successfully created user on all nodes
            self.context.logger.log(
                "Successfully created [%s] user on all nodes."
                % self.context.user)
        except Exception as e:
            # delete the temporary file on all nodes
            cmd = "(if [ -f '/tmp/temp.%s' ];then rm -f '/tmp/temp.%s';fi)" \
                  % (self.context.user, self.context.user)
            DefaultValue.execCommandWithMode(cmd,
                                             "delete temporary files",
                                             self.context.sshTool,
                                             self.context.isSingle,
                                             self.context.mpprcFile)
            raise Exception(str(e))

    def createDirs(self):
        """
        function: create directorys
        input: NA
        output: NA
        """
        self.context.logger.log("Creating cluster's path.", "addStep")
        try:
            # fix new created path's owner after create user for single cluster
            if self.context.isSingle:
                self.context.logger.debug(
                    "Paths need to be fixed owner:%s."
                    % self.context.needFixOwnerPaths)
                for onePath in self.context.needFixOwnerPaths:
                    g_file.changeOwner(self.context.user, onePath,
                                       recursive=True, cmdType="shell")

                dirName = os.path.dirname(self.context.logFile)
                topDirFile = "%s/topDirPath.dat" % dirName
                if os.path.exists(topDirFile):
                    keylist = g_file.readFile(topDirFile)
                    if keylist != []:
                        for key in keylist:
                            g_file.changeOwner(self.context.user, key.strip(),
                                               True, "shell")

                    g_file.removeFile(topDirFile)

            # create the directory on all nodes
            cmd = "%s -t %s -u %s -g %s -X '%s' -l '%s'" % (
                OMCommand.getLocalScript("Local_PreInstall"),
                ACTION_CREATE_CLUSTER_PATHS,
                self.context.user,
                self.context.group,
                self.context.xmlFile,
                self.context.localLog)
            # check the env file
            if self.context.mpprcFile != "":
                cmd += " -s '%s'" % self.context.mpprcFile
            # exec the cmd
            DefaultValue.execCommandWithMode(
                cmd,
                "create cluster's path",
                self.context.sshTool,
                self.context.localMode or self.context.isSingle,
                self.context.mpprcFile)
        except Exception as e:
            raise Exception(str(e))
        self.context.logger.log("Successfully created cluster's path.",
                                "constant")

    def setAndCheckOSParameter(self):
        """
        function: set and check OS parameter.
        If skipOSSet is true, pass; else call gs_checkos to do it.
        input: NA
        output: NA
        """
        self.context.logger.log("Set and check OS parameter.", "addStep")
        try:
            # get all node hostnames
            NodeNames = self.context.clusterInfo.getClusterNodeNames()
            namelist = ""

            # set the localmode
            if self.context.localMode or self.context.isSingle:
                # localmode
                namelist = DefaultValue.GetHostIpOrName()
            else:
                # Non-native mode
                namelist = ",".join(NodeNames)

            # check skip-os-set parameter
            if self.context.skipOSSet:
                # check the OS parameters
                self.checkOSParameter(namelist)
            else:
                # set and check parameters
                self.setOSParameter(namelist)
                self.checkOSParameter(namelist)
        except Exception as e:
            raise Exception(str(e))
        self.context.logger.log("Set and check OS parameter completed.",
                                "constant")

    def setOSParameter(self, namelist):
        """
        function: set and check OS parameter.
        If skipOSSet is true, pass; else call gs_checkos to do it.
        input: namelist
        output: NA
        """
        self.context.logger.log("Setting OS parameters.")

        # set OS parameters
        cmd = "%s -h %s -i B -l '%s' -X '%s'" % (
            OMCommand.getLocalScript("Gauss_CheckOS"),
            namelist,
            self.context.localLog,
            self.context.xmlFile)
        (status, output) = subprocess.getstatusoutput(cmd)
        # if cmd failed, then raise
        if status != 0 and output.strip() == "":
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                            + "Error:\n%s." % output)

        self.context.logger.log("Successfully set OS parameters.")

    def checkOSParameter(self, namelist):
        """
        check OS parameter.
        If skipOSSet is true, pass; else call gs_checkos to do it.
        """
        self.context.logger.debug("Checking OS parameters.")
        try:
            # check the OS parameters
            cmd = "%s -h %s -i A -l '%s' -X '%s'" % (
                OMCommand.getLocalScript("Gauss_CheckOS"),
                namelist,
                self.context.localLog,
                self.context.xmlFile)
            (status, output) = subprocess.getstatusoutput(cmd)
            # if cmd failed, then raise
            if status != 0 and output.strip() == "":
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                                + "Error:\n%s." % output)

            # parse the result
            result = ""
            abnormal_num = 0
            warning_num = 0
            # get the total numbers
            for line in output.split('\n'):
                if line.find("Total numbers") >= 0:
                    result = line
                    break
            if result == "":
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                                + "Error:\n%s." % output)
            # type [Total numbers:14. Abnormal numbers:0. Warning number:1.]
            try:
                # get the abnormal numbers
                abnormal_num = int(result.split('.')[1].split(':')[1].strip())
                # get the warning numbers
                warning_num = int(result.split('.')[2].split(':')[1].strip())
            except Exception as e:
                abnormal_num = 1
                warning_num = 0

            # get the path where  the script is located
            current_path = os.path.join(
                os.path.dirname(os.path.realpath(__file__)), "./../../")
            gs_checkos_path = os.path.realpath(
                os.path.join(current_path, "gs_checkos"))
            if abnormal_num > 0:
                raise Exception(
                    ErrorCode.GAUSS_524["GAUSS_52400"]
                    + "\nPlease get more details by \"%s "
                      "-i A -h %s --detail\"."
                    % (gs_checkos_path, namelist))
            if warning_num > 0:
                self.context.logger.log(
                    "Warning: Installation environment "
                    "contains some warning messages." + \
                    "\nPlease get more details by \"%s "
                    "-i A -h %s --detail\"."
                    % (gs_checkos_path, namelist))

        except Exception as e:
            raise Exception(str(e))

        self.context.logger.debug("Successfully check OS parameters.")

    def prepareCronService(self):
        """
        function: preparing CRON service
        input: NA
        output: NA
        """
        self.context.logger.log("Preparing CRON service.", "addStep")
        try:
            # Preparing CRON service
            cmd = "%s -t %s -u %s -l %s" % (
                OMCommand.getLocalScript("Local_PreInstall"),
                ACTION_PREPARE_USER_CRON_SERVICE,
                self.context.user,
                self.context.localLog)
            DefaultValue.execCommandWithMode(
                cmd,
                "prepare CRON service",
                self.context.sshTool,
                self.context.localMode or self.context.isSingle,
                self.context.mpprcFile)
        except Exception as e:
            raise Exception(str(e))
        # Successfully prepared CRON service
        self.context.logger.log("Successfully prepared CRON service.",
                                "constant")

    def prepareSshdService(self):
        """
        function: preparing SSH service
        input: NA
        output: NA
        """
        self.context.logger.log("Preparing SSH service.", "addStep")
        try:
            # Preparing SSH service
            cmd = "%s -t %s -u %s -X %s -l %s" % (
                OMCommand.getLocalScript("Local_PreInstall"),
                ACTION_PREPARE_USER_SSHD_SERVICE,
                self.context.user,
                self.context.xmlFile,
                self.context.localLog)
            DefaultValue.execCommandWithMode(
                cmd,
                "prepare SSH service",
                self.context.sshTool,
                self.context.localMode or self.context.isSingle,
                self.context.mpprcFile)
        except Exception as e:
            raise Exception(str(e))
        # Successfully prepared SSH service
        self.context.logger.log("Successfully prepared SSH service.",
                                "constant")

    def setEnvParameter(self):
        """
        function: setting cluster environmental variables
        input: NA
        output: NA
        """
        pass

    def setLibrary(self):
        """
        function: setting the dynamic link library
        input: NA
        output: NA
        """
        self.context.logger.log("Setting the dynamic link library.", "addStep")
        try:
            # Setting the dynamic link library
            cmd = "%s -t %s -u %s -l %s " % (
                OMCommand.getLocalScript("Local_PreInstall"),
                ACTION_SET_LIBRARY,
                self.context.user,
                self.context.localLog)
            self.context.logger.debug("Command for setting library: %s" % cmd)
            # exec the cmd for set library
            DefaultValue.execCommandWithMode(
                cmd,
                "set library",
                self.context.sshTool,
                self.context.localMode or self.context.isSingle,
                self.context.mpprcFile)
        except Exception as e:
            # failed to set the dynamic link library
            raise Exception(str(e))
        # Successfully set the dynamic link library
        self.context.logger.log("Successfully set the dynamic link library.",
                                "constant")

    def setCorePath(self):
        """
        function: setting core path
        input: NA
        output: NA
        """
        pass

    def setPssh(self):
        """
        function: setting pssh
        input: NA
        output: NA
        """
        pass

    def setSctp(self):
        """
        function: setting SCTP service
        input: NA
        output: NA
        """
        self.context.logger.log("Setting SCTP service.", "addStep")
        try:
            # set SCTP service
            cmd = "%s -t %s -u %s -l %s" % (
                OMCommand.getLocalScript("Local_PreInstall"),
                ACTION_SET_SCTP,
                self.context.user,
                self.context.localLog)
            # check the mpprcFile
            if self.context.mpprcFile != "":
                cmd += " -s '%s'" % self.context.mpprcFile
            self.context.logger.debug("Command for setting SCTP: %s" % cmd)

            # exec cmd for set SCTP
            DefaultValue.execCommandWithMode(
                cmd,
                "set SCTP",
                self.context.sshTool,
                self.context.localMode or self.context.isSingle,
                self.context.mpprcFile)
        except Exception as e:
            # failed set SCTP service
            raise Exception(str(e))
        # Successfully set SCTP service
        self.context.logger.log("Successfully set SCTP service.", "constant")

    def setVirtualIp(self):
        """
        function: set the virtual IPs
        input: NA
        output: NA
        """
        pass

    def doPreInstallSucceed(self):
        """
        function: setting finish flag
        input: NA
        output: NA
        """
        # Before set finish flag,
        # we need to check if path permission is correct in local mode.
        self.checkLocalPermission()

        self.context.logger.log("Setting finish flag.", "addStep")
        try:
            # set finish flag
            cmd = "%s -t %s -u %s -l '%s' -X '%s' -Q %s" % (
                OMCommand.getLocalScript("Local_PreInstall"),
                ACTION_SET_FINISH_FLAG,
                self.context.user,
                self.context.localLog,
                self.context.xmlFile,
                self.context.clusterToolPath)
            # check the mpprcFile
            if self.context.mpprcFile != "":
                cmd += " -s '%s'" % self.context.mpprcFile
            # exec the cmd for set finish flag
            DefaultValue.execCommandWithMode(
                cmd,
                "setting finish flag",
                self.context.sshTool,
                self.context.localMode or self.context.isSingle,
                self.context.mpprcFile)
        except Exception as e:
            # failed set finish flag
            raise Exception(str(e))
        # Successfully set finish flag
        self.context.logger.log("Successfully set finish flag.", "constant")

    def checkLocalPermission(self):
        """
        function: check if path have permission in local mode or single mode.
        input : NA
        output: NA
        """
        # check if path have permission in local mode or single mode.
        if self.context.localMode or self.context.isSingle:
            dirName = os.path.dirname(os.path.realpath(__file__))
            packageDir = os.path.realpath(
                os.path.join(dirName, "./../../../")) + "/"

            # check enter permission
            cmd = "su - %s -c 'cd '%s''" % (self.context.user, packageDir)
            (status, output) = subprocess.getstatusoutput(cmd)
            # if cmd failed, then exit
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                                + " Error: \n%s" % output)

            user_dir = "%s/%s" % (
                self.context.clusterInfo.logPath, self.context.user)

            # the user_dir may not been created now,
            # so we need check its exists
            if os.path.exists(user_dir):
                # check enter permission
                cmd = "su - %s -c 'cd '%s''" % (self.context.user, user_dir)
                (status, output) = subprocess.getstatusoutput(cmd)
                # if cmd failed, then exit
                if status != 0:
                    raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                                    + " Error: \n%s" % output)

            # check enter permission
            log_file_dir = os.path.dirname(self.context.logger.logFile)

            cmd = "su - %s -c 'cd '%s''" % (self.context.user, log_file_dir)
            (status, output) = subprocess.getstatusoutput(cmd)
            # if cmd failed, then exit
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                                + " Error: \n%s" % output)

    def createStepTmpFile(self):
        """
        function: create step tmp file
        input : NA
        output: NA
        """
        if self.context.localMode or self.context.isSingle:
            return

        try:
            global g_stepTrustTmpFile
            global TRUST_TMP_FILE_DIR
            TRUST_TMP_FILE_DIR = "/tmp/%s" % TRUST_TMP_FILE
            g_file.createFileInSafeMode(TRUST_TMP_FILE_DIR)
            with open(TRUST_TMP_FILE_DIR, "w") as g_stepTrustTmpFile:
                g_stepTrustTmpFile.flush()
        except Exception as e:
            raise Exception(str(e))

    def deleteStepTmpFile(self):
        """
        function: delete step tmp file
        input : NA
        output: NA
        """
        if self.context.localMode or self.context.isSingle:
            return

        try:
            cmd = "rm -rf '%s'" % TRUST_TMP_FILE_DIR
            self.context.sshTool.executeCommand(cmd, "delete step tmp file")
        except Exception as e:
            self.context.logger.error(str(e))

    def checkEnvFile(self):
        """
        function: delete step tmp file
        input : NA
        output: NA
        """
        if self.context.localMode or self.context.isSingle:
            return

        try:
            cmd = "%s -t %s -u %s -l %s" % (
                OMCommand.getLocalScript("Local_PreInstall"),
                ACTION_CHECK_ENVFILE,
                self.context.user,
                self.context.localLog)
            if self.context.mpprcFile != "":
                cmd += " -s '%s'" % self.context.mpprcFile
            self.context.sshTool.executeCommand(cmd, "delete step tmp file")
        except Exception as e:
            raise Exception(str(e))

    def checkDiskSpace(self):
        """
        function: check remain disk space of GAUSSHOME for olap
        input: NA
        output: NA
        """
        pass

    def setHostIpEnv(self):
        """
        function: set host ip env
        input  : NA
        output : NA
        """
        pass

    def checkRepeat(self):
        """
        function: check repeat
        input  : NA
        output : NA
        """
        gphome = gausshome = pghost = gausslog \
            = agent_path = agent_log_path = ""
        if self.context.mpprcFile and os.path.isfile(self.context.mpprcFile):
            source_file = self.context.mpprcFile
        elif self.context.mpprcFile:
            self.context.logger.debug(
                "Environment file is not exist environment file,"
                " skip check repeat.")
            return
        elif os.path.isfile(
                os.path.join("/home", "%s/.bashrc" % self.context.user)):
            source_file = os.path.join("/home",
                                       "%s/.bashrc" % self.context.user)
        else:
            self.context.logger.debug(
                "There is no environment file, skip check repeat.")
            return
        with open(source_file, 'r') as f:
            env_list = f.readlines()
        new_env_list = []
        if not self.context.mpprcFile:
            with open(os.path.join("/etc", "profile"), "r") as etc_file:
                gp_home_env = etc_file.readlines()
            gphome_env_list = [env.replace('\n', '') for env in gp_home_env]
            for env in gphome_env_list:
                if env.startswith("export GPHOME="):
                    if len(new_env_list) != 0:
                        new_env_list = []
                    new_env_list.append(env.strip())

        new_env_list.extend([env.replace('\n', '') for env in env_list])
        if "export GAUSS_ENV=2" not in new_env_list:
            self.context.logger.debug(
                "There is no install cluster exist. "
                "Skip check repeat install.")
            return
        for env in new_env_list:
            if env.startswith("export GPHOME=") and env.split('=')[1] != "":
                gphome = env.split('=')[1]
            if env.startswith("export GAUSSHOME="):
                gausshome = env.split('=')[1]
            if env.startswith("export PGHOST="):
                pghost = env.split('=')[1]
            if env.startswith("export GAUSSLOG="):
                gausslog = env.split('=')[1]
            if env.startswith("export AGENTPATH="):
                agent_path = env.split('=')[1]
            if env.startswith("export AGENTLOGPATH="):
                agent_log_path = env.split('=')[1]

        gaussdbToolPath = DefaultValue.getPreClusterToolPath(
            self.context.user,
            self.context.xmlFile)
        gaussdbAppPath = self.context.getOneClusterConfigItem(
            "gaussdbAppPath",
            self.context.xmlFile)
        DefaultValue.checkPathVaild(gaussdbAppPath)
        tmpMppdbPath = self.context.clusterInfo.readClusterTmpMppdbPath(
            self.context.user, self.context.xmlFile)
        gaussdbLogPath = self.context.clusterInfo.readClusterLogPath(
            self.context.xmlFile)
        agentToolPath = self.context.getOneClusterConfigItem(
            "agentToolPath",
            self.context.xmlFile)
        DefaultValue.checkPathVaild(agentToolPath)
        agentLogPath = self.context.getOneClusterConfigItem(
            "agentLogPath",
            self.context.xmlFile)
        DefaultValue.checkPathVaild(agentLogPath)
        if gphome and gphome.strip() != gaussdbToolPath:
            raise Exception(
                ErrorCode.GAUSS_527["GAUSS_52704"] % "preinstall repeat" +
                "gaussdbToolPath [%s] is not same with environment[%s]" % (
                    gaussdbToolPath, gphome))
        if gausshome and gausshome.strip() != gaussdbAppPath:
            raise Exception(
                ErrorCode.GAUSS_527["GAUSS_52704"] % "preinstall repeat" +
                "gaussdbAppPath [%s] is not same with environment[%s]" % (
                    gaussdbAppPath, gausshome))
        if pghost and pghost.strip() != tmpMppdbPath:
            raise Exception(
                ErrorCode.GAUSS_527["GAUSS_52704"] % "preinstall repeat" +
                "tmpMppdbPath [%s] is not same with environment[%s]" % (
                    tmpMppdbPath, pghost))
        if gausslog and gausslog.strip() != os.path.join(
                gaussdbLogPath.strip(), self.context.user):
            raise Exception(
                ErrorCode.GAUSS_527["GAUSS_52704"] % "preinstall repeat" +
                "gaussdbLogPath [%s] is not same with environment[%s]"
                % (os.path.join(gaussdbLogPath.strip(), self.context.user),
                   gausslog))
        if agent_path and agentToolPath \
                and agent_path.strip() != agentToolPath.strip():
            raise Exception(
                ErrorCode.GAUSS_527["GAUSS_52704"] % "preinstall repeat" +
                "agentToolPath [%s] is not same with environment[%s]" % (
                    agentToolPath, agent_path))
        if agent_log_path \
                and agentLogPath \
                and agent_log_path.strip() != agentLogPath.strip():
            raise Exception(
                ErrorCode.GAUSS_527["GAUSS_52704"] % "preinstall repeat" +
                "agentLogPath [%s] is not same with environment[%s]" % (
                    agentLogPath, agent_log_path))

        self.context.logger.debug("Preinstall check repeat success.")

    def checkInstanceDir(self):
        """
        function : Check whether the instance path is in the gausshome path
        input : None
        output : None
        """
        appPath = self.context.clusterInfo.appPath
        self.checkRepeat()
        for dbNode in self.context.clusterInfo.dbNodes:
            # dn
            for dataInst in dbNode.datanodes:
                if os.path.dirname(dataInst.datadir) == appPath:
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50232"] % (
                        dataInst.datadir, appPath))

    def checkOSSoftware(self):
        """
        function: setting the dynamic link library
        input: NA
        output: NA
        """
        self.context.logger.log("Checking OS software.", "addStep")
        try:
            # Checking software
            cmd = "%s -t %s -u %s -l %s " % (
                OMCommand.getLocalScript("Local_PreInstall"),
                ACTION_CHECK_OS_SOFTWARE,
                self.context.user,
                self.context.localLog)
            self.context.logger.debug("Checking OS software: %s" % cmd)
            # exec the cmd for Checking software
            DefaultValue.execCommandWithMode(
                cmd,
                "check software",
                self.context.sshTool,
                self.context.localMode or self.context.isSingle,
                self.context.mpprcFile)
        except Exception as e:
            # failed to Check software
            raise Exception(str(e))
        # Successfully Check software
        self.context.logger.log("Successfully check os software.",
                                "constant")

    def get_package_path(self):
        """
        get package path, then can get script path, /package_path/script/
        :return:
        """
        dir_name = os.path.dirname(os.path.realpath(__file__))
        package_dir = os.path.join(dir_name, "./../../../")
        return os.path.realpath(package_dir)

    def doPreInstall(self):
        """
        function: the main process of preinstall
        input: NA
        output: NA
        """
        self.context.logger.debug(
            "gs_preinstall execution takes %s steps in total" % \
            ClusterCommand.countTotalSteps(
                "gs_preinstall", "",
                self.context.localMode or self.context.isSingle))
        # Check whether the instance directory
        # conflicts with the application directory.
        self.checkInstanceDir()
        # install tools phase1
        self.installToolsPhase1()

        # no need do the following steps in local mode
        # create tmp file
        self.createStepTmpFile()
        # exchange user key for root user
        self.createTrustForRoot()
        # distribute server package
        # set HOST_IP env
        self.setHostIpEnv()
        self.distributePackages()
        # create user and exchange keys for database user
        self.createOSUser()
        # prepare sshd service for user.
        # This step must be nearly after createOSUser,
        # which needs sshd service to be restarted.
        self.prepareSshdService()
        # check env file
        self.checkEnvFile()
        # install tools phase2
        self.installToolsPhase2()
        # check whether the /etc/hosts file correct
        self.checkMappingForHostName()
        # exchage user key for common user
        self.createTrustForCommonUser()
        # delete tmp file
        self.deleteStepTmpFile()
        # the end of functions which do not use in in local mode
        #check software
        self.checkOSSoftware()
        # check os version
        self.checkOSVersion()
        # create path and set mode
        self.createDirs()

        # set Sctp
        if not DefaultValue.checkDockerEnv():
            self.setSctp()
        # set os parameters
        self.setAndCheckOSParameter()
        # prepare cron service for user
        self.prepareCronService()
        # set environment parameters
        self.setEnvParameter()
        # set virtual IP
        self.setVirtualIp()
        # set Library
        self.setLibrary()
        # set core path
        self.setCorePath()
        # set core path
        self.setPssh()
        self.setArmOptimization()
        # fix server package mode
        self.fixServerPackageOwner()

        # set user env and a flag,
        # indicate that the preinstall.py has been execed succeed
        self.doPreInstallSucceed()

        self.context.logger.log("Preinstallation succeeded.")

    def run(self):
        """
        function: run method
        """
        try:
            # do preinstall option
            self.doPreInstall()
            # close log file
            self.context.logger.closeLog()
        except Exception as e:
            self.deleteStepTmpFile()
            for rmPath in self.context.needFixOwnerPaths:
                if os.path.isfile(rmPath):
                    g_file.removeFile(rmPath)
                elif os.path.isdir(rmPath):
                    g_file.removeDirectory(rmPath)
            self.context.logger.logExit(str(e))
        sys.exit(0)
