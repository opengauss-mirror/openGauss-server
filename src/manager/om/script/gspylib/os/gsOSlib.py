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
""" The following platform framework is used to handle any differences between
    the platform's we support.  The GenericPlatform class is the base class
    that a supported platform extends from and overrides any of the methods
    as necessary.
"""

import os
import sys
import subprocess
import pwd
import grp

sys.path.append(sys.path[0] + "/../../")
from gspylib.common.ErrorCode import ErrorCode
from gspylib.common.VersionInfo import VersionInfo
from gspylib.os.gsplatform import g_Platform, findCmdInPath
from gspylib.os.gsfile import g_file

sys.path.append(sys.path[0] + "/../../../lib")
import psutil


class PlatformCommand():
    """
    Command for os
    """

    def __init__(self):
        """
        function : init function
        input  : NA
        output : NA
        """
        pass

    def getDate(self):
        """
        function : Get current system time
        input : NA
        output: String
        """
        dateCmd = g_Platform.getDateCmd() + " -R "
        (status, output) = subprocess.getstatusoutput(dateCmd)
        # if cmd failed, then exit
        if status != 0:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50219"] % "date" +
                            "The cmd is %s" % dateCmd)
        return output

    def getAllCrontab(self):
        """
        function : Get the crontab
        input : NA
        output: status, output
        """
        cmd = g_Platform.getAllCrontabCmd()
        (status, output) = subprocess.getstatusoutput(cmd)
        if output.find("no crontab for") >= 0:
            output = ""
            status = 0
        # if cmd failed, then exit
        if status != 0:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50219"] %
                            "crontab list" + " Error:%s." % output +
                            "The cmd is %s" % cmd)
        return status, output

    def execCrontab(self, path):
        """
        function : Get the crontab
        input : string
        output: True or False
        """
        if not os.path.exists(path):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % path)
        filePath = os.path.dirname(path)
        fileName = os.path.basename(path)
        cmd = g_Platform.getCdCmd(filePath)
        cmd += " && "
        cmd += g_Platform.getCrontabCmd()
        cmd += (" ./%s" % fileName)
        cmd += " && %s" % g_Platform.getCdCmd("-")
        # if cmd failed, then exit
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                            " Error:\n%s" % output)
        return True

    def source(self, path):
        """
        function : Get the source
        input : string
        output: True or False
        """
        cmd = g_Platform.getSourceCmd()
        cmd += " %s" % path
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                            " Error:\n%s" % output)
        return True

    def getGrepValue(self, para="", value="", path=""):
        """
        function : grep value
        input : string,value,path
        output: status, output
        """
        if not os.path.exists(path):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % path)
        cmd = g_Platform.getGrepCmd() + " %s '%s' '%s'" % (para, value, path)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                            " Error:\n%s" % output)
        return status, output

    def getHostName(self):
        """
        function : Get host name
        input : NA
        output: string
        """
        hostCmd = findCmdInPath("hostname")
        (status, output) = subprocess.getstatusoutput(hostCmd)
        # if cmd failed, then exit
        if status != 0:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50219"] % "host name"
                            + "The cmd is %s" % hostCmd)
        return output

    def getSysConfiguration(self):
        """
        function : The size range of PAGE_SIZE obtained by getconf
        input : NA
        output: string
        """
        configCmd = g_Platform.getGetConfValueCmd()
        (status, output) = subprocess.getstatusoutput(configCmd)
        # if cmd failed, then exit
        if status != 0:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50219"] %
                            "system config pagesize" +
                            "The cmd is %s" % configCmd)
        return output

    def getUserLimits(self, limitType):
        """
        function : Get current user process limits
        input : string
        output: string
        """
        limit = g_Platform.getUlimitCmd()
        limitCmd = "%s -a | %s -F '%s'" % (limit, g_Platform.getGrepCmd(),
                                           limitType)
        (status, output) = subprocess.getstatusoutput(limitCmd)
        # if cmd failed, then exit
        if status != 0:
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % limitCmd +
                            " Error:\n%s" % output)
        return output

    def chageExpiryInformation(self, user):
        """
        function : Query user password expiration time
        input : user
        output: True or False
        """
        changeTemp = g_Platform.getPasswordExpiresCmd(user)
        changeCmd = "%s | %s -i '^Password expires'" % \
                    (changeTemp, g_Platform.getGrepCmd())
        (status, output) = subprocess.getstatusoutput(changeCmd)
        # if cmd failed, then exit
        if status != 0:
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % changeCmd +
                            " Error:\n%s" % output)

        expireTime = output.split(":")[1]
        if expireTime.find("never") == 1:
            return False
        else:
            return True

    def getIOStat(self):
        """
        function : Get device IO information
        input : NA
        output: string
        """
        ioStatCmd = g_Platform.getIOStatCmd()
        (status, output) = subprocess.getstatusoutput(ioStatCmd)
        # if cmd failed, then exit
        if status != 0:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50219"] %
                            "IO information" + "The cmd is %s" % ioStatCmd)
        return output

    def scpFile(self, ip, sourcePath, targetPath, copyTo=True):
        """
        function : if copyTo is True, scp files to remote host else,
                   scp files to local host
        input : destination host ip
                source path
                target path
                copyTo
        output: NA
        """
        scpCmd = ""
        if os.path.isdir(sourcePath):
            scpCmd = g_Platform.getRemoteCopyCmd(sourcePath, targetPath, ip,
                                                 copyTo, "directory")
        elif os.path.exists(sourcePath):
            scpCmd = g_Platform.getRemoteCopyCmd(sourcePath, targetPath, ip,
                                                 copyTo)

        (status, output) = subprocess.getstatusoutput(scpCmd)
        # if cmd failed, then exit
        if status != 0:
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % scpCmd +
                            " Error:\n%s" % output)

    def getLocaleInfo(self, para):
        """
        function : Get OS character set information
        input : para
        output: string
        """
        localCmd = "%s | grep '^%s='" % (g_Platform.getLocaleCmd(), para)
        (status, output) = subprocess.getstatusoutput(localCmd)
        # if cmd failed, then exit
        if status != 0:
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % localCmd +
                            " Error:\n%s" % output)
        return output.split("=")[1][1:-1]

    def mangerSysMode(self, operateType, module):
        """
        type: list     --list system module
              load     --load system module
              insert   --insert system module by force
              remove   --remove system module
              dep      --generate modules.dep and map files
        """
        modCmd = g_Platform.getSysModManagementCmd(operateType, module)
        (status, output) = subprocess.getstatusoutput(modCmd)
        # if cmd failed, then exit
        if status != 0:
            raise Exception(str(output) + " The cmd is %s" % modCmd)

    def getSshCommand(self, ip, cmd):
        """
        function : Get ssh command
        input  : null
        output : exe_cmd
        """
        exe_cmd = "%s \"%s\"" % (g_Platform.getSshCmd(ip), cmd)
        return exe_cmd

    def getProcess(self, processKeywords):
        """
        function : Get process id by keywords
        input  : processKeywords
        output : processId
        """
        processId = []
        cmd = g_Platform.getProcessIdByKeyWordsCmd(processKeywords)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status == 0 and str(output.strip()) != "":
            # kill process
            processId = output.strip().split("\n")
        return processId

    def getProcPidList(self, procName):
        """
        function : Get process id by procName
        input  : procName
        output : pidList
        """
        pidList = []
        for pid in psutil.pids():
            try:
                p = psutil.Process(pid)
                if procName == p.name():
                    pidList.append(pid)
            except psutil.NoSuchProcess:
                pass
        return pidList

    def killProcessByProcName(self, procName, killType=2):
        """
        function : Kill the process
        input : int, int
        output : boolean
        """
        try:
            pidList = self.getProcPidList(procName)
            for pid in pidList:
                os.kill(pid, killType)
            return True
        except Exception:
            return False

    def killallProcess(self, userName, procName, killType='2'):
        """
        function : Kill all processes by userName and procName.
        input : userName, procName, killType
        output : boolean
        """
        cmd = "%s >/dev/null 2>&1" % g_Platform.getKillallProcessCmd(killType,
                                                                     userName,
                                                                     procName)
        status = subprocess.getstatusoutput(cmd)[0]
        if status != 0:
            return False
        return True

    def cleanCommunicationStatus(self, user):
        """
        function : clean semaphore
        input  : user
        output : Successful return True,otherwise return false
        """
        cmd = g_Platform.getDeleteSemaphoreCmd(user)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status == 0:
            return True
        else:
            raise Exception(ErrorCode.GAUSS_504["GAUSS_50407"] +
                            " Error: \n%s." % str(output) +
                            "The cmd is %s" % cmd)

    def getUserInfo(self):
        """
        function : Get user information
        input  : null
        output : userInfo
        """
        userInfo = {"uid": os.getuid(), "name": pwd.getpwuid(
            os.getuid()).pw_name,
                    "gid": pwd.getpwuid(os.getuid()).pw_gid}
        userInfo["g_name"] = grp.getgrgid(userInfo["gid"]).gr_name

        return userInfo

    def getDeviceIoctls(self, devName):
        """
        function : Get device ioctls
        input  : devName   device name
        output : blockSize
        """
        blockSize = 0
        cmd = g_Platform.getBlockdevCmd(devName)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_504["GAUSS_50408"] % cmd +
                            " Error: \n%s" % str(output))
        if str(output.strip()) != "" and output.isdigit():
            blockSize = int(output)
        return blockSize

    def addUser(self, userName, groupName):
        """
        function : Add the user
        input  : userName
               : groupName
        output : Successful return True,otherwise return false
        """
        cmd = g_Platform.getUseraddCmd(userName, groupName)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status == 0:
            return True
        else:
            raise Exception(ErrorCode.GAUSS_503["GAUSS_50318"] % userName +
                            " Error: \n%s." % str(output) +
                            "The cmd is %s" % cmd)

    def delUser(self, userName):
        """
        function : Delete the user
        input  : userName
        output : Successful return True,otherwise return false
        """
        cmd = g_Platform.getUserdelCmd(userName)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status == 0:
            return True
        else:
            raise Exception(ErrorCode.GAUSS_503["GAUSS_50314"] % userName +
                            " Error: \n%s." % str(output) +
                            "The cmd is %s" % cmd)

    def addGroup(self, groupName):
        """
        function : Add the group
        input  : groupName
        output : Successful return True,otherwise return false
        """
        cmd = g_Platform.getGroupaddCmd(groupName)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status == 0:
            return True
        else:
            raise Exception(ErrorCode.GAUSS_503["GAUSS_50319"] % groupName +
                            " Error: \n%s." % str(output) +
                            "The cmd is %s" % cmd)

    def delGroup(self, groupName):
        """
        function : delete the group
        input  : groupName
        output : Successful return True,otherwise return false
        """
        cmd = g_Platform.getGroupdelCmd(groupName)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status == 0:
            return True
        else:
            raise Exception(ErrorCode.GAUSS_503["GAUSS_50313"] % groupName +
                            " Error:\n%s." % str(output) +
                            "The cmd is %s" % cmd)

    def getPathOwner(self, pathName):
        """
        function : Get the owner user of path.
        input : pathName
        output : user and group
        """
        user = ""
        group = ""
        # check path
        if not os.path.exists(pathName):
            return user, group
        # get use and group information
        try:
            user = pwd.getpwuid(os.stat(pathName).st_uid).pw_name
            group = grp.getgrgid(os.stat(pathName).st_gid).gr_name
            return user, group
        except Exception:
            return "", ""

    def getPackageFile(self, fileType="tarFile"):
        """
        function : Get the path of binary file version.
        input : NA
        output : String
        """
        (distName, version) = g_Platform.getCurrentPlatForm()
        return g_Platform.getPackageFile(distName, version,
                                         VersionInfo.getPackageVersion(),
                                         VersionInfo.PRODUCT_NAME_PACKAGE,
                                         fileType)

    def getTarFilePath(self):
        """
        function : Get the path of binary file version.
        input : NA
        output : str
        """
        return self.getPackageFile("tarFile")

    def getBz2FilePath(self):
        """
        function : Get the path of binary file version.
        input : NA
        output : str
        """
        return self.getPackageFile("bz2File")

    def getBinFilePath(self):
        """
        function : Get the path of binary file version..
        input : NA
        output : str
        """
        return self.getPackageFile("binFile")

    def getSHA256FilePath(self):
        """
        function : Get the path of sha256 file version..
        input : NA
        output : str
        """
        return self.getPackageFile("sha256File")

    def getFileSHA256Info(self):
        """
        function: get file sha256 info
        input:  NA
        output: str, str
        """
        try:
            bz2Path = self.getBz2FilePath()
            sha256Path = self.getSHA256FilePath()

            fileSHA256 = g_file.getFileSHA256(bz2Path)
            valueList = g_file.readFile(sha256Path)
            if len(valueList) != 1:
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50204"] %
                                sha256Path)
            sha256Value = valueList[0].strip()
            return fileSHA256, sha256Value
        except Exception as e:
            raise Exception(str(e))

    def checkLink(self, filePath):
        """
        function:check if file is a link
        input: filePath
        output:NA
        """
        if os.path.exists(filePath):
            if os.path.islink(filePath):
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50210"] % filePath)

    def getGroupByUser(self, user):
        """
        function : get group by user
        input : user
        output : group
        """
        try:
            group = grp.getgrgid(pwd.getpwnam(user).pw_gid).gr_name
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_503["GAUSS_50300"] % user +
                            "Detail msg: %s" % str(e))
        return group

    def getPortProcessInfo(self, port):
        """
        function : get port occupation process
        input : port
        output : process info
        """
        try:
            processInfo = ""
            cmd = "netstat -an | grep -w %s" % port
            output = subprocess.getstatusoutput(cmd)[1]
            processInfo += "%s\n" % output
            return processInfo
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                            " Error:\n%s" % str(e))


g_OSlib = PlatformCommand()
