# -*- coding:utf-8 -*-
#############################################################################
# Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
# Portions Copyright (c) 1999-2000, Marc-Andre Lemburg; mailto:mal@lemburg.com
# Portions Copyright (c) 2000-2010, eGenix.com Software GmbH; mailto:info@egenix.com
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
# Description  : gsplatform.py is a utility to do something for
#               platform information.
#############################################################################

""" The following platform framework is used to handle any differences between
    the platform's we support.  The GenericPlatform class is the base class
    that a supported platform extends from and overrides any of the methods
    as necessary.
"""

import os
import sys
import re
import subprocess
import platform
import socket
import time

sys.path.append(sys.path[0] + "/../../")
from gspylib.common.ErrorCode import ErrorCode

localDirPath = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, localDirPath + "/../../../lib/netifaces/")
sys.path.append(localDirPath + "/../inspection/lib/netifaces/")
try:
    from netifaces import interfaces, ifaddresses, AF_INET, AF_INET6
except ImportError as e:
    # get python unicode value. The current environment python is compiled
    # with UCS2 or UCS4.
    # 1114111 is UCS4
    # 65535 is UCS2
    flagNum = 4 if sys.maxunicode == 1114111 else 2
    omToolsNetifacesPath = os.path.join(
        localDirPath, "./../../../lib/netifaces/netifaces.so")
    inspectToolsNetifacesPath = os.path.join(
        localDirPath, "./../../../script/gspylib/inspection/\
        lib/netifaces/netifaces.so")
    newPythonDependNetifacesPath = "%s_UCS%d" % (omToolsNetifacesPath,
                                                 flagNum)
    glo_cmd = "rm -f '%s' && " \
              "cp -f -p '%s' '%s' " % (omToolsNetifacesPath,
                                       newPythonDependNetifacesPath,
                                       omToolsNetifacesPath)
    glo_cmd += " && rm -f '%s' && " \
               "cp -f -p '%s' '%s' " % (inspectToolsNetifacesPath,
                                        newPythonDependNetifacesPath,
                                        inspectToolsNetifacesPath)
    flagExce = True
    for retryNum in range(3):
        (statusExec, outputExec) = subprocess.getstatusoutput(glo_cmd)
        if statusExec != 0:
            flagExce = False
            time.sleep(1)
        else:
            flagExce = True
            break
    if not flagExce:
        raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % glo_cmd
                        + "Error:\n%s" % outputExec)
    from netifaces import interfaces, ifaddresses, AF_INET, AF_INET6

# ---------------platforms--------------------
# global variable for our platform
_supported_dists = (
    'SuSE', 'debian', 'fedora', 'redhat', 'centos', 'euleros', "openEuler",
    'mandrake', 'mandriva', 'rocks', 'slackware', 'yellowdog', 'gentoo',
    'UnitedLinux', 'turbolinux')
_release_filename = re.compile(r'(\w+)[-_](release|version)')
_lsb_release_version = re.compile(r'(.+)'
                                  ' release '
                                  '([\d.]+)'
                                  '[^(]*(?:\((.+)\))?')
_release_version = re.compile(r'([^0-9]+)'
                              '(?: release )?'
                              '([\d.]+)'
                              '[^(]*(?:\((.+)\))?')
SUSE = "suse"
REDHAT = "redhat"
CENTOS = "centos"
EULEROS = "euleros"
KYLIN = "kylin"
OPENEULER = "openeuler"
ASIANUX = "asianux"
SUPPORT_WHOLE_PLATFORM_LIST = [SUSE, REDHAT, CENTOS, EULEROS, OPENEULER, KYLIN, ASIANUX]
# RedhatX platform
SUPPORT_RHEL_SERIES_PLATFORM_LIST = [REDHAT, CENTOS, "kylin", "asianux"]
SUPPORT_RHEL6X_VERSION_LIST = ["6.4", "6.5", "6.6", "6.7", "6.8", "6.9", "10"]
SUPPORT_RHEL7X_VERSION_LIST = ["7.0", "7.1", "7.2", "7.3", "7.4", "7.5", "7.6", "10"]
SUPPORT_RHEL_SERIES_VERSION_LIST = (SUPPORT_RHEL6X_VERSION_LIST +
                                    SUPPORT_RHEL7X_VERSION_LIST)
# EulerOS 2.3 -> 2.0 SP3
SUPPORT_EULEROS_VERSION_LIST = ["2.0"]
# SuSE platform
SUSE11 = "11"
SUSE12 = "12"
SUPPORT_SUSE_VERSION_LIST = [SUSE11, SUSE12]
SUPPORT_SUSE11X_VERSION_LIST = ["1", "2", "3", "4"]
SUPPORT_RHEL12X_VERSION_LIST = ["0", "1", "2", "3"]
BIT_VERSION = "64bit"

# ---------------command path--------------------
CMD_PATH = ['/bin', '/usr/local/bin', '/usr/bin', '/sbin', '/usr/sbin']
CMD_CACHE = {}
BLANK_SPACE = " "
COLON = ":"
# Need to be consistent with the packaging script
PAK_CENTOS = "CentOS"
PAK_EULER = "Euler"
PAK_OPENEULER = "openEuler"
PAK_REDHAT = "RedHat"
PAK_ASIANUX = "asianux"


#######################################################
_supported_dists = (
    'SuSE', 'debian', 'fedora', 'redhat', 'centos', 'euleros', "openEuler",
    'mandrake', 'mandriva', 'rocks', 'slackware', 'yellowdog', 'gentoo',
    'UnitedLinux', 'turbolinux', 'kylin', 'asianux')
_release_filename = re.compile(r'(\w+)[-_](release|version)')
_lsb_release_version = re.compile(r'(.+)'
                                  ' release '
                                  '([\d.]+)'
                                  '[^(]*(?:\((.+)\))?')
_release_version = re.compile(r'([^0-9]+)'
                              '(?: release )?'
                              '([\d.]+)'
                              '[^(]*(?:\((.+)\))?')


def _parse_release_file(firstline):
    """
    Default to empty 'version' and 'id' strings.  Both defaults are used
    when 'firstline' is empty.  'id' defaults to empty when an id can not
    be deduced.
    """
    version = ''
    idNum = ''

    # Parse the first line
    m = _lsb_release_version.match(firstline)
    if m is not None:
        # LSB format: "distro release x.x (codename)"
        return tuple(m.groups())

    # Pre-LSB format: "distro x.x (codename)"
    m = _release_version.match(firstline)
    if m is not None:
        return tuple(m.groups())

    # Unkown format... take the first two words
    l = str.split(str.strip(firstline))
    if l:
        version = l[0]
        if len(l) > 1:
            idNum = l[1]
    return '', version, idNum


def linux_distribution(distname='', version='', idNum='',

                       supported_dists=_supported_dists,
                       full_distribution_name=1):
    """
    Tries to determine the name of the Linux OS distribution name.

        The function first looks for a distribution release file in
        /etc and then reverts to _dist_try_harder() in case no
        suitable files are found.

        supported_dists may be given to define the set of Linux
        distributions to look for. It defaults to a list of currently
        supported Linux distributions identified by their release file
        name.

        If full_distribution_name is true (default), the full
        distribution read from the OS is returned. Otherwise the short
        name taken from supported_dists is used.

        Returns a tuple (distname,version,id) which default to the
        args given as parameters.

    """
    try:
        etc = os.listdir('/etc')
    except os.error:
        # Probably not a Unix system
        return distname, version, idNum
    sorted(etc)
    gFile = None
    for file in etc:
        if os.path.islink('/etc/' + file):
            continue
        m = _release_filename.match(file)
        if m is not None:
            _distname, dummy = m.groups()
            if _distname in supported_dists:
                gFile = file
                distname = _distname
                break

    # Read the first line
    if gFile is None:
        return distname, version, idNum
    with open('/etc/' + gFile, 'r') as f:
        firstline = f.readline()
    _distname, _version, _id = _parse_release_file(firstline)

    if _distname and full_distribution_name:
        distname = _distname
    if _version:
        version = _version
    if _id:
        idNum = _id
    return distname, version, idNum


def dist(supported_dists=_supported_dists):
    """ Tries to determine the name of the Linux OS distribution name.

        The function first looks for a distribution release file in
        /etc and then reverts to _dist_try_harder() in case no
        suitable files are found.

        Returns a tuple (distname,version,id) which default to the
        args given as parameters.

    """
    return linux_distribution(supported_dists=supported_dists,
                              full_distribution_name=0)


# ------------------platform module----------------------
class CommandNotFoundException(Exception):
    """
    """

    def __init__(self, cmd, paths):
        """
        function: constructor
        """
        self.cmd = cmd
        self.paths = paths

    def __str__(self):
        """
        function: str
        input  : NA
        output : NA
        """
        return "Could not locate command: '%s' in this " \
               "set of paths: %s" % (self.cmd, repr(self.paths))


def findCmdInPath(cmd, additionalPaths=None, printError=True):
    """
    function: find cmd in path
    input: cmd, additionalPaths, printError
    output: NA
    """
    global CMD_CACHE
    if additionalPaths is None:
        additionalPaths = []
    if cmd not in CMD_CACHE:
        # Search additional paths and don't add to cache.
        for p in additionalPaths:
            f = os.path.join(p, cmd)
            if os.path.exists(f):
                return f

        for p in CMD_PATH:
            f = os.path.join(p, cmd)
            if os.path.exists(f):
                CMD_CACHE[cmd] = f
                return f

        if cmd == "killall":
            gphome = os.getenv("GPHOME")
            if gphome is None or \
                    not os.path.exists(os.path.join(gphome, "script/killall")):
                gphome = os.path.dirname(os.path.realpath(__file__))\
                         + "/../../.."
            gphome = gphome.replace("\\", "\\\\").replace('"', '\\"\\"')
            for rac in ["|", ";", "&", "$", "<", ">", "`", "\\", "'", "\"",
                        "{", "}", "(", ")", "[", "]", "~", "*",
                        "?", " ", "!", "\n"]:
                if rac in gphome:
                    raise Exception(
                        ErrorCode.GAUSS_502["GAUSS_50219"] % gphome
                        + " There are illegal characters in the path.")
            if gphome != "" and os.path.exists(os.path.join(gphome,
                                                            "script/killall")):
                return os.path.join(gphome, "script/killall")
            else:
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % "killall")

        if printError:
            print('Command %s not found' % cmd)
        search_path = CMD_PATH[:]
        search_path.extend(additionalPaths)
        raise CommandNotFoundException(cmd, search_path)
    else:
        return CMD_CACHE[cmd]


# Requirements:
# 1. ulimit, ntpq, source, kerberos is not found under system path


class GenericPlatform:
    """
    manage OS command,config or service for muti-platform
    """

    def __init__(self):
        """
        function: constructor
        """
        pass

    def echoCmdWithNoReturn(self, line, filePath):
        """
        function: echo cmd with no return
        input  : line, filePath
        output : str
        """
        cmd = "echo %s >> '%s' 2>/dev/null" % (line, filePath)
        return cmd

    def getCreateFileCmd(self, path):
        """
        function: get create file cmd
        input  : path
        output : str
        """
        cmd = "touch '%s'" % path
        return cmd

    def getMoveFileCmd(self, src, dest):
        """
        function: get move file cmd
        input  : src, dest
        output : str
        """
        cmd = "mv '%s' '%s'" % (src, dest)
        return cmd

    def get_machine_arch_cmd(self):
        """
        function: get machine arch cmd
        input  : NA
        output : str
        """
        return 'uname -i'

    def getDefaultLocale(self):
        """
        function: get default locale
        input  : NA
        output : str
        """
        return 'en_US.utf-8'

    def getRemoveCmd(self, pathType):
        """
        function: get remove cmd
        input  : pathType
        output : str
        """
        opts = " "
        if pathType == "file":
            opts = " -f "
        elif pathType == "directory":
            opts = " -rf "
        return findCmdInPath('rm') + opts

    def getChmodCmd(self, Permission, src, recursive=False):
        """
        function: get chmod cmd
        input  : Permission, src, recursive
        output : str
        """
        return findCmdInPath('chmod') + \
               (" -R " if recursive else BLANK_SPACE) + \
               Permission + BLANK_SPACE + src

    def getChownCmd(self, owner, group, src, recursive=False):
        """
        function: get chown cmd
        input  : owner, group, src, recursive
        output : str
        """
        return findCmdInPath('chown') + \
               (" -R " if recursive else BLANK_SPACE) + owner + \
               COLON + group + BLANK_SPACE + src

    def getCopyCmd(self, src, dest, pathType=""):
        """
        function: get copy cmd
        input  : src, dest, pathType
        output : str
        """
        opts = " "
        if pathType == "directory":
            opts = " -r "
        return findCmdInPath('cp') + " -p  -f " + opts + BLANK_SPACE + "'" + \
               src + "'" + BLANK_SPACE + "'" + dest + "'"

    def getRemoteCopyCmd(self, src, dest, remoteHost, copyTo=True,
                         pathType="", otherHost=None):
        """
        function: get remote copy cmd
        input  : src, dest, remoteHost, copyTo=True,
                         pathType="", otherHost
        output : str
        """
        opts = " "
        if pathType == "directory":
            opts = " -r "
        if copyTo:
            return "pscp -x '%s' -H %s %s %s " % (opts.strip(),
                                                  remoteHost, src, dest)
        else:
            localhost = self.getLocalIp()
            if otherHost is not None:
                localhost = otherHost
            return "pssh -s -H %s \" pscp -x '%s' -H %s %s %s \" " % (
                remoteHost, opts.strip(), localhost, src, dest)

    def getHostName(self):
        """
        function : Get host name
        input : NA
        output: string
        """
        hostCmd = findCmdInPath('hostname')
        (status, output) = subprocess.getstatusoutput(hostCmd)
        # if cmd failed, then exit
        if status != 0:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50219"] % "host name"
                            + "The cmd is %s" % hostCmd)
        return output

    def getLocalIp(self):
        """
        function: Obtaining the local IP address
        input: NA
        output: str
        """
        return self.getHostName()

    def getScpCmd(self):
        """
        Get scp cmd for special remotely copy, just like remote to
        remote or remote to local.
        :return: str
        """
        return "pscp -H "

    def getUseraddCmd(self, user, group):
        """
        function: get user add cmd
        input  : user, group
        output : str
        """
        return findCmdInPath('useradd') + " -m " + user + " -g " + group

    def getUserdelCmd(self, user):
        """
        function: get userdel cmd
        input  : user
        output : str
        """
        return findCmdInPath('userdel') + " -r " + user

    def getGroupaddCmd(self, group):
        """
        function: get group add cmd
        input  : group
        output : str
        """
        return findCmdInPath('groupadd') + " " + group

    def getGroupdelCmd(self, group):
        """
        function: get group del cmd
        input  : group
        output : str
        """
        return findCmdInPath('groupdel') + " " + group

    def getMoveCmd(self, src, dest):
        """
        function: get move cmd
        input  : src, dest
        output : str
        """
        return findCmdInPath('mv') + " -f " + "'" + src + \
               "'" + BLANK_SPACE + "'" + dest + "'"

    def getMakeDirCmd(self, src, recursive=False):
        """
        function: get make dir cmd
        input  : src, recursive
        output : str
        """
        return findCmdInPath('mkdir') + \
               (" -p " if recursive else BLANK_SPACE) + "'" + src + "'"

    def getPingCmd(self, host, count, interval, packetSize=56):
        """
        function: get ping cmd
        input  : host, count, interval, packetSize
        output : str
        """
        opts = " "
        if int(packetSize) != int(56):
            opts = " -s " + str(packetSize)
        return findCmdInPath('ping') + BLANK_SPACE + host + " -c " + \
               count + " -i " + interval + opts

    def getWcCmd(self):
        """
        function: get wc cmd
        input  : NA
        output : str
        """
        return findCmdInPath('wc')

    def getTarCmd(self):
        """
        function: get tar cmd
        input  : NA
        output : str
        """
        return findCmdInPath('tar')

    def getZipCmd(self):
        """
        function: get zip cmd
        input  : NA
        output : str
        """
        return findCmdInPath('zip')

    def getUnzipCmd(self):
        """
        function: get unzip cmd
        input  : NA
        output : str
        """
        return findCmdInPath('unzip')

    def getEchoCmd(self, echoString):
        """
        function: get echo cmd
        input  : echoString
        output : str
        """
        cmdStr = '%s "%s"' % (findCmdInPath('echo'), echoString)
        return cmdStr

    def getSedCmd(self):
        """
        function: get sed cmd
        input  : NA
        output : str
        """
        return findCmdInPath('sed')

    def getGrepCmd(self):
        """
        function: get grep cmd
        input  : NA
        output : str
        """
        return findCmdInPath('grep')

    def getLsofCmd(self):
        """
        function: get lsof cmd
        input  : NA
        output : str
        """
        return findCmdInPath('lsof') + " -i:"

    def getIfconfigCmd(self):
        """
        function: get ifconfig cmd
        input  : NA
        output : str
        """
        return findCmdInPath('ifconfig')

    def getIpCmd(self):
        """
        function: get ip cmd
        input  : NA
        output : str
        """
        return findCmdInPath('ip')

    def getDateCmd(self):
        """
        function: get date cmd
        input  : NA
        output : str
        """
        return findCmdInPath('date')

    def getAwkCmd(self):
        """
        function: get awk cmd
        input  : NA
        output : str
        """
        return findCmdInPath('awk')

    def getFindCmd(self):
        """
        function: get find cmd
        input  : NA
        output : str
        """
        return findCmdInPath('find')

    def getTouchCmd(self, filename):
        """
        function: get touch cmd
        input  : filename
        output : str
        """
        return findCmdInPath('touch') + BLANK_SPACE + filename

    def getListCmd(self):
        """
        function: get list cmd
        input  : NA
        output : str
        """
        return findCmdInPath('ls')

    def getSHA256Cmd(self):
        """
        function: get sha256 cmd
        input  : NA
        output : str
        """
        return findCmdInPath('sha256sum')

    def getProcessCmd(self):
        """
        function: get process cmd
        input  : NA
        output : str
        """
        return findCmdInPath('ps')

    def getCatCmd(self):
        """
        function: get cat cmd
        input  : NA
        output : str
        """
        return findCmdInPath('cat')

    def getDdCmd(self):
        """
        function: get dd cmd
        input  : NA
        output : str
        """
        return findCmdInPath('dd')

    def getCdCmd(self, path):
        """
        function: get cd cmd
        input  : path
        output : str
        """
        return 'cd' + BLANK_SPACE + "'" + path + "'"

    def getAllCrontabCmd(self):
        """
        function: get all crontab cmd
        input  : NA
        output : str
        """
        cmd = findCmdInPath('crontab') + BLANK_SPACE + " -l"
        return cmd

    def getCrontabCmd(self):
        """
        function: get crontab cmd
        input  : NA
        output : str
        """
        return findCmdInPath('crontab')

    def getKillProcessCmd(self, signal, pid):
        """
        function: get kill process cmd
        input  : signal, pid
        output : str
        """
        return findCmdInPath('kill') + " -" + signal + BLANK_SPACE + pid

    def getKillallCmd(self):
        """
        function: get killall cmd
        input  : NA
        output : str
        """
        return findCmdInPath('killall')

    def getKillallProcessCmd(self, signal, username, procName=""):
        """
        function: get killall process cmd
        input  : signal, username, procName
        output : str
        """
        if procName != "":
            return findCmdInPath('killall') + " -s " + signal + " -u " + \
                   username + BLANK_SPACE + procName
        else:
            return findCmdInPath('killall') + " -s " + signal + " -u " + \
                   username

    def getXargsCmd(self):
        """
        function: get xargs cmd
        input  : NA
        output : str
        """
        return findCmdInPath('xargs')

    def getDeleteSemaphoreCmd(self, user):
        """
        function: get delete semaphore cmd
        input  : user
        output : str
        """
        ipcs = findCmdInPath('ipcs')
        ipcrm = findCmdInPath('ipcrm')
        xargs = findCmdInPath('xargs')
        awk = findCmdInPath('awk')
        return "%s -s | %s '/ %s /{print $2}' | %s -n1 %s -s" % (
            ipcs, awk, user, xargs, ipcrm)

    def getProcessIdByKeyWordsCmd(self, keywords):
        """
        function: get proecess id by keywords cmd
        input  : keywords
        output : str
        """
        ps = findCmdInPath('ps')
        grep = findCmdInPath('grep')
        awk = findCmdInPath('awk')
        return "%s -ef| %s -F '%s' | %s -F -v 'grep'| %s '{print $2}'" % (
            ps, grep, keywords, grep, awk)

    def getSysctlCmd(self):
        """
        function: get sysctl cmd
        input  : NA
        output : str
        """
        return findCmdInPath('sysctl')

    def getServiceCmd(self, serviceName, action):
        """
        function: get service cmd
        input  : serviceName, action
        output : str
        """
        return findCmdInPath('service') + BLANK_SPACE + serviceName + \
               BLANK_SPACE + action

    def getSystemctlCmd(self, serviceName, action):
        """
        function: get systemctl cmd
        input  : serviceName, action
        output : str
        """
        return findCmdInPath('systemctl') + BLANK_SPACE + action + \
               BLANK_SPACE + serviceName

    def getUlimitCmd(self):
        """
        function: get ulimit cmd
        input  : NA
        output : str
        """
        return 'ulimit'

    def getGetConfValueCmd(self):
        """
        function: get conf value cmd
        input  : NA
        output : str
        """
        return findCmdInPath('getconf') + " PAGESIZE "

    def getBlockdevCmd(self, device, value="", isSet=False):
        """
        function: get block dev cmd
        input  :  device, value, isSet
        output : str
        """
        if isSet and value != "":
            return findCmdInPath('blockdev') + " --setra " + value + \
                   BLANK_SPACE + device
        else:
            return findCmdInPath('blockdev') + " --getra " + device

    def getSysModManagementCmd(self, OperType, module):

        """
        OperType: list     --list system module
              load     --load system module
              insert   --insert system module by force
              remove   --remove system module
              dep      --generate modules.dep and map files
        """
        if OperType == "list":
            return findCmdInPath('lsmod') + BLANK_SPACE + module
        elif OperType == "load":
            return findCmdInPath('modprobe') + BLANK_SPACE + module
        elif OperType == "insert":
            return findCmdInPath('insmod') + BLANK_SPACE + module
        elif OperType == "remove":
            return findCmdInPath('rmmod') + BLANK_SPACE + module
        elif OperType == "dep":
            return findCmdInPath('depmod') + BLANK_SPACE + module
        else:
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51404"] + OperType)

    def getMountCmd(self):
        """
        function: get dd cmd
        input  : NA
        output : str
        """
        return findCmdInPath('mount')

    def getLocaleCmd(self):
        """
        function: get locale cmd
        input  : NA
        output : str
        """
        return findCmdInPath('locale')

    def getPasswordExpiresCmd(self, user):
        """
        function: get password expires cmd
        input  : NA
        output : str
        """
        return findCmdInPath('chage') + " -l " + user

    def getIOStatCmd(self):
        """
        function: get io stat cmd
        input  : NA
        output : str
        """
        return findCmdInPath('iostat') + " -xm 2 3 "

    def getEthtoolCmd(self):
        """
        function: get eth tool cmd
        input  : NA
        output : str
        """
        return findCmdInPath('ethtool')

    def getTailCmd(self):
        """
        function: get tail cmd
        input  : NA
        output : str
        """
        return findCmdInPath('tail')

    def getSshCmd(self, address):
        """
        function: get ssh cmd
        input  : address
        output : str
        """
        return "pssh -s -H " + BLANK_SPACE + address

    def getChkconfigCmd(self, OperType, service=""):
        """
        function: get chkconfig cmd
        input  : OperType, service
        output : str
        """
        if OperType == "list":
            return findCmdInPath('chkconfig') + " --list "
        elif OperType == "delete" and service:
            return findCmdInPath('chkconfig') + " --del " + service

    def getManageKerberosCmd(self, OperType):
        """
        OperType: init      --init kerberos
              destory   --destory kerberos
        """
        if OperType == "init":
            return "kinit -k -t "
        elif OperType == "destory":
            return "kdestroy"

    def getManageSSDCmd(self):
        """
        function: get manage ssd cmd
        input  : NA
        output : NA
        """
        pass

    def getPythonCmd(self):
        """
        function: get python cmd
        input  : NA
        output : str
        """
        return findCmdInPath('python3')

    def getShellCmd(self):
        """
        function: get shell cmd
        input  : NA
        output : str
        """
        return findCmdInPath('sh')

    def getSourceCmd(self):
        """
        function: get source cmd
        input  : NA
        output : str
        """
        return 'source'

    def getTestCmd(self):
        """
        Linux test cmd
        example: test -f /etc/profile && echo 1 || echo 2
        """
        return findCmdInPath("test")

    def getPgrepCmd(self):
        """
        Linux pgrep cmd
        """
        return findCmdInPath("pgrep")

    def getExportCmd(self, key="", value=""):
        """
        Linux export cmd
        """
        cmd = findCmdInPath("export")
        if key:
            cmd += " %s=%s" % (key, value)
        return cmd


class LinuxPlatform(GenericPlatform):
    """
    manage Linux command,config or service for muti-platform
    """

    def __init__(self):
        """
        function: constructor
        """
        pass

    def dist(self):
        """
        function: dist
        input  : NA
        output : distname, version, id
        """
        return dist()

    def getCpuInfoFile(self):
        """
        function: get cpu info file
        input  : NA
        output : str
        """
        return "/proc/cpuinfo"

    def getMemInfoFile(self):
        """
        function: get dd cmd
        input  : NA
        output : str
        """
        return "/proc/meminfo"

    def getBondInfoPath(self):
        """
        function: get bond info path
        input  : NA
        output : str
        """
        return "/proc/net/bonding/"

    def getSysctlFile(self):
        """
        function: get sysctl file
        input  : NA
        output : str
        """
        return "/etc/sysctl.conf"

    def getMtablFile(self):
        """
        function: get mtab file
        input  : NA
        output : str
        """
        return "/etc/mtab"

    def getInterruptFile(self):
        """
        function: get interrput file
        input  : NA
        output : str
        """
        return "/proc/interrupts"

    def getHostsFile(self):
        """
        function: get hostfile
        input  : NA
        output : str
        """
        return "/etc/hosts"

    def getName(self):
        """
        function: get name
        input  : NA
        output : str
        """
        return "linux"

    def getDefaultLocale(self):
        """
        function: get default locale
        input  : NA
        output : str
        """
        return 'en_US.utf8'

    def getDiskFreeCmd(self, Mounted="", inode=False):
        # -P is for POSIX formatting.  Prevents error
        # on lines that would wrap
        return findCmdInPath('df') + " -Pk " + \
               (" -i " if inode else " -h ") + Mounted

    def getDirSizeCmd(self, path, unit=""):
        # -s only shows the total size
        # unit specify the output size unit
        return findCmdInPath('du') + " -s " + (" -B %s " % unit
                                               if unit else " -h ") + path

    def getSadcCmd(self, interval, outFileName):
        """
        function: get sadc cmd
        input  : interval, outFileName
        output : str
        """
        cmd = "/usr/lib64/sa/sadc -F -d " + str(interval) + " " + outFileName
        return cmd

    def getCompressFilesCmd(self, tarName, fileSrc):
        """
        function: get compress file cmd
        input  : tarName, fileSrc
        output : str
        """
        cmd = "%s -zvcf '%s' %s" % (self.getTarCmd(), tarName, fileSrc)
        return cmd

    def getDecompressFilesCmd(self, srcPackage, dest):
        """
        function: get decompress file cmd
        input  : srcPackage, dest
        output : str
        """
        cmd = "%s -zxvf '%s' -C '%s'" % (self.getTarCmd(), srcPackage, dest)
        return cmd

    def getCompressZipFilesCmd(self, zipName, fileSrc):
        """
        function: get compress zip files cmd
        input  : zipName, fileSrc
        output : str
        """
        cmd = "cd %s && %s -r '%s.zip' ./*" % (fileSrc, self.getZipCmd(),
                                               zipName)
        return cmd

    def getDecompressZipFilesCmd(self, srcPackage, dest):
        """
        function: get decompress zip files cmd
        input  : srcPackage, dest
        output : str
        """
        cmd = "%s -o '%s' -d '%s'" % (self.getUnzipCmd(), srcPackage, dest)
        return cmd

    def getReplaceFileLineContentCmd(self, oldLine, newLine, path):
        """
        function: get replace file line content cmd
        input  : oldLine, newLine, path
        output : str
        """
        cmd = "%s -i \"s/%s/%s/g\" '%s'" % (self.getSedCmd(), oldLine,
                                            newLine, path)
        return cmd

    def getDirPermissionCmd(self, dirPath):
        """
        function: get dir permission cmd
        input  : dirPath
        output : str
        """
        cmd = "%s -ld '%s' | %s -F\" \" '{print $1}' " % (self.getListCmd(),
                                                          dirPath,
                                                          self.getAwkCmd())
        return cmd

    def getFileSHA256Cmd(self, fileName):
        """
        function: get file sha256 cmd
        input  : fileName
        output : str
        """
        cmd = "%s '%s' | %s -F\" \" '{print $1}' " % (self.getSHA256Cmd(),
                                                      fileName,
                                                      self.getAwkCmd())
        return cmd

    def getExecuteCmdWithUserProfile(self, user, userProfile, executeCmd,
                                     ignoreError=True):
        """
        function: get execute cmd with user profile
        input:  user, userProfile, executeCmd, ignoreError
        output: str
        """
        if (user != "") and (os.getuid() == 0):
            cmd = "su - %s -c '%s %s; %s'" % (user, self.getSourceCmd(),
                                              userProfile, executeCmd)
        else:
            cmd = "%s %s; %s" % (self.getSourceCmd(), userProfile, executeCmd)
        if ignoreError:
            cmd += " 2>/dev/null"
        return cmd

    def getUserHomePath(self):
        """
        function: get user home path
        input:  NA
        output: str
        """
        # converts the relative path to an absolute path
        cmd = "echo ~ 2>/dev/null"
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50219"] % "user home"
                            + "The cmd is %s" % cmd)
        return output

    def checkProcAlive(self, procPid):
        """
        function: check Proc alive
        input:  procPid
        output: True/False
        """
        try:
            os.kill(procPid, 0)
        except OSError:
            return False
        else:
            return True

    def getIpAddressAndNIC(self, ipType="ipv4"):
        """
        function: get ip address and nic
        input:  ipType
        output: NA
        """
        if ipType == "ipv4":
            key = AF_INET
        else:
            key = AF_INET6

        for iface in interfaces():
            if key in ifaddresses(iface):
                ipAddress = ifaddresses(iface)[key][0]['addr']
                yield (iface, ipAddress)

    def getIpAddressAndNICList(self, ipType="ipv4"):
        """
        function: get ip address and nicList
        input:  ipType
        output: []
        """
        return list(self.getIpAddressAndNIC(ipType))

    def getNetworkNumByIPAddr(self, ipAddress, ipType="ipv4"):
        """
        function: get netWork num by IP addr
        input:  ipAddress, ipType
        output: str
        """
        try:
            mappingList = self.getIpAddressAndNICList(ipType)
            for mapInfo in mappingList:
                if mapInfo[1] == ipAddress:
                    return mapInfo[0]
            raise Exception(ErrorCode.GAUSS_506["GAUSS-50612"] % ipAddress)
        except Exception as e:
            raise Exception(str(e))

    def getHostNameByIPAddr(self, ipAddress):
        """
        function: get host name by ip addr
        input:  ipAddress
        output: str
        """
        try:
            return socket.gethostbyaddr(ipAddress)[0]
        except Exception as e:
            raise Exception(str(e))

    def getLinuxNetworkConfigFile(self, networkConfPath, networkCardNum):
        """
        function: get linux network config file
        input:  networkConfPath, networkCardNum
        output: str
        """
        try:
            networkConfFile = "%sifcfg-%s" % (networkConfPath, networkCardNum)
            # Network configuration file does not exist
            if not os.path.exists(networkConfFile):
                cmd = "%s %s -iname 'ifcfg-*-%s' -print" % (self.getFindCmd(),
                                                            networkConfFile,
                                                            networkCardNum)
                (status, output) = subprocess.getstatusoutput(cmd)
                if (status != 0 or output.strip() == ""
                        or len(output.split('\n')) != 1):
                    raise Exception(ErrorCode.GAUSS_502["GAUSS-50201"] %
                                    networkConfFile + "The cmd is %s" % cmd)
                networkConfFile = output.strip()
            return networkConfFile
        except Exception as e:
            raise Exception(str(e))

    def getNetworkBondModeByBondConfigFile(self, bondingConfFile):
        """
        function: get Network Bond Mode By Bond ConfigFile
        input:  bondingConfFile
        output: str
        """
        try:
            # Check the bond mode
            cmd = "%s -w '\<Bonding Mode\>' %s | %s  -F ':' '{print $NF}'" % (
                self.getGrepCmd(), bondingConfFile, self.getAwkCmd())
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                                " Error: \n%s " % output)
            return "BondMode %s" % output.strip()
        except Exception as e:
            raise Exception(str(e))

    def getNetworkBondModeInfo(self, networkConfFile, networkCardNum):
        """
        function: get Network Bond Mode Info
        input: networkConfFile, networkCardNum
        output: str
        """
        # Get the bond profile
        if not os.path.isfile(networkConfFile):
            return "BondMode Null"

        bondingConfFile = "%s%s" % (self.getBondInfoPath(), networkCardNum)
        cmd = "%s -i 'BONDING_OPTS\|BONDING_MODULE_OPTS' %s" % (
            self.getGrepCmd(), networkConfFile)
        output = subprocess.getstatusoutput(cmd)[1]
        # Analysis results
        if output.strip() != "":
            if (output.find("mode") > 0) and os.path.exists(bondingConfFile):
                bondInfo = self.getNetworkBondModeByBondConfigFile(
                    bondingConfFile)
            else:
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                                " Error: \n%s " % output)
        elif os.path.exists(bondingConfFile):
            bondInfo = self.getNetworkBondModeByBondConfigFile(bondingConfFile)
            bondInfo += "\nNo 'BONDING_OPTS' or \
            'BONDING_MODULE_OPTS' in bond config file[%s]." % networkConfFile
        else:
            bondInfo = "BondMode Null"
        return bondInfo

    def getNetworkMaskByNICNum(self, networkCardNum, ipType="ipv4"):
        """
        function: get Network Mask By NICNum
        input:  networkCardNum, ipType
        output: str
        """
        if ipType == "ipv4":
            return ifaddresses(networkCardNum)[AF_INET][0]["netmask"]
        else:
            return ifaddresses(networkCardNum)[AF_INET6][0]["netmask"]

    def getNetworkRXTXValueByNICNum(self, networkCardNum, valueType):
        """
        function: get Network RXTX Value By NICNum
        input:  networkCardNum, valueType
        output: int
        """
        try:
            cmd = "%s -g %s | %s '%s:' | %s -n 1" % (self.getEthtoolCmd(),
                                                     networkCardNum,
                                                     self.getGrepCmd(),
                                                     valueType.upper(),
                                                     self.getTailCmd())
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                                " Error: \n%s " % output)
            value = output.split(':')[-1].split(' ')[0].strip()
            if not str(value).isdigit():
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                                " Error: \n%s " % output)
            return int(value)
        except Exception as e:
            raise Exception(str(e))

    def setNetworkRXTXValue(self, networkCardNum, rxValue=8192,
                            txValue=8192):
        """
        function: set Network RXTX Value
        input:  networkCardNum, rxValue, txValue
        output: NA
        """
        try:
            cmd = "%s -G %s rx %s tx %s" % (
                self.getEthtoolCmd(), networkCardNum, rxValue, txValue)
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                if output.find("no ring parameters changed, aborting") < 0:
                    raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                                    " Error: \n%s " % output)
        except Exception as e:
            raise Exception(str(e))

    def getNetworkSpeedByNICNum(self, networkCardNum):
        """
        function: get Network Speed By NICNum
        input:  networkCardNum
        output: int
        """
        keyWord = "Speed: "
        speedUnit = "Mb/s"
        try:
            cmd = "%s %s | grep '%s'" % (self.getEthtoolCmd(),
                                         networkCardNum, keyWord)
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0 or output == "":
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                                " Error: \n%s " % output)
            if len(output.split('\n')) >= 1:
                for line in output.split('\n'):
                    if line.find(keyWord) >= 0 and line.find(speedUnit) >= 0:
                        return int(line.split(':')[-1].strip()[:-4])
            return 0
        except Exception as e:
            raise Exception(str(e))

    def checkNetworkInterruptByNIC(self, networkCardNum):
        """
        function: check Network Interrupt By NIC
        """
        try:
            interruptConfFile = self.getInterruptFile()
            numberedListCmd = "%s %s | %s '%s-' | \
            %s -F ' ' '{print $1}' | %s -F ':' '{print $1}'" % (
                self.getCatCmd(), interruptConfFile, self.getGrepCmd(),
                networkCardNum, self.getAwkCmd(), self.getAwkCmd())
            irqCmd = "%s /proc/irq/$i/smp_affinity" % self.getCatCmd()
            cmd = "for i in `%s`; do %s ; done" % (numberedListCmd, irqCmd)
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                                " Error: \n%s " % output)
        except Exception as e:
            raise Exception(str(e))

        # cpu core number followed by 1 2 4 8,every 4 left shift one
        Mapping = {0: "1", 1: "2", 2: "4", 3: "8"}
        flag = True
        for index, eachLine in enumerate(output.split()):
            # Remove the ','
            eachLine = eachLine.replace(",", "")
            # Replace 0000,00001000 to 1,Remove invalid content
            validValue = eachLine.replace("0", "")
            # Convert the row index to the expected value
            expandNum = Mapping[index % 4]
            # Convert line index to expected position
            expandBit = index // 4 * -1 - 1
            # value and position is correct
            if eachLine[expandBit] == expandNum and validValue == expandNum:
                continue
            else:
                print("Network card [%s] multi-queue \
                support is not enabled.\n" % networkCardNum)
                flag = False
                break
        return flag

    def getInterruptCountNum(self, networkCardNum):
        """
        function : We can makesure that all dev names is startwith
        'ethX-' and endwith '-X'
        input  : String
        output : Int
        """
        try:
            interruptConfFile = self.getInterruptFile()
            cmd = "%s %s | %s '%s-' | %s -l" % (self.getCatCmd(),
                                                interruptConfFile,
                                                self.getGrepCmd(),
                                                networkCardNum,
                                                self.getWcCmd())
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_506["GAUSS_50622"] % cmd)
            if not str(output.strip()).isdigit():
                return 0
            return int(output.strip())
        except Exception as e:
            raise Exception(str(e))

    def getPackageFile(self, distName, version, packageVersion,
                       productVersion, fileType="tarFile"):
        """
        function : Get the path of binary file version.
        input : distName, version, packageVersion,
                productVersion, fileType
        output : String
        """
        distname, version, idnum = dist()
        distname = distname.lower()
        dirName = os.path.dirname(os.path.realpath(__file__))
        prefixStr = productVersion
        if fileType == "tarFile":
            postfixStr = "tar.gz"
        elif fileType == "binFile":
            postfixStr = "bin"
        elif fileType == "sha256File":
            postfixStr = "sha256"
        elif  fileType == "bz2File":
            postfixStr = "tar.bz2"
        else:
            raise Exception(ErrorCode.GAUSS_500["GAUSS_50019"] % "fileType")

        # RHEL and CentOS have the same kernel version,
        # So RHEL cluster package can run directly on CentOS.
        if distname in REDHAT:
            fileName = os.path.join(dirName, "./../../../",
                                    "%s-%s-%s-%s.%s" % (
                                        prefixStr, packageVersion, PAK_REDHAT,
                                        BIT_VERSION, postfixStr))
        elif distname in CENTOS:
            if os.path.isfile(os.path.join("/etc", "euleros-release")):
                fileName = os.path.join(dirName, "./../../../",
                                        "%s-%s-%s-%s.%s" % (
                                            prefixStr, packageVersion,
                                            PAK_EULER,
                                            BIT_VERSION, postfixStr))
                if not os.path.isfile(fileName):
                    fileName = os.path.join(dirName, "./../../../",
                                            "%s-%s-%s-%s.%s" % (
                                                prefixStr, packageVersion,
                                                PAK_CENTOS, BIT_VERSION,
                                                postfixStr))
            else:
                fileName = os.path.join(dirName, "./../../../",
                                        "%s-%s-%s-%s.%s" % (
                                            prefixStr, packageVersion,
                                            PAK_CENTOS,
                                            BIT_VERSION, postfixStr))
            if not os.path.isfile(fileName):
                fileName = os.path.join(dirName, "./../../../",
                                        "%s-%s-%s-%s.%s" % (
                                            prefixStr, packageVersion,
                                            PAK_REDHAT,
                                            BIT_VERSION, postfixStr))
        elif distname in ASIANUX:
            fileName = os.path.join(dirName, "./../../../",
                                    "%s-%s-%s-%s.%s" % (
                                        prefixStr, packageVersion, PAK_ASIANUX,
                                        BIT_VERSION, postfixStr))
        elif distname == SUSE and version.split('.')[0] in ("11", "12"):
            fileName = os.path.join(dirName, "./../../../",
                                    "%s-%s-%s-%s.%s" % (
                                        prefixStr, packageVersion, "SUSE11",
                                        BIT_VERSION, postfixStr))
        elif distname in EULEROS and (idnum in ["SP2", "SP3", "SP5"]):
            fileName = os.path.join(dirName, "./../../../",
                                    "%s-%s-%s-%s.%s" % (
                                        prefixStr, packageVersion, PAK_EULER,
                                        BIT_VERSION, postfixStr))
            if not os.path.isfile(fileName):
                fileName = os.path.join(dirName, "./../../../",
                                        "%s-%s-%s-%s.%s" % (
                                            prefixStr, packageVersion,
                                            PAK_REDHAT,
                                            BIT_VERSION, postfixStr))
        elif distname in EULEROS and (idnum == "SP8"):
            fileName = os.path.join(dirName, "./../../../",
                                    "%s-%s-%s-%s.%s" % (
                                        prefixStr, packageVersion, PAK_EULER,
                                        BIT_VERSION, postfixStr))
        elif distname in EULEROS:
            fileName = os.path.join(dirName, "./../../../",
                                    "%s-%s-%s-%s.%s" % (
                                        prefixStr, packageVersion, PAK_REDHAT,
                                        BIT_VERSION, postfixStr))
        elif distname in OPENEULER or distname in KYLIN:
            fileName = os.path.join(dirName, "./../../../",
                                    "%s-%s-%s-%s.%s" % (
                                        prefixStr, packageVersion,
                                        PAK_OPENEULER,
                                        BIT_VERSION, postfixStr))
        else:
            raise Exception(ErrorCode.GAUSS_519["GAUSS_51900"] +
                            "Supported platforms are: %s." % str(
                SUPPORT_WHOLE_PLATFORM_LIST))

        fileName = os.path.normpath(fileName)
        if not os.path.exists(fileName):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % fileName)
        if not os.path.isfile(fileName):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50210"] % fileName)
        return fileName

    def setKeyValueInSshd(self, key, value):
        """
        function: Set a (key, value) pair into /etc/ssh/sshd_config,
        before "Match" section.
                "Match" section in sshd_config should always places in the end.
                Attention: you need to remove the old (key, value)
                from sshd_config manually.
        input:
            key: the configuration name of sshd_config
            value: the configuration value(Only single line string
            permitted here).
        output:
            void
        """
        sshd_config = '/etc/ssh/sshd_config'
        cmd = "grep -E '^\<Match\>' %s" % sshd_config
        (status, output) = subprocess.getstatusoutput(cmd)

        if status == 0:
            cmd = "sed -i '/^\<Match\>.*/i %s %s' %s" % (key, value,
                                                         sshd_config)
        else:
            if output is not None and len(output.strip()) != 0:
                raise Exception(ErrorCode.GAUSS_503["GAUSS_50321"] %
                                "Match section" + "Command: %s, Error: %s" %
                                (cmd, output))
            cmd = "echo '' >> %s ; echo '%s %s' >> %s" % (sshd_config,
                                                          key, value,
                                                          sshd_config)

        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception((ErrorCode.GAUSS_503["GAUSS_50320"] % (
                key, value)) + ("Command: %s, Error: %s" % (cmd, output)))


class SLESPlatform(LinuxPlatform):
    """
    manage SUSE Linux Enterprise Server command,
    config or service for muti-platform
    """

    def __init__(self):
        self.NetWorkConfPath = "/etc/sysconfig/network/"
        self.SuSEReleaseFile = "/etc/SuSE-release"
        self.OSReleaseFile = "/etc/SuSE-release"

    def isPlatFormEulerOSOrRHEL7X(self):
        """
        function: the patform is euleros or rhel7x
        input  : NA
        output : bool
        """
        return False

    def getManageFirewallCmd(self, action):
        """
        function: get manage firewall cmd
        input  : action
        output : str
        """
        return findCmdInPath('SuSEfirewall2') + BLANK_SPACE + action

    def getLinuxFirewallStatus(self):
        """
        function: get Linux Firewall Status
        input:  NA
        output: str
        """
        try:
            cmd = self.getManageFirewallCmd("status")
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0 or output == "":
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                                " Error: \n%s " % output)

            if output.strip().find("SuSEfirewall2 not active") > 0:
                firewallStatus = "disabled"
            else:
                firewallStatus = "enabled"
            return firewallStatus
        except Exception as e:
            raise Exception(str(e))

    def getManageCrondCmd(self, action):
        """
        function: get manage crond cmd
        input  : action
        output : str
        """
        return self.getServiceCmd("cron", action)

    def getManageSshdCmd(self, action):
        """
        function: get manage sshd cmd
        input  : action
        output : str
        """
        return self.getServiceCmd("sshd", action)

    def getManageSyslogCmd(self, action):
        """
        function: get manage syslog cmd
        input  : action
        output : str
        """
        return self.getServiceCmd("syslog", action)

    def getManageRsyslogCmd(self, action):
        """
        function: get manage rsyslog cmd
        input  : action
        output : str
        """
        return self.getServiceCmd("rsyslog", action)

    def getManageSystemdJournaldCmd(self, action):
        """
        function: get systemd-jorunald cmd
        input  : action
        output : str
        """
        return self.getServiceCmd("systemd-journald", action)

    def getManageGsOsServerCmd(self, action):
        """
        function: get rhel/centos cmd
        input  : action
        output : NA
        """
        try:
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53021"]
                            % ("gs-OS-set service", "RHEL/CentOS"))
        except Exception as e:
            raise Exception(str(e))

    def getCurrentPlatForm(self):
        """
        function: get current platform
        input:  NA
        output: str, str
        """
        try:
            distName, version = dist()[0:2]
            bits = platform.architecture()[0]
            if (distName.lower() != SUSE or
                    version not in SUPPORT_SUSE_VERSION_LIST):
                raise Exception(ErrorCode.GAUSS_530["GAUSS_53022"]
                                % (distName.lower(), version))

            # os-release is added since SLE 12; SuSE-release will
            # be removed in a future service pack or release
            if os.path.exists(self.SuSEReleaseFile):
                cmd = "%s -i 'PATCHLEVEL' %s  | " \
                      "%s -F '=' '{print $2}'" % (self.getGrepCmd(),
                                                  self.SuSEReleaseFile,
                                                  self.getAwkCmd())
            else:
                cmd = "%s -i 'VERSION_ID' %s  | " \
                      "%s -F '.' '{print $2}' | %s 's/\"//'" % (
                          self.getGrepCmd(), self.OSReleaseFile,
                          self.getAwkCmd(), self.getSedCmd())
            (status, output) = subprocess.getstatusoutput(cmd)
            if status == 0 and output != "":
                patchlevel = output.strip()
            else:
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                                " Error: \n%s " % output)

            if (bits == BIT_VERSION and
                    ((version == SUSE11 and
                      patchlevel in SUPPORT_SUSE11X_VERSION_LIST) or
                     (version == SUSE12 and
                      patchlevel in SUPPORT_RHEL12X_VERSION_LIST))):
                platformVersion = "%s.%s" % (version, patchlevel)
                return distName.lower(), platformVersion
            else:
                raise Exception(ErrorCode.GAUSS_519["GAUSS_51900"] +
                                " The current system is: %s%s.%s" % (
                                    distName.lower(), version, patchlevel))
        except Exception as e:
            raise Exception(str(e))

    def getNetworkConfigFileByNICNum(self, networkCardNum):
        """
        function: get Network ConfigFile By NICNum
        input:  networkCardNum
        output: str
        """
        return self.getLinuxNetworkConfigFile(self.NetWorkConfPath,
                                              networkCardNum)

    def getNetworkConfigFileByIPAddr(self, ipAddress):
        """
        function: get Network ConfigFile By ip addr
        input:  ipAddress
        output: str
        """
        networkCardNum = self.getNetworkNumByIPAddr(ipAddress)
        return self.getNetworkConfigFileByNICNum(networkCardNum)


class RHELPlatform(LinuxPlatform):
    """
    manage Red Hat Enterprise Linux command,config or service for muti-platform
    """

    def __init__(self):
        """
        function: constructor
        """
        self.NetWorkConfPath = "/etc/sysconfig/network-scripts/"

    def isSupportSystemctl(self):
        """
        function: isSupportSystemctl
        input:  NA
        output: bool
        """
        distName, version = dist()[0:2]
        if ((distName.lower() == EULEROS and version[0:3] in
             SUPPORT_EULEROS_VERSION_LIST) or
                (distName.lower() in SUPPORT_RHEL_SERIES_PLATFORM_LIST and
                 version[0:3] in SUPPORT_RHEL7X_VERSION_LIST) or
                (distName.lower() == CENTOS and version[0:3] ==
                 SUPPORT_EULEROS_VERSION_LIST and
                 os.path.isfile(os.path.join("/etc", "euleros-release"))) or
                distName.lower() == OPENEULER):
            return True
        else:
            return False

    def isPlatFormEulerOSOrRHEL7X(self):
        """
        function: check is PlatForm EulerOS Or RHEL7X
        """
        return self.isSupportSystemctl()

    def getManageFirewallCmd(self, action):
        """
        function: get manage firewall cmd
        input  : action
        output : str
        """
        if self.isSupportSystemctl():
            return self.getSystemctlCmd("firewalld.service", action)
        else:
            return self.getServiceCmd("iptables", action)

    def getLinuxFirewallStatus(self):
        """
        function: get Linux Firewall Status
        """
        try:
            cmd = self.getManageFirewallCmd("status")
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0 and output == "":
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                                " Error: \n%s " % output)

            if self.isSupportSystemctl():
                if output.strip().find("Active: active (running)") > 0:
                    firewallStatus = "enabled"
                else:
                    firewallStatus = "disabled"
            else:
                if output.strip().find("Firewall is not running") > 0:
                    firewallStatus = "disabled"
                else:
                    firewallStatus = "enabled"
            return firewallStatus
        except Exception as e:
            raise Exception(str(e))

    def getManageCrondCmd(self, action):
        """
        function: get crond.server cmd
        input  : action
        output : str
        """
        if self.isSupportSystemctl():
            return self.getSystemctlCmd("crond.service", action)
        else:
            return self.getServiceCmd("crond", action)

    def getManageSshdCmd(self, action):
        """
        function: get sshd.server cmd
        input  : action
        output : str
        """
        if self.isSupportSystemctl():
            return self.getSystemctlCmd("sshd.service", action)
        else:
            return self.getServiceCmd("sshd", action)

    def getManageGsOsServerCmd(self, action):
        """
        function: get gs-OS-set.service cmd
        input  : action
        output : str
        """
        if self.isSupportSystemctl():
            return self.getSystemctlCmd("gs-OS-set.service", action)
        else:
            return self.getServiceCmd("gs-OS-set", action)

    def getManageSyslogCmd(self, action):
        """
        function: get syslog service cmd
        """
        try:
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53021"]
                            % ("Syslog service", "SuSE"))
        except Exception as e:
            raise Exception(str(e))

    def getManageRsyslogCmd(self, action):
        """
        function: get syslog cmd
        """
        try:
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53021"]
                            % ("Rsyslog service", "SuSE"))
        except Exception as e:
            raise Exception(str(e))

    def getManageSystemdJournaldCmd(self, action):
        """
        function: get systemd journal cmd
        """
        try:
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53021"]
                            % ("systemd-journald", "SuSE"))
        except Exception as e:
            raise Exception(str(e))

    def getCurrentPlatForm(self):
        """
        function: get current platform
        """
        try:
            distName, version, currentId = dist()
            bits = platform.architecture()[0]
            if ((bits == BIT_VERSION and
                 ((distName.lower() == EULEROS and version[0:3] in
                   SUPPORT_EULEROS_VERSION_LIST) or
                  (distName.lower() in SUPPORT_RHEL_SERIES_PLATFORM_LIST and
                   version[0:3] in SUPPORT_RHEL_SERIES_VERSION_LIST)) or
                 (distName.lower() == OPENEULER)
            )):
                return distName.lower(), version[0:3]
            else:
                if distName.lower() == CENTOS and os.path.isfile(
                        os.path.join("/etc", "euleros-release")) and \
                        (version[0:3] in SUPPORT_EULEROS_VERSION_LIST):
                    return EULEROS, version[0:3]
                if distName.lower() == EULEROS:
                    raise Exception(ErrorCode.GAUSS_519["GAUSS_51900"] +
                                    " The current system is: %s%s%s" % (
                                        distName.lower(),
                                        version[0:3], currentId))
                else:
                    raise Exception(ErrorCode.GAUSS_519["GAUSS_51900"] +
                                    " The current system is: %s%s" % (
                                        distName.lower(), version[0:3]))
        except Exception as e:
            raise Exception(str(e))

    def getNetworkConfigFileByIPAddr(self, ipAddress):
        """
        function: get Network ConfigFile By IPAddr
        """
        networkCardNum = self.getNetworkNumByIPAddr(ipAddress)
        return self.getLinuxNetworkConfigFile(self.NetWorkConfPath,
                                              networkCardNum)

    def getNetworkConfigFileByNICNum(self, networkCardNum):
        """
        function: get Network ConfigFile By NICNum
        """
        return self.getLinuxNetworkConfigFile(self.NetWorkConfPath,
                                              networkCardNum)


class UserPlatform():
    """
    manage Red Hat Enterprise Linux command,config or service for muti-platform
    """

    def __init__(self):
        """
        function : Check support OS version and init OS class
        """
        # now we support this platform:
        #     RHEL/CentOS     "6.4", "6.5", "6.6", "6.7", "6.8", "6.9",
        #     "7.0", "7.1", "7.2", "7.3", "7.4", "7.5 "64bit
        #     EulerOS         "2.0", "2.3" 64bit
        #     SuSE11          sp1/2/3/4 64bit
        #     SuSE12          sp0/1/2/3 64bit
        #     Kylin           "10" 64bit
        distName, version, idNum = dist()
        if distName.lower() not in SUPPORT_WHOLE_PLATFORM_LIST:
            raise Exception(ErrorCode.GAUSS_519["GAUSS_51900"] +
                            "Supported platforms are: %s." % str(
                SUPPORT_WHOLE_PLATFORM_LIST))

        if distName.lower() == SUSE:
            # SuSE11.X SUSE12.X
            self.userPlatform = SLESPlatform()
        elif distName.lower() in SUPPORT_RHEL_SERIES_PLATFORM_LIST:
            # RHEL6.X RHEL7.X
            self.userPlatform = RHELPlatform()
        else:
            # EULEROS 2.0/2.3
            self.userPlatform = RHELPlatform()
        try:
            self.userPlatform.getCurrentPlatForm()
        except Exception as e:
            raise Exception(str(e))


# global platform class
g_Platform = UserPlatform().userPlatform

