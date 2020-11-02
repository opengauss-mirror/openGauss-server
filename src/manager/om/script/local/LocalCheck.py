#!/usr/bin/env python3
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
# Description  :
#############################################################################
import os
import sys
import getopt
import subprocess
import platform
import glob
import xml.etree.cElementTree as ETree

sys.path.append(sys.path[0] + "/../")
sys.path.append(os.path.realpath(os.path.dirname(__file__)) + "/../../lib")
from gspylib.os.gsfile import g_file
from gspylib.common.GaussLog import GaussLog
from gspylib.common.DbClusterInfo import dbClusterInfo
from gspylib.common.Common import DefaultValue
from gspylib.common.VersionInfo import VersionInfo
from gspylib.common.ErrorCode import ErrorCode
from gspylib.common.ParameterParsecheck import Parameter
from gspylib.os.gsOSlib import g_OSlib
from gspylib.os.gsfile import g_Platform

actioItemMap = {
    "Check_SysCtl_Parameter": ['/etc/sysctl.conf', False],
    "Check_FileSystem_Configure": ['/etc/security/limits.conf', False],
    "osKernelParameterCheck": ['/etc/sysctl.conf', False],
    "Set_SysCtl_Parameter": ['/etc/sysctl.conf', True],
    "Set_FileSystem_Configure": ['/etc/security/limits.conf', True]
}

docker_no_need_check = ["net.core.wmem_max", "net.core.rmem_max",
                        "net.core.wmem_default", "net.core.rmem_default",
                        "net.sctp.sctp_mem", "net.sctp.sctp_rmem",
                        "net.sctp.sctp_wmem", "net.core.netdev_max_backlog",
                        "net.ipv4.tcp_max_tw_buckets", "net.ipv4.tcp_tw_reuse",
                        "net.ipv4.tcp_tw_recycle", "net.ipv4.tcp_retries2",
                        "net.ipv4.ip_local_reserved_ports", "net.ipv4.tcp_rmem",
                        "net.ipv4.tcp_wmem", "net.ipv4.tcp_max_syn_backlog",
                        "net.ipv4.tcp_syncookies", "net.ipv4.tcp_fin_timeout",
                        "net.ipv4.tcp_sack", "net.ipv4.tcp_timestamps",
                        "net.ipv4.tcp_retries1", "net.ipv4.tcp_syn_retries",
                        "net.ipv4.tcp_synack_retries"]

paraList = {}

#############################################################################
# Global variables
#   g_opts: globle option
#   g_logger: globle logger
#   g_clusterInfo: global clueter information
#############################################################################
g_logger = None
g_opts = None
g_clusterInfo = None
g_check_os = False
configFile = ''
resultList = list()
netWorkBondInfo = None
netWorkLevel = 10000


class CmdOptions():
    def __init__(self):
        """
        function: constructor
        """
        # initialize variable
        self.action = ""
        self.user = ""
        self.extrachecklist = []
        self.logFile = ""
        self.confFile = ""
        self.mtuValue = ""
        self.hostname = ""


class netWork:
    """
    class: netWork
    """

    def __init__(self):
        """
        function : Init class netWork
        input  : NA
        output : NA
        """
        self.netLevel = ""
        self.netNum = ""
        self.variables = dict()
        self.modeType = False
        self.nums = 0


#############################################################################
# Parse and check parameters
#############################################################################
def usage():
    """
Usage:
 python3 --help | -?
 python3 LocalCheck -t action [-l logfile] [-U user] [--check-os] [-V]
Common options:
 -t                                The type of action.
 -l --log-file=logfile             The path of log file.
 -? --help                         Show this help screen.
 -U                                Cluster user with root permissions.
    --check-os                     Whether or not gs_checkos
 -V --version
    """
    print(usage.__doc__)


def checkSpecifiedItems(key, paralist, isSet=False):
    """
    function: check specified item name
    input : key, paralist, isSet
    output: NA
    """
    # checkItemMap[key][0] is the check function about the key
    func = checkItemMap[key][0]
    try:
        if (hasattr(func, "__name__")):
            func(paralist, isSet)
        else:
            g_logger.logExit(ErrorCode.GAUSS_530["GAUSS_53010"]
                             % (func, "LocalCheck.py"))
    except Exception as e:
        g_logger.logExit(str(e))


def checkNetWorkMTU():
    """
    function: gs_check check NetWork card MTU parameters
    input: NA
    output: int
    """
    try:
        # Init cluster info
        DbClusterInfo = dbClusterInfo()
        DbClusterInfo.initFromStaticConfig(g_opts.user)
        localHost = DefaultValue.GetHostIpOrName()
        nodeIp = None
        for dbnode in DbClusterInfo.dbNodes:
            if (dbnode.name == localHost):
                nodeIp = dbnode.backIps[0]
                break
        networkCardNum = DefaultValue.CheckNetWorkBonding(nodeIp, False)
        # check NetWork card MTU parameters
        valueStr = DefaultValue.checkNetWorkMTU(nodeIp, False)
        if (not str(valueStr).isdigit()):
            g_logger.log("Abnormal reason: Failed to obtain network"
                         " card MTU value." + " Error: \n%s" % valueStr)
            return 1
        netParameter = DefaultValue.getConfigFilePara(configFile,
                                                      '/sbin/ifconfig')
        if (int(valueStr) != int(g_opts.mtuValue)):
            g_logger.log("        Abnormal: network '%s' 'mtu' value[%s:%s]"
                         " is different from the other node [%s:%s]"
                         % (networkCardNum, localHost, valueStr,
                            g_opts.hostname, g_opts.mtuValue))
            return 1
        elif (int(valueStr) != int(netParameter["mtu"])):
            g_logger.log("        Warning reason: variable 'MTU' RealValue "
                         "'%s' ExpectedValue '%s'." % (valueStr,
                                                       netParameter["mtu"]))
            return 2
        else:
            return 0

    except Exception as e:
        g_logger.log("        Abnormal reason: Failed to obtain the"
                     " networkCard parameter [MTU]. Error: \n        %s"
                     % str(e))
        return 1


def checkSysctlParameter(kernelParameter, isSet):
    """
    function: check and set the OS parameters
    input: kernelParameter: OS parameters list will be check and set
           isSet: the flag, when it is only True then will set OS parameters
    output: NA
    """
    setParameterList = {}
    patchlevel = ""

    # get the suggest parameters and updata kernelParameter
    suggestParameterList = DefaultValue.getConfigFilePara(
        configFile, 'SUGGEST:%s' % actioItemMap["Check_SysCtl_Parameter"][0])
    kernelParameter.update(suggestParameterList)

    # check the OS parameters
    if ("fs.aio-max-nr" in kernelParameter):
        g_logger.log("        Warning reason: Checking or setting the"
                     " parameter 'fs.aio-max-nr' should be use "
                     "'gs_checkos -i A10' or 'gs_checkos -i B4'.")
        kernelParameter.pop("fs.aio-max-nr")
    # Get OS version
    distname, version = g_Platform.dist()[0:2]
    if (distname == "SuSE" and version == "11"):
        cmd = "grep -i 'PATCHLEVEL' /etc/SuSE-release  |" \
              " awk -F '=' '{print $2}'"
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status == 0 and output != ""):
            patchlevel = output.strip()
    # Gs_check get NetWork card MTU
    if (g_opts.action == "osKernelParameterCheck"):
        checkrestult = checkNetWorkMTU()
        if checkrestult != 0:
            resultList.append(checkrestult)

    for key in kernelParameter:
        # The SuSE 11 SP1 operating system
        # does not have vm.extfrag_threshold parameter, skip check
        if (patchlevel == "1" and key == "vm.extfrag_threshold"):
            continue
        sysFile = "/proc/sys/%s" % key.replace('.', '/')
        # High version of linux no longer supports tcp_tw_recycle
        if not os.path.exists(
                sysFile) and key == "net.ipv4.tcp_tw_recycle":
            continue
        if (DefaultValue.checkDockerEnv() and key in docker_no_need_check):
            continue
        # The parameter sctpchecksumerrors check method is independent
        if (key == "sctpchecksumerrors"):
            cmd = "cat /proc/net/sctp/snmp | grep SctpChecksumErrors" \
                  " | awk '{print $2}'"
        else:
            cmd = "cat %s" % ("/proc/sys/%s" % key.replace('.', '/'))
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status == 0):
            if (key == "vm.min_free_kbytes"
                    and output.split() != kernelParameter[key].split()):
                expected_min = float(kernelParameter[key].split()[0]) * 0.9
                expected_max = float(kernelParameter[key].split()[0]) * 1.1
                if (int(output.split()[0]) > expected_max
                        or int(output.split()[0]) < expected_min):
                    resultList.append(1)
                    g_logger.log("        Abnormal reason: variable '%s'"
                                 " RealValue '%s' ExpectedValue '%s'."
                                 % (key, output, kernelParameter[key]))
                    setParameterList[key] = kernelParameter[key]
            elif (key == "net.ipv4.ip_local_port_range"
                  and output.split() != kernelParameter[key].split()):
                expected_min = int(kernelParameter[key].split()[0])
                expected_max = int(kernelParameter[key].split()[1])
                if (int(output.split()[0]) < expected_min
                        or int(output.split()[1]) > expected_max):
                    resultList.append(2)
                    g_logger.log("        Warning reason: variable '%s'"
                                 " RealValue '%s' ExpectedValue '%s'."
                                 % (key, output, kernelParameter[key]))
            elif (output.split() != kernelParameter[key].split() and
                  key not in list(suggestParameterList.keys())):
                resultList.append(1)
                g_logger.log("        Abnormal reason: variable '%s'"
                             " RealValue '%s' ExpectedValue '%s'."
                             % (key, output, kernelParameter[key]))
                setParameterList[key] = kernelParameter[key]
            elif output.split() != kernelParameter[key].split():
                if (key == "vm.overcommit_ratio"):
                    cmd = "cat /proc/sys/vm/overcommit_memory"
                    (status, value) = subprocess.getstatusoutput(cmd)
                    if (status == 0 and value == "0"):
                        continue
                resultList.append(2)
                g_logger.log("        Warning reason: variable '%s' RealValue"
                             " '%s' ExpectedValue '%s'."
                             % (key, output, kernelParameter[key]))
        else:
            resultList.append(1)
            g_logger.log("        Abnormal reason: Failed to obtain the OS "
                         "kernel parameter [%s]. Error: \n        %s"
                         % (key, output))
            setParameterList[key] = kernelParameter[key]

    if (1 in resultList and 'Check' in g_opts.action):
        g_logger.log("        %s failed." % g_opts.action)
    elif (2 in resultList and 'Check' in g_opts.action):
        g_logger.log("        %s warning." % g_opts.action)
    else:
        g_logger.log("        All values about system control"
                     " parameters are correct: Normal")

    # set the OS parameters
    if isSet:
        setOSParameter(setParameterList, patchlevel)


def setOSParameter(setParameterList, patchlevel):
    """
    function: set os parameter
    input  : setParameterList, patchlevel
    output : NA
    """
    # The SuSE 11 SP1 operating system does not have
    # vm.extfrag_threshold parameter, skip set
    if ("vm.extfrag_threshold" in setParameterList and patchlevel == "1"):
        setParameterList.pop("vm.extfrag_threshold")
    # The parameter sctpchecksumerrors set method is independent
    if ("sctpchecksumerrors" in setParameterList):
        cmd = "echo 1 > /sys/module/sctp/parameters/no_checksums"
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            g_logger.debug("The cmd is %s " % cmd)
            g_logger.log("        Failed to enforce sysctl kernel variable"
                         " 'sctpchecksumerrors'. Error: %s" % output)
        setParameterList.pop("sctpchecksumerrors")

    if (len(setParameterList) != 0):
        g_logger.debug("Setting sysctl parameter.")
        for key in setParameterList:
            SetSysctlForList(key, setParameterList[key])
            g_logger.log("        Set variable '%s' to '%s'"
                         % (key, setParameterList[key]))
        cmd = "sysctl -p"
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            cmderrorinfo = "sysctl -p | grep 'No such file or directory'"
            (status, outputresult) = subprocess.getstatusoutput(cmderrorinfo)
            if (status != 0 and outputresult == ""):
                g_logger.logExit(ErrorCode.GAUSS_514["GAUSS_51400"]
                                 % cmderrorinfo)
            for key in setParameterList:
                tmp = "/proc/sys/%s" % key.replace('.', '/')
                if (tmp in outputresult or key in outputresult):
                    # delete the record about key from the /etc/sysctl.conf
                    delSysctlForList(key, setParameterList[key])
                    g_logger.log("        Failed to enforce sysctl kernel"
                                 " variable '%s'. Error: the variable name"
                                 " is incorrect." % key)


def SetSysctlForList(key, value):
    """
    function: Set sysctl parameter
    input : key, value
    output: NA
    """
    kernelParameterFile = "/etc/sysctl.conf"
    cmd = """sed -i '/^\\s*%s *=.*$/d' %s &&
           echo %s = %s  >> %s 2>/dev/null""" % (key, kernelParameterFile,
                                                 key, value,
                                                 kernelParameterFile)
    (status, output) = subprocess.getstatusoutput(cmd)
    if (status != 0):
        g_logger.log("        Failed to set variable '%s %s'." % (key, value))
        g_logger.debug("Command:\n  %s\nOutput:\n  %s" % (cmd, str(output)))


def delSysctlForList(key, value):
    """
    function: delete the record about key from the /etc/sysctl.conf
    input: key, value
    output: NA
    """
    g_logger.debug("Deleting sysctl parameter.")
    kernelParameterFile = "/etc/sysctl.conf"
    cmd = """sed -i '/^\\s*%s *=.*$/d' %s """ % (key, kernelParameterFile)
    (status, output) = subprocess.getstatusoutput(cmd)
    if (status != 0):
        g_logger.log("        Failed to delete variable"
                     " '%s %s' from /etc/sysctl.conf." % (key, value))
        g_logger.debug("Command:\n  %s\nOutput:\n  %s" % (cmd, str(output)))


def checkLimitsParameter(limitPara, isSet):
    """
    function: check and set the limit parameter
    input: limitPara, isSet
    output: NA
    """

    # utility class for this function only
    class limitsconf_data:
        """
        Class: limitsconf_data
        """

        def __init__(self, expected):
            """
            function: constructor
            """
            self.domain = None
            self.value_found = None
            self.value_expected = expected

    # check the limit parameter
    table = dict()

    for key in list(limitPara.keys()):
        cmd = "ulimit -a |  grep -F '%s' 2>/dev/null" % key
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status == 0):
            resLines = output.split('\n')
            resList = resLines[0].split(' ')
            limitValue = resList[-1].strip()
            if (limitPara[key] == 'unlimited'):
                resultList.append(2)
                if limitValue != 'unlimited':
                    g_logger.log("        Warning reason: variable '%s'"
                                 " RealValue '%s' ExpectedValue '%s'"
                                 % (key, limitValue, limitPara[key]))
                if (key == 'virtual memory'):
                    table[('soft', 'as')] = limitsconf_data(limitPara[key])
                    table[('hard', 'as')] = limitsconf_data(limitPara[key])
                if (key == 'max user processes'):
                    table[('soft', 'nproc')] = limitsconf_data(limitPara[key])
                    table[('hard', 'nproc')] = limitsconf_data(limitPara[key])

            elif (limitPara[key] != 'unlimited'):
                if (limitValue == 'unlimited'):
                    continue
                if (int(limitValue) < int(limitPara[key])):
                    if (key == "stack size"):
                        resultList.append(1)
                        g_logger.log("        Abnormal reason: variable '%s'"
                                     " RealValue '%s' ExpectedValue '%s'"
                                     % (key, limitValue, limitPara[key]))
                    else:
                        resultList.append(2)
                        g_logger.log("        Warning reason: variable '%s'"
                                     " RealValue '%s' ExpectedValue '%s'"
                                     % (key, limitValue, limitPara[key]))
                    if (key == 'stack size'):
                        table[('soft',
                               'stack')] = limitsconf_data(limitPara[key])
                        table[('hard',
                               'stack')] = limitsconf_data(limitPara[key])
                if (key == 'open files'):
                    table[('soft',
                           'nofile')] = limitsconf_data(limitPara[key])
                    table[('hard',
                           'nofile')] = limitsconf_data(limitPara[key])
        else:
            resultList.append(1)
            g_logger.debug("The cmd is %s " % cmd)
            g_logger.log("        Failed to obtain '%s'. Error: \n%s"
                         % (key, output))

    # set the open file numbers
    if isSet and len(list(table.keys())):
        for key in list(table.keys()):
            if (key[1] == "nofile" or key[1] == "nproc"):
                limitPath = '/etc/security/limits.d/'
                nofiles = glob.glob("/etc/security/limits.d/*.conf")
                for conf in nofiles:
                    g_file.changeMode(DefaultValue.HOSTS_FILE, conf)
                    SetLimitsConf(key[0], key[1],
                                  table[key].value_expected, conf)
                if os.path.isfile(os.path.join(limitPath, '91-nofile.conf')):
                    limitFile = '91-nofile.conf'
                else:
                    limitFile = '90-nofile.conf'
            if (key[1] == "stack" or key[1] == "as" or key[1] == "nproc"):
                limitPath = '/etc/security/'
                limitFile = 'limits.conf'
            if (checkLimitFile(limitPath, limitFile) != 0):
                return

            SetLimitsConf(key[0], key[1], table[key].value_expected,
                          limitPath + limitFile)
            g_logger.log("        Set variable '%s %s' to '%s'"
                         % (key[0], key[1], table[key].value_expected))


def checkLimitFile(limitPath, limitFile):
    """
    function: check limits file
    input : limitPath, limitFile
    output: status
    """
    g_logger.debug("check limits configuration file.")

    pathCmd = "if [ ! -d '%s' ]; then mkdir '%s' -m %s;fi; cd '%s';" \
              % (limitPath, limitPath,
                 DefaultValue.MAX_DIRECTORY_MODE, limitPath)
    pathCmd += "if [ ! -f '%s' ]; then touch '%s';chmod %s '%s';fi" \
               % (limitFile, limitFile,
                  DefaultValue.FILE_MODE, limitFile)
    (status, output) = subprocess.getstatusoutput(pathCmd)
    if (status != 0):
        g_logger.debug("The cmd is %s " % pathCmd)
        g_logger.log("        Abnormal reason: Failed to create %s%s."
                     " Error: \n%s" % (limitPath, limitFile, output))
    return status


def SetLimitsConf(typename, item, value, limitfile):
    """
    function: write the /etc/security/limits.conf
    input: typename, item, value, limitfile
    output: NA
    """
    g_logger.debug("Setting limits config.")
    clusterUser = getClusterUser()
    cmd = """sed -i '/^.* %s *%s .*$/d' %s &&
           echo "root       %s    %s  %s" >> %s && """ \
          % (typename, item, limitfile, typename, item, value, limitfile)
    cmd += """echo "%s       %s    %s  %s" >> %s""" \
           % (clusterUser, typename, item, value, limitfile)
    (status, output) = subprocess.getstatusoutput(cmd)
    if (status != 0):
        g_logger.debug("The cmd is %s " % cmd)
        g_logger.log("        Abnormal reason: Failed to set variable"
                     " '%s %s'. Error: \n%s" % (typename, item, output))


def getGphome(xmlFilePath):
    """
    function: Get GPHOME path
    input : xmlFilePath
    output: str
    """
    gphome = ""
    if os.path.exists(xmlFilePath):
        with open(xmlFilePath, 'r') as fp:
            xmlstr = fp.read()
        domTree = ETree.fromstring(xmlstr)
        rootNode = domTree
        if not rootNode.findall('CLUSTER'):
            raise Exception(ErrorCode.GAUSS_512["GAUSS_51200"] % 'CLUSTER')
        element = rootNode.findall('CLUSTER')[0]
        nodeArray = element.findall('PARAM')
        for node in nodeArray:
            name = node.attrib['name']
            if (name == "gaussdbToolPath"):
                gphome = str(node.attrib['value'])
    return gphome


def getClusterUser():
    """
    function: Check user information
    input : NA
    output: str
    """
    # get user and group
    gphome = getGphome(g_opts.confFile)
    if not gphome or not os.path.exists(gphome):
        user = "*"
        return user
    user = g_OSlib.getPathOwner(gphome)[0]
    return user


def CheckSection(section, isSetting=False):
    """
    function: check the section parameters status
    input: section, isSetting
    output: NA
    """

    global configFile
    dirName = os.path.dirname(os.path.realpath(__file__))
    configFile = "%s/../gspylib/etc/conf/check_list.conf" % dirName

    # get the parameter and value about section from configuration file
    if (section == '/etc/security/limits.conf'):
        checkList = ['open files', 'pipe size']
        commParameterList = DefaultValue.getConfigFilePara(configFile,
                                                           section, checkList)
    else:
        commParameterList = DefaultValue.getConfigFilePara(configFile,
                                                           section)

    # checking or setting the parameter what in the commParameterList
    checkSpecifiedItems(section, commParameterList, isSetting)


def parseCommandLine():
    """
    function: Parse command line and save to global variables
    input : NA
    output: NA
    """
    try:
        # Resolves the command line
        opts, args = getopt.getopt(sys.argv[1:], "t:X:l:U:V?",
                                   ["help", "log-file=", "xmlfile=",
                                    "MTUvalue=", "hostname=",
                                    "check-os", "version"])
    except Exception as e:
        usage()
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] % str(e))

    if (len(args) > 0):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"]
                               % str(args[0]))

    global g_opts
    global g_check_os
    g_opts = CmdOptions()

    # Output help information and exit
    for (key, value) in opts:
        if (key == "-?" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-V" or key == "--version"):
            print(("%s %s" % (sys.argv[0].split("/")[-1],
                              VersionInfo.COMMON_VERSION)))
            sys.exit(0)
        elif (key == "-t"):
            g_opts.action = value
        elif (key == "-U"):
            g_opts.user = value
        elif (key == "--check-os"):
            g_check_os = True
        elif (key == "-l" or key == "--log-file"):
            g_opts.logFile = os.path.realpath(value)
        elif (key == "--MTUvalue"):
            g_opts.mtuValue = value
        elif (key == "--hostname"):
            g_opts.hostname = value
        elif (key == "-X"):
            g_opts.confFile = value
        # check para vaild
        Parameter.checkParaVaild(key, value)


def checkParameter():
    """
    function: check parameter for different action
    input : NA
    output: NA
    """

    # check if user exist and is the right user
    if (g_opts.user != ''):
        DefaultValue.checkUser(g_opts.user)
        tmpDir = DefaultValue.getTmpDirFromEnv(g_opts.user)
        if (not os.path.exists(tmpDir)):
            GaussLog.exitWithError(ErrorCode.GAUSS_502["GAUSS_50201"]
                                   % ("temporary directory[" + tmpDir + "]"))

    # check the -t parameter
    if (g_opts.action == ""):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"] % 't' + '.')
    if (g_opts.action not in list(actioItemMap.keys())):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50004"] % "t")

    if (g_opts.logFile == ""):
        dirName = os.path.dirname(os.path.realpath(__file__))
        g_opts.logFile = os.path.join(dirName, "gaussdb_localcheck.log")


def initGlobals():
    """
    function: Init global log
    input : NA
    output: NA
    """
    # state global variable
    global g_logger
    global g_clusterInfo
    # Init the log file
    g_logger = GaussLog(g_opts.logFile, "gaussdb_localcheck")
    if os.path.exists(g_opts.confFile):
        g_clusterInfo = dbClusterInfo()
        g_clusterInfo.initFromXml(g_opts.confFile)


def setLocalReservedPort():
    """
    function: Set local reserved port in check_list.conf
    input : NA
    output: NA
    """
    portList = []
    rportList = []
    portStr = ""
    checkListfile = "%s/../gspylib/etc/conf/check_list.conf" \
                    % os.path.dirname(os.path.realpath(__file__))
    if g_clusterInfo is not None:
        for dbNode in g_clusterInfo.dbNodes:
            for cn in dbNode.coordinators:
                if cn.port not in portList:
                    portList.append(cn.port)
                if cn.haPort != "" and cn.haPort != (int(cn.port) + 1):
                    if cn.haPort not in portList:
                        portList.append(cn.haPort)
            for dn in dbNode.datanodes:
                if dn.port not in portList:
                    portList.append(dn.port)
                if dn.haPort != "" and dn.haPort != (int(dn.port) + 1):
                    if dn.haPort not in portList:
                        portList.append(dn.haPort)
            for cm in dbNode.cmservers:
                if cm.port not in portList:
                    portList.append(cm.port)
                if cm.haPort != "" and cm.haPort != (int(cm.port) + 1):
                    if cm.haPort not in portList:
                        portList.append(cm.haPort)
            for gtm in dbNode.gtms:
                if gtm.port not in portList:
                    portList.append(gtm.port)
                if gtm.haPort != "" and gtm.haPort != (int(gtm.port) + 1):
                    if gtm.haPort not in portList:
                        portList.append(gtm.haPort)
            for etcd in dbNode.etcds:
                if etcd.port not in portList:
                    portList.append(etcd.port)
                if etcd.haPort != "" and etcd.haPort != (int(etcd.port) + 1):
                    if etcd.haPort not in portList:
                        portList.append(etcd.haPort)
        if 20050 not in portList:
            portList.append(20050)
        sorted(portList)
        for port in portList:
            localPortList = []
            nport = port
            while nport <= port + 7:
                if len(rportList) != 0 and port <= max(rportList[-1]) + 1:
                    if nport not in rportList[-1]:
                        rportList[-1].append(nport)
                else:
                    if nport not in localPortList:
                        localPortList.append(nport)
                nport += 1
            if len(localPortList) != 0:
                rportList.append(localPortList)
        for rport in rportList:
            if rport == rportList[-1]:
                portStr += "%s-%s" % (min(rport), max(rport))
            else:
                portStr += "%s-%s," % (min(rport), max(rport))
        cmd = "sed -i '/%s/d' %s && sed -i '/%s/a\%s = %s' %s " % \
              ("ipv4.ip_local_reserved_ports", checkListfile, "tcp_retries2",
               "net.ipv4.ip_local_reserved_ports", portStr, checkListfile)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                            + "Error:\n%s" % str(output))


def doLocalCheck():
    """
    function: check OS item on local node
    input : NA
    output: NA
    """

    global resultList
    global netWorkBondInfo
    netWorkBondInfo = netWork()
    if (g_opts.action == "osKernelParameterCheck"):
        CheckSection(actioItemMap["Check_FileSystem_Configure"][0],
                     actioItemMap["Check_FileSystem_Configure"][1])
        CheckSection(actioItemMap["Check_SysCtl_Parameter"][0],
                     actioItemMap["Check_SysCtl_Parameter"][1])
    else:
        CheckSection(actioItemMap[g_opts.action][0],
                     actioItemMap[g_opts.action][1])


# checkItemMap is a dictionary of global variable
# checkItemMap.keys() is configuration file's section
# checkItemMap[key][0] is the check function about the key
# checkItemMap[key][1] is the parameter of the check function
checkItemMap = {'/etc/sysctl.conf':
                    [checkSysctlParameter, (paraList, g_check_os)],
                '/etc/security/limits.conf':
                    [checkLimitsParameter, (paraList, g_check_os)]
                }

if __name__ == '__main__':
    """
    main function
    """
    try:
        # parse cmd lines
        parseCommandLine()
        # check Parameter
        checkParameter()
        # init globals
        initGlobals()
        setLocalReservedPort()
        # check OS item on local node
        doLocalCheck()
    except Exception as e:
        GaussLog.exitWithError(str(e))
    finally:
        # close log file
        g_logger.closeLog()
    sys.exit(0)
