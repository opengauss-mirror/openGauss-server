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
# Description : LocalPerformanceCheck.py is a utility to
#              check if GaussDB performance.
#############################################################################
import subprocess
import getopt
import os
import sys
import time

sys.path.append(sys.path[0] + "/../")
from gspylib.common.GaussLog import GaussLog
from gspylib.common.ParameterParsecheck import Parameter
from gspylib.common.Common import DefaultValue
from gspylib.common.ErrorCode import ErrorCode
from multiprocessing.dummy import Pool as ThreadPool

ACTION_SSDPerfCheck = "SSDPerfCheck"
INDENTATION_VALUE = 37


class CmdOptions():
    def __init__(self):
        """
        function: initialize variable
        input : NA
        output: NA
        """
        pass

    action = ""
    logFile = ""
    user = ""


g_opts = CmdOptions()
g_logger = None
g_perfChecker = None


class LocalPerformanceCheck():
    def __init__(self):
        """
        function: initialize variable
        input : NA
        output: NA
        """
        self.user = ""
        self.logFile = ""
        self.action = ""

    def CheckSSDPerf(self):
        """
        function: check SSD performance
        input : NA
        output: NA
        """
        diskDevList = []
        # Obtain the SSD device
        devList = DefaultValue.obtainSSDDevice()
        # traverse dev
        for dev in devList:
            cmd = "df -P -h | grep %s | awk '{print $6}'" % dev
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status == 0):
                if (output == ""):
                    continue
                diskDirInfo = output.split('\n')
                for diskDir in diskDirInfo:
                    diskDevList.append("%s:%s" % (dev, diskDir))
        # check if SSD disk exists on current node
        if diskDevList == []:
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53005"])
        # Concurrent execution
        pool = ThreadPool(DefaultValue.getCpuSet())
        results = pool.map(self.CheckSingleSSDPerf, diskDevList)
        pool.close()
        pool.join()

    def CheckSingleSSDPerf(self, diskDev):
        """
        function: check Single SSD performance
        input : diskDev
        output: NA
        """
        try:
            devlist = diskDev.split(':')
            dev = devlist[0]
            diskDir = devlist[1]
            # get current time
            currentTime = time.strftime("%Y-%m-%d_%H%M%S")
            # get tmp File
            tmpFile = os.path.join(diskDir, "%s-%s-%d" % ("tmpfile_SSDperf",
                                                          currentTime,
                                                          os.getpid()))
            cmd = "dd if=/dev/zero of=%s bs=8M count=2560 oflag=direct &&" \
                  % tmpFile
            cmd += "dd if=%s of=/dev/null bs=8M count=2560 iflag=direct" \
                   % tmpFile
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status == 0):
                output = output.split("\n")
                writeInfolist = output[2].strip().split(",")
                readInfolist = output[5].strip().split(",")
                result = "        %s (%s) Path (%s)\n" \
                         "            %s:    %s\n" \
                         "            %s:    %s\n            %s:    %s\n" \
                         "            %s:    %s\n            %s:    %s" \
                         % (dev.split('/')[2], dev, diskDir,
                            "Data size".ljust(INDENTATION_VALUE),
                            writeInfolist[0][:-7],
                            "Write time".ljust(INDENTATION_VALUE),
                            (writeInfolist[1]).strip(),
                            "Write speed".ljust(INDENTATION_VALUE),
                            (writeInfolist[2]).strip(),
                            "Read time".ljust(INDENTATION_VALUE),
                            (readInfolist[1]).strip(),
                            "Read speed".ljust(INDENTATION_VALUE),
                            (readInfolist[2]).strip())
                g_logger.log(result)
            else:
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                                + " Error:\n%s" % output)
            os.remove(tmpFile)
            g_logger.debug("Successfully checked SSD performance.")
        except Exception as e:
            os.remove(tmpFile)
            g_logger.log("%s failed." % g_opts.action)
            g_logger.debug(str(e))


def usage():
    """
localPerfCheck.py is a utility to check if GaussDB performance.
Internal use only.
Usage:
    python3 --help | -?
    python3 LocalPerformanceCheck.py -t action [-l logfile] [-U username]
Common options:
    -t                                The type of action.
    -U                                The user and group name.
    -l                                The path of log file.
    -? --help                         Show this help screen.
    """
    print(usage.__doc__)


def initGlobal():
    """
    function: Init global variables
    input : NA
    output: NA
    """
    global g_logger
    global g_perfChecker

    try:
        g_logger = GaussLog(g_opts.logFile, g_opts.action)
        # Modify log File Permissions
        DefaultValue.modifyFileOwner(g_opts.user, g_logger.logFile)
        g_perfChecker = LocalPerformanceCheck()
    except Exception as e:
        g_logger.logExit(str(e))


def parseCommandLine():
    """
    function: Parse command line and save to global variable
    input : NA
    output: NA
    """
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], "t:U:l:", ["help"])
    except Exception as e:
        # print help information
        usage()
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"]
                               % str(e))

    if (len(args) > 0):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"]
                               % str(args[0]))

    # parse parameter
    for (key, value) in opts:
        if (key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-t"):
            g_opts.action = value
        elif (key == "-l"):
            g_opts.logFile = value
        elif (key == "-U"):
            g_opts.user = value

        Parameter.checkParaVaild(key, value)


def checkParameter():
    """
    function: Check parameter from command line
    input : NA
    output: NA
    """
    # check if user exist and is the right user
    DefaultValue.checkUser(g_opts.user)
    # check log file
    if (g_opts.logFile == ""):
        g_opts.logFile = DefaultValue.getOMLogPath(
            DefaultValue.LOCAL_LOG_FILE, g_opts.user, "")
    # check if absolute path
    if (not os.path.isabs(g_opts.logFile)):
        GaussLog.exitWithError(ErrorCode.GAUSS_502["GAUSS_50213"] % "log")
    # check if installed SSD
    if not DefaultValue.checkSSDInstalled():
        GaussLog.exitWithError(ErrorCode.GAUSS_530["GAUSS_53008"])


def docheck():
    """
    function: check SSD performance
    input : NA
    output: NA
    """
    if (g_opts.action == ACTION_SSDPerfCheck):
        # check SSD performance
        g_perfChecker.CheckSSDPerf()
    else:
        g_logger.logExit(ErrorCode.GAUSS_500["GAUSS_50000"] % g_opts.action)


if __name__ == '__main__':
    """
    main function
    """
    try:
        # arse command line and save to global variable
        parseCommandLine()
        # Check parameter from command line
        checkParameter()
        # Init global variables
        initGlobal()
    except Exception as e:
        # Modify the file's owner
        DefaultValue.modifyFileOwner(g_opts.user, g_opts.logFile)
        GaussLog.exitWithError(str(e))

    try:
        # check SSD performance
        docheck()
        # close log file
        g_logger.closeLog()
    except Exception as e:
        # Modify the file's owner
        DefaultValue.modifyFileOwner(g_opts.user, g_logger.logFile)
        g_logger.logExit(str(e))

    sys.exit(0)
