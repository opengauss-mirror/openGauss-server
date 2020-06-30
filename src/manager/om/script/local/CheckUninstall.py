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
# Description  : CheckUninstall.py is a utility to check the
#               instance status on local node.
#############################################################################

import getopt
import os
import sys
import platform
import subprocess

sys.path.append(sys.path[0] + "/../")
from gspylib.common.GaussLog import GaussLog
from gspylib.common.Common import DefaultValue
from gspylib.common.ParameterParsecheck import Parameter
from gspylib.common.ErrorCode import ErrorCode


class CheckUninstall:
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
        self.installPath = ""
        self.user = ""
        self.cleanUser = False
        self.cleanData = False
        self.logger = None

    ##########################################################################
    # Help context. U:R:oC:v:
    ##########################################################################
    def usage(self):
        """
        function: usage
        input  : NA
        output : NA
        """
        print("CheckUninstall.py is a utility to check Gauss MPP Database"
              " status .")
        print(" ")
        print("Usage:")
        print("  python3 CheckUninstall.py --help")
        print("  python3 CheckUninstall.py -R installpath -U user [-d] [-u]"
              " [-l log]")
        print(" ")
        print("Common options:")
        print("  -U        the database program and cluster owner")
        print("  -R        the database program path")
        print("  -d        clean data path")
        print("  -u        clean user")
        print("  -l        log directory")
        print("  --help    show this help, then exit")
        print(" ")

    ##########################################################################
    # check uninstall
    ##########################################################################
    def checkUninstall(self):
        """
        function:
            Check all kinds of environment. It includes:
            1. Input parameters.
            2. OS version.
            3. User Info
            4. If it has a old install.
        input : NA
        output: NA
        """
        self.__checkParameters()
        self.__checkOSVersion()
        self.__checkOsUser()
        self.__checkInstanllPath()
        self.logger.closeLog()

    def __checkParameters(self):
        """
        function: check input parameters
        input : NA
        output: NA
        """
        try:
            opts, args = getopt.getopt(sys.argv[1:], "U:R:l:du", ["help"])
        except getopt.GetoptError as e:
            GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"]
                                   % str(e))

        if (len(args) > 0):
            GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"]
                                   % str(args[0]))

        logFile = ""
        for key, value in opts:
            if (key == "-U"):
                self.user = value
            elif (key == "-R"):
                self.installPath = value
            elif (key == "-l"):
                logFile = value
            elif (key == "-d"):
                self.cleanData = True
            elif (key == "-u"):
                self.cleanUser = True
            elif (key == "--help"):
                self.usage()
                sys.exit(0)
            else:
                GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"]
                                       % key)

            Parameter.checkParaVaild(key, value)

        if (self.user == ""):
            GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"]
                                   % 'U' + ".")

        if (self.installPath == ""):
            raise Exception(ErrorCode.GAUSS_500["GAUSS_50001"] % 'R' + ".")

        if (logFile == ""):
            logFile = DefaultValue.getOMLogPath(DefaultValue.LOCAL_LOG_FILE,
                                                "", self.installPath, "")

        self.logger = GaussLog(logFile, "CheckUninstall")
        self.logger.debug("The installation path of program: "
                          + self.installPath)
        self.logger.debug("The parameter of clean user is: %s."
                          % self.cleanUser)
        self.logger.debug("The parameter of clean data is: %s."
                          % self.cleanData)

    def __checkOSVersion(self):
        """
        function: Check operator system version, install binary file version.
        input : NA
        output: NA
        """
        self.logger.log("Checking OS version.")
        try:
            if (not DefaultValue.checkOsVersion()):
                raise Exception(ErrorCode.GAUSS_519["GAUSS_51900"]
                                + "The current system is: %s."
                                % platform.platform())
        except Exception as e:
            raise Exception(str(e))

        self.logger.log("Successfully checked OS version.")

    def __checkOsUser(self):
        """
        function: Check if user exists and get $GAUSSHOME
        input : NA
        output: NA
        """
        if not self.cleanUser:
            self.logger.log("Skipping user check. ")
            return

        self.logger.log("Checking OS user.")
        try:
            DefaultValue.checkUser(self.user, False)
        except Exception as e:
            raise Exception(str(e))

        # Get GAUSSHOME
        cmd = "echo $GAUSSHOME 2>/dev/null"
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            self.logger.debug("The cmd is %s " % cmd)
            raise Exception(ErrorCode.GAUSS_518["GAUSS_51802"]
                            % "$GAUSSHOME" + " Error:\n%s" % output)

        gaussHome = output.strip()
        if (gaussHome == ""):
            raise Exception(ErrorCode.GAUSS_518["GAUSS_51800"] % "$GAUSSHOME")

        if (gaussHome != self.installPath):
            self.logger.debug("$GAUSSHOME: %s." % gaussHome)
            self.logger.debug("Installation path parameter: %s."
                              % self.installPath)
            raise Exception(ErrorCode.GAUSS_518["GAUSS_51807"])
        self.logger.log("Successfully checked OS user.")

    def __checkInstanllPath(self):
        """
        function: Check if path exists and get owner
        input : NA
        output: NA
        """
        self.logger.log("Checking installation path.")
        if (not os.path.exists(self.installPath)):
            self.logger.log("Installation path does not exist: %s."
                            % self.installPath)
            if (not self.cleanData and not self.cleanUser):
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50219"]
                                % "installation path")
        else:
            # Get owner
            cmd = "stat -c '%%U:%%G' %s" % self.installPath
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                self.logger.debug("The cmd is %s " % cmd)
                raise Exception(ErrorCode.GAUSS_503["GAUSS_50308"]
                                + " Error: \n%s" % str(output))

            owerInfo = output.strip()
            (user, group) = owerInfo.split(':')
            if (self.user != user.strip()):
                self.logger.debug("The owner information of installation"
                                  " path: %s." % owerInfo)
                self.logger.debug("User parameter : %s." % self.user)
                raise Exception(ErrorCode.GAUSS_503["GAUSS_50315"]
                                % (self.user, self.installPath))
        self.logger.log("Successfully checked installation path.")


if __name__ == '__main__':
    """
    main function
    """
    try:
        checker = CheckUninstall()
        checker.checkUninstall()
    except Exception as e:
        GaussLog.exitWithError(str(e))

    sys.exit(0)
