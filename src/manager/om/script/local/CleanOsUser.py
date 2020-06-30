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
# Description  : CleanOsUser.py is a utility to clean OS user.
#############################################################################
import getopt
import sys
import subprocess

sys.path.append(sys.path[0] + "/../")
from gspylib.common.GaussLog import GaussLog
from gspylib.common.ParameterParsecheck import Parameter
from gspylib.common.Common import DefaultValue
from gspylib.common.ErrorCode import ErrorCode
from gspylib.common.LocalBaseOM import LocalBaseOM


class CleanOsUser(LocalBaseOM):
    '''
    This class is for cleaning os user, it will not cleaning group.
    '''

    def __init__(self):
        '''
        Constructor
        '''
        self.userProfile = ""
        self.user = ""
        self.logger = None

    ##########################################################################
    # Help context.
    ##########################################################################
    def usage(self):
        """
        function: usage
        input  : NA
        output : NA
        """
        print("CleanOsUser.py is a utility to clean OS user.")
        print(" ")
        print("Usage:")
        print("  python3 CleanOsUser.py --help")
        print("  python3 CleanOsUser.py -U user")
        print(" ")
        print("Common options:")
        print("  -U        the database program and cluster owner")
        print("  --help    show this help, then exit")
        print(" ")

    def __checkParameters(self):
        """
        function: Check parameter from command line
        input : NA
        output: NA
        """
        try:
            opts, args = getopt.getopt(sys.argv[1:], "U:l:", ["help"])
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
            elif (key == "-l"):
                logFile = value
            elif (key == "--help"):
                self.usage()
                sys.exit(0)
            else:
                GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"]
                                       % key)

            Parameter.checkParaVaild(key, value)

        if (self.user == ""):
            GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"] % 'U'
                                   + ".")
        try:
            DefaultValue.checkUser(self.user, False)
        except Exception as e:
            GaussLog.exitWithError(str(e))

        if (logFile == ""):
            logFile = DefaultValue.getOMLogPath(DefaultValue.LOCAL_LOG_FILE,
                                                self.user, "")

        self.logger = GaussLog(logFile, "CleanOsUser")
        self.logger.ignoreErr = True

    ##########################################################################
    # This is the main clean OS user flow.
    ##########################################################################
    def cleanOsUser(self):
        """
        function: Clean OS user
        input : NA
        output: NA
        """
        self.__checkParameters()
        self.logger.log("Cleaning crash OS user.")
        try:
            # clean semaphore
            subprocess.getstatusoutput("ipcs -s|awk '/ %s /{print $2}'|"
                                       "xargs -n1 ipcrm -s" % self.user)

            # get install path
            cmd = "su - %s -c 'echo $GAUSSHOME' 2>/dev/null" % self.user
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                self.logger.logExit(ErrorCode.GAUSS_518["GAUSS_51802"]
                                    % "$GAUSSHOME" + " Error:\n%s" % output)
            gaussHome = output.strip()
            if (gaussHome == ""):
                self.logger.debug("$GAUSSHOME is null. This means you may "
                                  "must clean crash install path manually.")
            self.logger.debug("The installation path is %s." % gaussHome)

            # delete user
            status, output = subprocess.getstatusoutput("userdel -f %s"
                                                        % self.user)
            if (status != 0):
                self.logger.logExit(ErrorCode.GAUSS_503["GAUSS_50314"]
                                    % self.user + " Error: \n%s" % output)

            # delete path
            status, output = subprocess.getstatusoutput("rm -rf '%s'"
                                                        % gaussHome)
            if (status != 0):
                self.logger.logExit(ErrorCode.GAUSS_502["GAUSS_50209"]
                                    % gaussHome + " Error: \n%s" % output)

        except Exception as e:
            self.logger.logExit(str(e))
        self.logger.log("Successfully cleaned OS user.")

    def removeAllowUsers(self):
        """
        function: Remove the specific user from 'AllowUsers'
         in /etc/ssh/sshd_config
        input : NA
        output: NA
        """
        sshd_config = "/etc/ssh/sshd_config"
        try:
            cmd = "cat %s | grep -E '\\<AllowUsers\\>'" % sshd_config
            (status, output) = subprocess.getstatusoutput(cmd)
            # Not found, or there is an error.
            if status != 0:
                if output is None or len(output.lstrip()) == 0:
                    self.logger.debug("No 'AllowUsers' configuration found"
                                      " in %s" % sshd_config)
                else:
                    # Error occurred, but there is no need to report.
                    self.logger.debug("Failed to get 'AllowUsers' from %s"
                                      % sshd_config)
                return

            allowUsersLineBefore = output.lstrip()
            userList = allowUsersLineBefore.split()
            userList.remove(self.user)
            allowUsersLineRemoved = ' '.join(userList)
            cmd = "sed -i 's/%s/%s/g' %s" % (allowUsersLineBefore,
                                             allowUsersLineRemoved,
                                             sshd_config)
            (status, output) = subprocess.getstatusoutput(cmd)
            # Not found, or there is an error.
            if status != 0:
                self.logger.debug("Failed to remove user '%s' from "
                                  "'AllowUsers' in %s. Command: %s, Error: %s"
                                  % (self.user, sshd_config, cmd, output))
        except Exception as e:
            self.logger.debug("Failed to remove user '%s' from 'AllowUsers'"
                              " in %s. Error: %s" % (self.user, sshd_config,
                                                     str(e)))


if __name__ == '__main__':
    """
    main function
    """
    try:
        cleaner = CleanOsUser()
        cleaner.cleanOsUser()
        cleaner.removeAllowUsers()
    except Exception as e:
        GaussLog.exitWithError(str(e))

    sys.exit(0)
