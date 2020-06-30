#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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
import getopt

sys.path.append(sys.path[0] + "/../")
from gspylib.common.GaussLog import GaussLog
from gspylib.common.Common import DefaultValue
from gspylib.common.ErrorCode import ErrorCode
from gspylib.common.LocalBaseOM import LocalBaseOM
from gspylib.common.ParameterParsecheck import Parameter


class Start(LocalBaseOM):
    """
    The class is used to do perform start
    """

    def __init__(self):
        """
        function: initialize the parameters
        input: NA
        output: NA
        """
        super(Start, self).__init__()
        self.user = ""
        self.dataDir = ""
        self.time_out = 300
        self.logFile = ""
        self.logger = None
        self.installPath = ""
        self.security_mode = ""

    def usage(self):
        """
gs_start is a utility to start the database

Uasge:
    gs_start -? | --help
    gs_start -U USER [-D DATADIR][-t SECS][-l LOGFILE]

General options:
    -U USER                  the database program and cluster owner")
    -D DATADIR               data directory of instance
    -t SECS                  seconds to wait
    -l LOGFILE               log file
    -?, --help               show this help, then exit
        """
        print(self.usage.__doc__)

    def parseCommandLine(self):
        """
        function: Check input parameters
        input : NA
        output: NA
        """
        try:
            opts, args = getopt.getopt(sys.argv[1:], "U:D:R:l:t:h?",
                                       ["help", "security-mode="])
        except getopt.GetoptError as e:
            GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] % str(e))

        if (len(args) > 0):
            GaussLog.exitWithError(
                ErrorCode.GAUSS_500["GAUSS_50000"] % str(args[0]))

        for key, value in opts:
            if key == "-U":
                self.user = value
            elif key == "-D":
                self.dataDir = value
            elif key == "-t":
                self.time_out = int(value)
            elif key == "-l":
                self.logFile = value
            elif key == "-R":
                self.installPath = value
            elif key == "--help" or key == "-h" or key == "-?":
                self.usage()
                sys.exit(0)
            elif key == "--security-mode":
                self.security_mode = value
            else:
                GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"]
                                       % key)
            Parameter.checkParaVaild(key, value)

        if self.user == "":
            GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"]
                                   % 'U' + ".")
        if self.logFile == "":
            self.logFile = DefaultValue.getOMLogPath(
                DefaultValue.LOCAL_LOG_FILE, self.user, self.installPath)

    def __initLogger(self):
        """
        function: Init logger
        input : NA
        output: NA
        """
        self.logger = GaussLog(self.logFile, "StartInstance")

    def init(self):
        """
        function: constructor
        """
        self.__initLogger()
        self.readConfigInfo()
        self.initComponent()

    def doStart(self):
        """
        function: do start database
        input  : NA
        output : NA
        """
        isDataDirCorrect = False
        for dn in self.dnCons:
            if self.dataDir != "" and dn.instInfo.datadir != self.dataDir:
                continue
            dn.start(self.time_out, self.security_mode)
            isDataDirCorrect = True

        if not isDataDirCorrect:
            raise Exception(ErrorCode.GAUSS_536["GAUSS_53610"] % self.dataDir)


def main():
    """
    main function
    """
    try:
        start = Start()
        start.parseCommandLine()
        start.init()
    except Exception as e:
        GaussLog.exitWithError(ErrorCode.GAUSS_536["GAUSS_53608"] % str(e))
    try:
        start.doStart()
    except Exception as e:
        GaussLog.exitWithError(str(e))


if __name__ == "__main__":
    main()
