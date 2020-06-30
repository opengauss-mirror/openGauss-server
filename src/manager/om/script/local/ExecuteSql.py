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
# Description  : ExecuteSql.py is a utility to execute sql by using libpq.
#############################################################################

import getopt
import sys
import os
import json
import subprocess

sys.path.append(sys.path[0] + "/../")
from gspylib.common.GaussLog import GaussLog
from gspylib.common.ParameterParsecheck import Parameter
from gspylib.common.Common import DefaultValue, ClusterCommand

libpath = os.path.join(DefaultValue.getEnv("GAUSSHOME"), "lib")
sys.path.append(libpath)
from gspylib.common.ErrorCode import ErrorCode
from gspylib.os.gsfile import g_file


def usage():
    """
Usage:
    python3 CheckCNStatus.py -h|--help
    python3 CheckCNStatus.py -p port -S sql -f outputfile -s snapid -d database

    General options:
      -p                               cn port
      -S                               SQL senned to be executed
      -f,                              result output file
      -s,                              snapid for special use
      -d,                              database to execute the sql
      -h, --help                       Show help information for this utility,
                                       and exit the command line mode.
    """
    print(usage.__doc__)


def main():
    """
    main function
    """
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], "p:S:f:s:d:h", ["help"])
    except Exception as e:
        usage()
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] % str(e))

    if (len(args) > 0):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"]
                               % str(args[0]))

    port = ""
    sqlfile = ""
    outputfile = ""
    database = ""
    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-p"):
            port = value
        elif (key == "-S"):
            sqlfile = value
        elif (key == "-f"):
            outputfile = value
        elif (key == "-s"):
            snapid = value
        elif (key == "-d"):
            database = value
        else:
            GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] % key)

        Parameter.checkParaVaild(key, value)

    # check parameter
    if (port == ""):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"]
                               % 'p' + ".")
    if (sqlfile == ""):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"]
                               % 'S' + ".")
    if (outputfile == ""):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"]
                               % 'f' + ".")
    if (database == ""):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"]
                               % 'd' + ".")
    try:
        output = {}
        exesql = ""
        if os.path.exists(sqlfile):
            with open(sqlfile, "r") as fp:
                lines = fp.readlines()
                for line in lines:
                    exesql += line + "\n"
        (status, result, err_output) = \
            ClusterCommand.excuteSqlOnLocalhost(port, exesql, database)
        cmd = "rm -rf %s" % sqlfile
        if (err_output != ""):
            output["status"] = status
            output["error_output"] = err_output
            GaussLog.exitWithError(ErrorCode.GAUSS_513["GAUSS_51300"] % exesql
                                   + "Errors:%s" % err_output)
        output["status"] = status
        output["result"] = result
        output["error_output"] = err_output
        g_file.createFileInSafeMode(outputfile)
        with open(outputfile, "w") as fp_json:
            json.dump(output, fp_json)
        (status, outpout) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                            + "Error:\n%s" % output)
    except Exception as e:
        GaussLog.exitWithError("Errors:%s" % str(e))


if __name__ == '__main__':
    main()
