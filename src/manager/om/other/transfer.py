#!/usr/bin/env python3
#Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
#openGauss is licensed under Mulan PSL v2.
#You can use this software according to the terms and conditions of the Mulan PSL v2.
#You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
#-------------------------------------------------------------------------
#
# transfer.py
#    relfilenode to oid mapping cache.
#
# IDENTIFICATION
#    src/manager/om/other/transfer.py
#
#-------------------------------------------------------------------------

import os
import sys
import pwd
import getopt

from script.gspylib.common.DbClusterInfo import dbClusterInfo
from script.gspylib.common.Common import DefaultValue
GPPATH = os.getenv("GPHOME")
DefaultValue.checkPathVaild(GPPATH)
sys.path.insert(0, GPPATH)
from script.gspylib.common.GaussLog import GaussLog
from gspylib.common.ErrorCode import ErrorCode
from script.gspylib.threads.SshTool import SshTool

# source file path
SRCFILEPATH = ""
DRCPATH = ""
DNINSTANCEID = []
ISALLHOSTS = False
g_logger = None
g_clusterUser = ""
g_clusterInfo = None
g_sshTool = None


def usage():
    """
transfer.py is a utility to transfer C function lib file to all nodes or standy node.

Usage:
  transfer.py -? | --help
  transfer.py 1 sourcefile  destinationpath            copy sourcefile to Cluster all nodes.
  transfer.py 2 sourcefile  pgxc_node_name             copy sourcefile to the same path of node contain pgxc_node_name standy instance.
    """

    print (usage.__doc__)


def initGlobals():
    global g_logger
    global g_clusterUser
    global g_clusterInfo
    global g_sshTool

    if os.getuid() == 0:
        GaussLog.exitWithError(ErrorCode.GAUSS_501["GAUSS_50105"])
        sys.exit(1)
    # Init user
    g_clusterUser = pwd.getpwuid(os.getuid()).pw_name
    # Init logger
    logFile = DefaultValue.getOMLogPath(DefaultValue.LOCAL_LOG_FILE, g_clusterUser, "", "")
    g_logger = GaussLog(logFile, "Transfer_C_function_file")
    # Init ClusterInfo
    g_clusterInfo = dbClusterInfo()
    g_clusterInfo.initFromStaticConfig(g_clusterUser)
    # Init sshtool
    g_sshTool = SshTool(g_clusterInfo.getClusterNodeNames(), g_logger.logFile)


def checkSrcFile(srcFile):
    g_logger.log("Check whether the source file exists.")
    if not os.path.isfile(srcFile):
        g_logger.debug("The %s does not exist. " % srcFile)
        return False
    else:
        g_logger.log("The source file exists.")
        return True


def parseCommandLine():
    g_logger.log("Start parse parameter.")
    try:
        opts, args = getopt.getopt(sys.argv[1:], "")
        if len(args) != 3:
            raise getopt.GetoptError("The number of parameters is not equal to 3.")
    except getopt.GetoptError as e:
        g_logger.logExit("Parameter error, Error:\n%s" % str(e))

    global SRCFILEPATH
    global DRCPATH
    global DNINSTANCEID
    global ISALLHOSTS

    if args[0] not in ['1', '2']:
        g_logger.logExit("Parameter error.")
    if args[0] == "1":
        ISALLHOSTS = True
        if not checkSrcFile(args[1]):
            g_logger.logExit("Parameter error.")
        SRCFILEPATH = args[1]
        DRCPATH = args[2]
    elif args[0] == "2":
        if not checkSrcFile(args[1]):
            g_logger.logExit("Parameter error.")
        SRCFILEPATH = args[1]
        nodenamelst = args[2].split("_")
        # when the clustertype is primary-standy-dummy,the standby DNinstence ID is the third arg in "nodenamelst"
        if len(nodenamelst) == 3:
            DNINSTANCEID.append(nodenamelst[2])
            return
        # when the clustertype is primary-multi-standby,the standby DNinstence IDs are following the third parameter
        for dnId in nodenamelst[2:]:
            DNINSTANCEID.append(dnId)
    else:
        g_logger.logExit("Parameter error.")
    g_logger.log("Successfully parse parameter.")


def scpFileToAllHost(srcFile, drcpath):
    try:
        g_logger.log("Transfer C function file to all hosts.")
        g_sshTool.scpFiles(srcFile, drcpath, g_clusterInfo.getClusterNodeNames())
        cmd = "chmod 600 '%s'" % drcpath
        g_sshTool.executeCommand(cmd,
                                 "Transfer C function file to all hosts.",
                                 DefaultValue.SUCCESS,
                                 g_clusterInfo.getClusterNodeNames())
    except Exception as e:
        raise Exception(ErrorCode.GAUSS_536["GAUSS_53611"] % str(e))


def scpFileToStandy(srcFile, InstanceID):
    try:
        g_logger.log("Transfer C function file to standy node.")

        mirrorID = 0
        peerNode = []
        # Get  instance mirrorID by InstanceID
        for dbNode in g_clusterInfo.dbNodes:
            for dbInst in dbNode.datanodes:
                if str(dbInst.instanceId) == InstanceID:
                    mirrorID = dbInst.mirrorId

        if mirrorID == 0:
            g_logger.logExit("Failed to find primary instance mirrorId.")

            # Get standy instance
        for node in g_clusterInfo.dbNodes:
            for instance in node.datanodes:
                if instance.mirrorId == mirrorID and (instance.instanceType == 1 or instance.instanceType == 0):
                    peerNode.append(node.name)

        # send SOFile to peerInstance
        (despath, sofile) = os.path.split(srcFile)
        for deshost in peerNode:
            status = g_sshTool.checkRemoteFileExist(deshost, srcFile, "")
            if not status:
                g_sshTool.scpFiles(srcFile, despath, [deshost])
    except Exception as e:
        raise Exception(ErrorCode.GAUSS_536["GAUSS_53611"] % str(e))


if __name__ == '__main__':
    # help info
    if "-?" in sys.argv[1:] or "--help" in sys.argv[1:]:
        usage()
        sys.exit(0)

    # Init globle
    initGlobals()
    g_logger.log("Start transfer C function file.")

    # parse command line
    parseCommandLine()
    # start send soFile
    try:
        if ISALLHOSTS:
            scpFileToAllHost(SRCFILEPATH, DRCPATH)
        else:
            for dnInstanceId in DNINSTANCEID:
                scpFileToStandy(SRCFILEPATH, dnInstanceId)
    except Exception as e:
        g_logger.logExit("Failed to transfer C function file. Error:%s" % str(e))
    g_logger.log("Successfully transfer C function file.")
    sys.exit(0)
