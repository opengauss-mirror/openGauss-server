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
# Description :
# UpgradeUtility.py is a utility to execute upgrade on each local node
#############################################################################

import getopt
import sys
import os
import subprocess
import pwd
import re
import time
import traceback
import json
import platform
from multiprocessing.dummy import Pool as ThreadPool

sys.path.append(sys.path[0] + "/../")
from gspylib.common.GaussLog import GaussLog
from gspylib.common.Common import DefaultValue, ClusterCommand, \
    ClusterInstanceConfig
from gspylib.common.ParameterParsecheck import Parameter
from gspylib.common.DbClusterInfo import dbClusterInfo
from gspylib.common.ErrorCode import ErrorCode
from gspylib.os.gsfile import g_file
import impl.upgrade.UpgradeConst as const

INSTANCE_TYPE_UNDEFINED = -1
MASTER_INSTANCE = 0
STANDBY_INSTANCE = 1
DUMMY_STANDBY_INSTANCE = 2
# init value
INSTANCE_ROLE_UNDEFINED = -1
# cn
INSTANCE_ROLE_COODINATOR = 3
# dn
INSTANCE_ROLE_DATANODE = 4

BINARY_UPGRADE_TMP = "binary_upgrade"
PG_LOCATION = "pg_location"
CFDUMPPREFIX = "cfdump"

# Global parameter
g_oldVersionModules = None
g_clusterInfo = None
g_oldClusterInfo = None
g_logger = None
g_dbNode = None
g_opts = None
g_DWS_mode = False
g_gausshome = None


class CmdOptions():
    """
    Class to define some cmd options
    """

    def __init__(self):
        """
        function: constructor
        """
        # action value
        self.action = ""
        # user value
        self.user = ""
        # app install path
        self.appPath = ""
        # env file
        self.mpprcFile = ""
        self.userProfile = ""
        # log file
        self.logFile = ""
        # backup path
        self.bakPath = ""
        # old cluster version
        self.oldVersion = ""
        # xml file
        self.xmlFile = ""
        # inplace upgrade bak path or grey upgrade path
        self.upgrade_bak_path = ""
        self.rollback = False
        self.forceRollback = False
        self.oldClusterAppPath = ""
        self.newClusterAppPath = ""
        self.gucStr = ""
        self.postgisSOFileList = \
            {"postgis-*.*.so": "lib/postgresql/",
             "libgeos_c.so.*": "lib/",
             "libproj.so.*": "lib/",
             "libjson-c.so.*": "lib/",
             "libgeos-*.*.*so": "lib/",
             "postgis--*.*.*.sql": "share/postgresql/extension/",
             "postgis.control": "share/postgresql/extension/",
             "pgsql2shp": "bin/",
             "shp2pgsql": "bin/",
             "libgcc_s.so.*": "lib/",
             "libstdc++.so.*": "lib/"}


class OldVersionModules():
    """
    Class for providing some functions to apply old version cluster
    """
    def __init__(self):
        """
        function: constructor
        """
        # old cluster information module
        self.oldDbClusterInfoModule = None
        # old cluster status module
        self.oldDbClusterStatusModule = None


def importOldVersionModules():
    """
    function: import some needed modules from the old cluster.
    currently needed are: DbClusterInfo
    input: NA
    output:NA
    """
    # get install directory by user name
    installDir = DefaultValue.getInstallDir(g_opts.user)
    if installDir == "":
        GaussLog.exitWithError(
            ErrorCode.GAUSS_503["GAUSS_50308"] + " User: %s." % g_opts.user)
    # import DbClusterInfo module
    global g_oldVersionModules
    g_oldVersionModules = OldVersionModules()
    sys.path.append("%s/bin/script/util" % installDir)
    g_oldVersionModules.oldDbClusterInfoModule = __import__('DbClusterInfo')


def initGlobals():
    """
    function: init global variables
    input: NA
    output: NA
    """
    global g_oldVersionModules
    global g_clusterInfo
    global g_oldClusterInfo
    global g_logger
    global g_dbNode
    # make sure which env file we use
    g_opts.userProfile = g_opts.mpprcFile

    # init g_logger
    g_logger = GaussLog(g_opts.logFile, g_opts.action)

    if g_opts.action in [const.ACTION_RESTORE_CONFIG,
                         const.ACTION_SWITCH_BIN,
                         const.ACTION_CLEAN_INSTALL_PATH]:
        g_logger.debug(
            "No need to init cluster information under action %s."
            % g_opts.action)
        return
    # init g_clusterInfo
    # not all action need init g_clusterInfo
    try:
        g_clusterInfo = dbClusterInfo()
        if g_opts.xmlFile == "" or not os.path.exists(g_opts.xmlFile):
            g_clusterInfo.initFromStaticConfig(g_opts.user)
        else:
            g_clusterInfo.initFromXml(g_opts.xmlFile)
    except Exception as e:
        g_logger.debug(traceback.format_exc())
        g_logger.error(str(e))
        # init cluster info from install path failed
        # try to do it from backup path again
        g_opts.bakPath = DefaultValue.getTmpDirFromEnv() + "/"
        staticConfigFile = "%s/cluster_static_config" % g_opts.bakPath

        if os.path.isfile(staticConfigFile):
            try:
                # import old module
                g_oldVersionModules = OldVersionModules()
                sys.path.append(os.path.dirname(g_opts.bakPath))
                g_oldVersionModules.oldDbClusterInfoModule = __import__(
                    'OldDbClusterInfo')
                # init old cluster config
                g_clusterInfo = \
                    g_oldVersionModules.oldDbClusterInfoModule.dbClusterInfo()
                g_clusterInfo.initFromStaticConfig(g_opts.user,
                                                   staticConfigFile)
            except Exception as e:
                g_logger.error(str(e))
                # maybe the old cluster is V1R5C00 TR5 version,
                # not support specify static config file
                # path for initFromStaticConfig function,
                # so use new cluster format try again
                try:
                    g_clusterInfo = dbClusterInfo()
                    g_clusterInfo.initFromStaticConfig(g_opts.user,
                                                       staticConfigFile)
                except Exception as e:
                    g_logger.error(str(e))
                    try:
                        # import old module
                        importOldVersionModules()
                        # init old cluster config
                        g_clusterInfo = \
                            g_oldVersionModules \
                                .oldDbClusterInfoModule.dbClusterInfo()
                        g_clusterInfo.initFromStaticConfig(g_opts.user)
                    except Exception as e:
                        raise Exception(str(e))
        elif g_opts.xmlFile and os.path.exists(g_opts.xmlFile):
            try:
                sys.path.append(sys.path[0] + "/../../gspylib/common")
                curDbClusterInfoModule = __import__('DbClusterInfo')
                g_clusterInfo = curDbClusterInfoModule.dbClusterInfo()
                g_clusterInfo.initFromXml(g_opts.xmlFile)
            except Exception as e:
                raise Exception(str(e))
        else:
            try:
                # import old module
                importOldVersionModules()
                # init old cluster config
                g_clusterInfo = \
                    g_oldVersionModules.oldDbClusterInfoModule.dbClusterInfo()
                g_clusterInfo.initFromStaticConfig(g_opts.user)
            except Exception as e:
                raise Exception(str(e))

    # init g_dbNode
    localHost = DefaultValue.GetHostIpOrName()
    g_dbNode = g_clusterInfo.getDbNodeByName(localHost)
    if g_dbNode is None:
        raise Exception(
            ErrorCode.GAUSS_512["GAUSS_51209"] % ("NODE", localHost))


def usage():
    """
Usage:
  python3 UpgradeUtility.py -t action [-U user] [-R path] [-l log]

Common options:
  -t                               the type of action
  -U                               the user of old cluster
  -R                               the install path of cluster
  -l                               the path of log file
  -V                               original Version
  -X                               the xml configure file
  --help                           show this help, then exit
  --upgrade_bak_path               always be the $PGHOST/binary_upgrade
  --old_cluster_app_path           absolute path with old commit id
  --new_cluster_app_path           absolute path with new commit id
  --rollback                       is rollback
  --guc_string                     check the guc string has been successfully
   wrote in the configure file, format is guc:value,
   can only check upgrade_from, upgrade_mode
    """
    print(usage.__doc__)


def parseCommandLine():
    """
    function: Parse command line and save to global variables
    input: NA
    output: NA
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], "t:U:R:l:V:X:",
                                   ["help", "upgrade_bak_path=",
                                    "old_cluster_app_path=",
                                    "new_cluster_app_path=", "rollback",
                                    "force", "guc_string="])
    except Exception as e:
        usage()
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] % str(e))

    if len(args) > 0:
        GaussLog.exitWithError(
            ErrorCode.GAUSS_500["GAUSS_50000"] % str(args[0]))

    for (key, value) in opts:
        if key == "--help":
            usage()
            sys.exit(0)
        elif key == "-t":
            g_opts.action = value
        elif key == "-U":
            g_opts.user = value
        elif key == "-R":
            g_opts.appPath = value
        elif key == "-l":
            g_opts.logFile = os.path.realpath(value)
        elif key == "-V":
            g_opts.oldVersion = value
        elif key == "-X":
            g_opts.xmlFile = os.path.realpath(value)
        elif key == "--upgrade_bak_path":
            g_opts.upgrade_bak_path = os.path.normpath(value)
        elif key == "--old_cluster_app_path":
            g_opts.oldClusterAppPath = os.path.normpath(value)
        elif key == "--new_cluster_app_path":
            g_opts.newClusterAppPath = os.path.normpath(value)
        elif key == "--rollback":
            g_opts.rollback = True
        elif key == "--force":
            g_opts.forceRollback = True
        elif key == "--guc_string":
            g_opts.gucStr = value
        else:
            GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] % key)

        Parameter.checkParaVaild(key, value)


def checkParameter():
    """
    function: check parameter for different action
    input: NA
    output: NA
    """
    # check mpprc file path
    g_opts.mpprcFile = DefaultValue.getMpprcFile()
    # the value of "-t" can not be ""
    if g_opts.action == "":
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"] % "t" + ".")

    # check the value of "-t"
    if g_opts.action in [const.ACTION_SWITCH_PROCESS,
                         const.ACTION_COPY_CERTS] and \
            (not g_opts.newClusterAppPath or not g_opts.oldClusterAppPath):
        GaussLog.exitWithError(
            ErrorCode.GAUSS_500["GAUSS_50001"]
            % "-new_cluster_app_path and --old_cluster_app_path")
    elif g_opts.action in \
            [const.ACTION_SYNC_CONFIG,
             const.ACTION_RESTORE_CONFIG] and not g_opts.newClusterAppPath:
        GaussLog.exitWithError(
            ErrorCode.GAUSS_500["GAUSS_50001"] % "-new_cluster_app_path")
    elif g_opts.action in \
            [const.ACTION_SWITCH_BIN,
             const.ACTION_CLEAN_INSTALL_PATH] and not g_opts.appPath:
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"] % "R")
    # Check the incoming parameter -U
    if g_opts.user == "":
        g_opts.user = pwd.getpwuid(os.getuid()).pw_name
    # Check the incoming parameter -l
    if g_opts.logFile == "":
        g_opts.logFile = DefaultValue.getOMLogPath(DefaultValue.LOCAL_LOG_FILE,
                                                   g_opts.user, "")

    global g_gausshome
    g_gausshome = DefaultValue.getInstallDir(g_opts.user)
    if g_gausshome == "":
        GaussLog.exitWithError(
            ErrorCode.GAUSS_518["GAUSS_51800"] % "$GAUSSHOME")
    g_gausshome = os.path.normpath(g_gausshome)


def switchBin():
    """
    function: switch link bin from old to new
    input  : NA
    output : NA
    """
    if g_opts.forceRollback:
        if not os.path.exists(g_opts.appPath):
            g_file.createDirectory(g_opts.appPath, True,
                                   DefaultValue.KEY_DIRECTORY_MODE)
    g_logger.log("Switch to %s." % g_opts.appPath)
    if g_opts.appPath == g_gausshome:
        raise Exception(ErrorCode.GAUSS_502["GAUSS_50233"] % (
            "install path", "$GAUSSHOME"))
    if os.path.exists(g_gausshome):
        if os.path.samefile(g_opts.appPath, g_gausshome):
            g_logger.log(
                "$GAUSSHOME points to %s. No need to switch." % g_opts.appPath)
    cmd = "ln -snf %s %s" % (g_opts.appPath, g_gausshome)
    g_logger.log("Command for switching binary directory: '%s'." % cmd)
    (status, output) = subprocess.getstatusoutput(cmd)
    if status != 0:
        raise Exception(
            ErrorCode.GAUSS_508["GAUSS_50803"] + " Error: \n%s" % str(output))


def readPostgresqlConfig(filePath):
    """
    function: read postgres sql config
    input filepath
    output gucParamDict
    """
    GUC_PARAM_PATTERN = "^\\s*.*=.*$"
    pattern = re.compile(GUC_PARAM_PATTERN)
    gucParamDict = {}
    try:
        with open(filePath, 'r') as fp:
            resList = fp.readlines()
        for oneLine in resList:
            # skip blank line
            if oneLine.strip() == "":
                continue
            # skip comment line
            if (oneLine.strip()).startswith('#'):
                continue
            # search valid line
            result = pattern.match(oneLine)
            if result is not None:
                paramAndValue = oneLine
                # remove comment if eixst
                pos = oneLine.find(' #')
                if pos >= 0:
                    paramAndValue = oneLine[:pos]
                # should use tab here
                pos = oneLine.find('\t#')
                if pos >= 0:
                    paramAndValue = oneLine[:pos]
                # if the value contain "$" ,
                # we should using "\\\\\\$" to instead of it
                resList = paramAndValue.split('=')
                if len(resList) == 2:
                    param = resList[0]
                    value = resList[1].replace("$", "\\\\\\$")
                    gucParamDict[param.strip()] = value.strip()
                elif len(resList) > 2:
                    # invalid line, skip it
                    # only support replconninfo1, replconninfo2
                    if not resList[0].strip().startswith("replconninfo"):
                        continue
                    pos = paramAndValue.find('=')
                    param = paramAndValue[:pos]
                    value = paramAndValue[pos + 1:].replace("$", "\\\\\\$")
                    gucParamDict[param.strip()] = value.strip()
                else:
                    continue
    except Exception as e:
        g_logger.debug(str(e))
        raise Exception(
            ErrorCode.GAUSS_502["GAUSS_50204"] % "postgressql.conf file")

    return gucParamDict


def syncPostgresqlconf(dbInstance):
    """
    function: syncPostgresqlconf during inplace upgrade
    input: dbInstance
    output: NA
    """
    # get config info of current node
    try:
        # get guc param info from old cluster
        gucCmd = "source %s" % g_opts.userProfile
        oldPostgresConf = "%s/postgresql.conf" % dbInstance.datadir
        gucParamDict = readPostgresqlConfig(oldPostgresConf)

        synchronousStandbyNames = ""
        # synchronous_standby_names only can be set by write file
        if "synchronous_standby_names" in gucParamDict.keys():
            synchronousStandbyNames = gucParamDict["synchronous_standby_names"]
            del gucParamDict["synchronous_standby_names"]

        # internal parameters are not supported. So skip them when do gs_guc
        internalGucList = ['block_size', 'current_logic_cluster',
                           'integer_datetimes', 'lc_collate',
                           'lc_ctype', 'max_function_args',
                           'max_identifier_length', 'max_index_keys',
                           'node_group_mode', 'segment_size',
                           'server_encoding', 'server_version',
                           'server_version_num', 'sql_compatibility',
                           'wal_block_size', 'wal_segment_size']
        for gucName in internalGucList:
            if gucName in gucParamDict.keys():
                del gucParamDict[gucName]

        if dbInstance.instanceRole == DefaultValue.INSTANCE_ROLE_DATANODE:
            # rebuild replconninfo
            connInfo1 = None
            connInfo2 = None
            dummyStandbyInst = None
            peerInsts = g_clusterInfo.getPeerInstance(dbInstance)
            if len(peerInsts) > 0:
                (connInfo1, connInfo2, dummyStandbyInst) = \
                    ClusterInstanceConfig.setReplConninfo(
                    dbInstance,
                    peerInsts,
                    g_clusterInfo)[0:3]
                gucParamDict["replconninfo1"] = "'%s'" % connInfo1
                if dummyStandbyInst is not None:
                    gucParamDict["replconninfo2"] = "'%s'" % connInfo2

        if len(gucParamDict) > 0:
            gucStr = ""
            for key, value in gucParamDict.items():
                gucStr += " -c \\\"%s=%s\\\" " % (key, value)
            gucCmd += "&& gs_guc set -D %s %s" % (dbInstance.datadir, gucStr)

        # set guc parameters about DummpyStandbyConfig at DN
        if dbInstance.instanceType == DUMMY_STANDBY_INSTANCE:
            gucstr = ""
            for entry in DefaultValue.getPrivateGucParamList().items():
                gucstr += " -c \"%s=%s\"" % (entry[0], entry[1])
            gucCmd += "&& gs_guc set -D %s %s " % (dbInstance.datadir, gucstr)

        g_logger.debug("Command for setting [%s] guc parameter:%s" % (
            dbInstance.datadir, gucCmd))

        # save guc parameter to temp file
        gucTempFile = "%s/setGucParam_%s.sh" % (
            g_opts.upgrade_bak_path, dbInstance.instanceId)
        # Do not modify the write file operation.
        # Escape processing of special characters in the content
        cmd = "echo \"%s\" > %s" % (gucCmd, gucTempFile)
        (status, output) = DefaultValue.retryGetstatusoutput(cmd)
        if status != 0:
            g_logger.debug("Command: %s. Error: \n%s" % (cmd, output))
            g_logger.logExit(
                ErrorCode.GAUSS_502["GAUSS_50205"] % gucTempFile
                + " Error: \n%s" % str(
                    output))
        g_file.changeOwner(g_opts.user, gucTempFile)
        g_file.changeMode(DefaultValue.KEY_FILE_MODE, gucTempFile)

        # replace old guc file with sample file
        newPostgresConf = "%s/share/postgresql/postgresql.conf.sample" \
                          % g_opts.newClusterAppPath
        if os.path.exists(newPostgresConf):
            g_file.cpFile(newPostgresConf, oldPostgresConf)
            g_file.changeMode(DefaultValue.KEY_FILE_MODE, oldPostgresConf)

        # set guc param
        cmd = "sh %s" % gucTempFile
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            g_logger.debug("Command: %s. Error: \n%s" % (cmd, output))
            g_logger.logExit(
                ErrorCode.GAUSS_514["GAUSS_51401"] % gucTempFile[:-3]
                + " Error: \n%s" % str(output))

        if synchronousStandbyNames != "":
            g_logger.debug(
                "Set the GUC value %s to synchronous_standby_names for %s" % (
                    synchronousStandbyNames, oldPostgresConf))
            g_file.deleteLine(oldPostgresConf,
                              "^\\s*synchronous_standby_names\\s*=.*$")
            g_file.writeFile(
                oldPostgresConf,
                ["synchronous_standby_names "
                 "= %s # standby servers that provide sync rep"
                 % synchronousStandbyNames])

        # clean temp file
        if os.path.isfile(gucTempFile):
            os.remove(gucTempFile)

    except Exception as e:
        g_logger.logExit(str(e))


def syncClusterConfig():
    """
    function: sync newly added guc during upgrade,
    for now we only sync CN/DN, gtm, cm_agent and cm_server
    input: NA
    output: NA
    """
    DnInstances = g_dbNode.datanodes
    if len(DnInstances) > 0:
        try:
            # sync postgresql.conf in parallel
            pool = ThreadPool(DefaultValue.getCpuSet())
            pool.map(syncPostgresqlconf, DnInstances)
            pool.close()
            pool.join()
        except Exception as e:
            g_logger.logExit(str(e))


def syncInstanceConfig(oldCmFile, newCmFile):
    """
    function: sync instance config
    input: NA
    output:NA
    """
    oldCmConfig = {}
    newCmConfig = {}
    newConfigItem = {}
    try:
        if not os.path.exists(oldCmFile):
            g_logger.logExit(ErrorCode.GAUSS_502["GAUSS_50201"] % oldCmFile)
        if not os.path.exists(newCmFile):
            g_logger.logExit(ErrorCode.GAUSS_502["GAUSS_50201"] % newCmFile)
        # Read and save old config file
        with open(oldCmFile, 'r') as fp:
            oldConfig = fp
            for eachLine in oldConfig:
                ParameterConfig = eachLine.strip()
                index = ParameterConfig.find("=")
                if index > 0 and ParameterConfig[0] != "#":
                    key = ParameterConfig[:index].strip()
                    value = ParameterConfig[index + 1:].strip()
                    oldCmConfig[key] = value
        # Read and save new config file
        with open(newCmFile, 'r') as fp:
            newConfig = fp
            for eachLine in newConfig:
                ParameterConfig = eachLine.strip()
                index = ParameterConfig.find("=")
                if index > 0 and ParameterConfig[0] != "#":
                    key = ParameterConfig[:index].strip()
                    value = ParameterConfig[index + 1:].strip()
                    newCmConfig[key] = value

        # Filter new configuration parameters
        for newConfig in newCmConfig.keys():
            keyExist = False
            for oldConfig in oldCmConfig.keys():
                if oldConfig == newConfig:
                    keyExist = True
                    break
            if not keyExist:
                newConfigItem[newConfig] = newCmConfig[newConfig]
        # Write new config item to old config file
        if len(newConfigItem) > 0:
            with open(oldCmFile, "a") as fp:
                for ConfigItem in newConfigItem.keys():
                    fp.write("\n%s = %s" % (ConfigItem,
                                            newConfigItem[ConfigItem]))
                fp.write("\n")
                fp.flush()

    except Exception as e:
        g_logger.logExit(str(e))


def touchInstanceInitFile():
    """
    function: touch upgrade init file for every primary and standby instance
    input: NA
    output: NA
    """
    g_logger.log("Touch init file.")
    try:
        InstanceList = []
        # find all CN instances need to touch
        if len(g_dbNode.coordinators) != 0:
            for eachInstance in g_dbNode.coordinators:
                InstanceList.append(eachInstance)
        # find all DB instances need to touch
        if len(g_dbNode.datanodes) != 0:
            for eachInstance in g_dbNode.datanodes:
                if (
                        eachInstance.instanceType == MASTER_INSTANCE
                        or eachInstance.instanceType == STANDBY_INSTANCE):
                    InstanceList.append(eachInstance)

        # touch each instance parallelly
        if len(InstanceList) != 0:
            pool = ThreadPool(len(InstanceList))
            pool.map(touchOneInstanceInitFile, InstanceList)
            pool.close()
            pool.join()
        else:
            g_logger.debug(
                "No instance found on this node, nothing need to do.")
            return

        g_logger.log(
            "Successfully created all instances init file on this node.")
    except Exception as e:
        g_logger.logExit(str(e))


def initDbInfo():
    """
    function: create a init dbInfo dict
    input: NA
    output: NA
    """
    tmpDbInfo = {}
    tmpDbInfo['dbname'] = ""
    tmpDbInfo['dboid'] = -1
    tmpDbInfo['spclocation'] = ""
    tmpDbInfo['CatalogList'] = []
    tmpDbInfo['CatalogNum'] = 0
    return tmpDbInfo


def initCatalogInfo():
    """
    function: create a init catalog dict
    input: NA
    output: NA
    """
    tmpCatalogInfo = {}
    tmpCatalogInfo['relname'] = ""
    tmpCatalogInfo['oid'] = -1
    tmpCatalogInfo['relfilenode'] = -1

    return tmpCatalogInfo


def cpDirectory(srcDir, destDir):
    """
    function: copy directory
    input  : NA
    output : NA
    """
    cmd = "rm -rf '%s' && cp -r -p '%s' '%s'" % (destDir, srcDir, destDir)
    g_logger.debug("Backup commad:[%s]." % cmd)
    (status, output) = subprocess.getstatusoutput(cmd)
    if status != 0:
        raise Exception(
            ErrorCode.GAUSS_514["GAUSS_51400"] % cmd + "\nOutput:%s" % output)


def touchOneInstanceInitFile(instance):
    """
    function: touch upgrade init file for this instance
    input: NA
    output: NA
    """
    g_logger.debug(
        "Touch instance init file. Instance data dir: %s" % instance.datadir)
    dbInfoDict = {}
    dbInfoDict["dblist"] = []
    dbInfoDict["dbnum"] = 0
    try:
        # we touch init file by executing a simple query for every database
        get_db_list_sql = """
        SELECT d.datname, d.oid, pg_catalog.pg_tablespace_location(t.oid) 
        AS spclocation 
        FROM pg_catalog.pg_database d 
        LEFT OUTER JOIN pg_catalog.pg_tablespace t 
        ON d.dattablespace = t.oid  
        ORDER BY 2;"""
        g_logger.debug("Get database info command: \n%s" % get_db_list_sql)
        (status, output) = ClusterCommand.execSQLCommand(get_db_list_sql,
                                                         g_opts.user, "",
                                                         instance.port,
                                                         "postgres", False,
                                                         "-m",
                                                         IsInplaceUpgrade=True)
        if status != 0:
            raise Exception(
                ErrorCode.GAUSS_513["GAUSS_51300"] % get_db_list_sql
                + " Error:\n%s" % output)
        if output == "":
            raise Exception(ErrorCode.GAUSS_529["GAUSS_52938"]
                            % "any database!!")
        g_logger.debug("Get database info result: \n%s." % output)
        resList = output.split('\n')
        for each_line in resList:
            tmpDbInfo = initDbInfo()
            (datname, oid, spclocation) = each_line.split('|')
            tmpDbInfo['dbname'] = datname.strip()
            tmpDbInfo['dboid'] = oid.strip()
            tmpDbInfo['spclocation'] = spclocation.strip()
            dbInfoDict["dblist"].append(tmpDbInfo)
            dbInfoDict["dbnum"] += 1

        # connect each database, run a simple query
        touch_sql = "SELECT 1;"
        for each_db in dbInfoDict["dblist"]:
            (status, output) = ClusterCommand.execSQLCommand(
                touch_sql,
                g_opts.user, "",
                instance.port,
                each_db["dbname"],
                False, "-m",
                IsInplaceUpgrade=True)
            if status != 0 or not output.isdigit():
                raise Exception(
                    ErrorCode.GAUSS_513["GAUSS_51300"] % touch_sql
                    + " Error:\n%s" % output)

    except Exception as e:
        raise Exception(str(e))

    g_logger.debug(
        "Successfully created instance init file. Instance data dir: %s"
        % instance.datadir)


def getInstanceName(instance):
    """
    function: get master instance name
    input: NA
    output: NA
    """
    instance_name = ""
    if instance.instanceRole == INSTANCE_ROLE_COODINATOR:
        instance_name = "cn_%s" % instance.instanceId
    elif instance.instanceRole == INSTANCE_ROLE_DATANODE:
        # if dn, it should be master or standby dn
        if instance.instanceType == DUMMY_STANDBY_INSTANCE:
            raise Exception(
                ErrorCode.GAUSS_529["GAUSS_52943"] % instance.instanceType)
        peerInsts = g_clusterInfo.getPeerInstance(instance)
        if len(peerInsts) != 2 and len(peerInsts) != 1:
            raise Exception(ErrorCode.GAUSS_516["GAUSS_51620"] % "peer")
        masterInst = None
        standbyInst = None
        for i in iter(peerInsts):
            if i.instanceType == MASTER_INSTANCE:
                masterInst = i
                standbyInst = instance
                instance_name = "dn_%d_%d" % (
                    masterInst.instanceId, standbyInst.instanceId)
            elif i.instanceType == STANDBY_INSTANCE:
                standbyInst = i
                masterInst = instance
                instance_name = "dn_%d_%d" % (
                    masterInst.instanceId, standbyInst.instanceId)
            else:
                # we are searching master or standby DB instance,
                # if dummy dn, just continue
                continue
        if instance_name == "":
            raise Exception(ErrorCode.GAUSS_529["GAUSS_52939"]
                            % "instance name!")
    else:
        raise Exception(ErrorCode.GAUSS_529["GAUSS_52940"]
                        % instance.instanceRole)

    return instance_name.strip()


def getStandbyInstance(instance):
    """
    function: get standby instance of input master instance
    input: NA
    output: NA
    """
    if instance.instanceType != MASTER_INSTANCE:
        raise Exception(ErrorCode.GAUSS_529["GAUSS_52940"]
                        % instance.instanceType)

    if instance.instanceRole != INSTANCE_ROLE_DATANODE:
        raise Exception(ErrorCode.GAUSS_529["GAUSS_52941"] %
                        instance.instanceRole)

    peerInsts = g_clusterInfo.getPeerInstance(instance)
    if len(peerInsts) != 2 and len(peerInsts) != 1:
        raise Exception(ErrorCode.GAUSS_516["GAUSS_51620"] % "peer")

    standbyInst = None
    for i in iter(peerInsts):
        if i.instanceType == STANDBY_INSTANCE:
            standbyInst = i
    if not standbyInst:
        raise Exception(
            "Can not find standby instance of instance [%s]!"
            % instance.datadir)

    return standbyInst


def getJsonFile(instance, backup_path):
    """
    function: get json file
    input  : instance, backup_path
    output : db_and_catalog_info_file_name: str
    """
    try:
        instance_name = getInstanceName(instance)
        # load db and catalog info from json file
        if instance.instanceRole == INSTANCE_ROLE_COODINATOR:
            db_and_catalog_info_file_name = \
                "%s/cn_db_and_catalog_info_%s.json" \
                % (backup_path, instance_name)
        elif instance.instanceRole == INSTANCE_ROLE_DATANODE:
            if instance.instanceType == MASTER_INSTANCE:
                db_and_catalog_info_file_name = \
                    "%s/master_dn_db_and_catalog_info_%s.json" \
                    % (backup_path, instance_name)
            elif instance.instanceType == STANDBY_INSTANCE:
                db_and_catalog_info_file_name = \
                    "%s/standby_dn_db_and_catalog_info_%s.json" \
                    % (backup_path, instance_name)
            else:
                raise Exception(
                    ErrorCode.GAUSS_529["GAUSS_52943"] % instance.instanceType)
        else:
            raise Exception(ErrorCode.GAUSS_529["GAUSS_52941"] %
                            instance.instanceRole)
        return db_and_catalog_info_file_name
    except Exception as e:
        raise Exception(str(e))


def __backup_base_folder(instance):
    """
    function: back base folder
    input  : instance
    output : NA
    """
    g_logger.debug(
        "Backup instance catalog physical files. Instance data dir: %s"
        % instance.datadir)

    backup_path = "%s/oldClusterDBAndRel/" % g_opts.upgrade_bak_path
    db_and_catalog_info_file_name = getJsonFile(instance, backup_path)

    with open(db_and_catalog_info_file_name, 'r') as fp:
        dbInfoStr = fp.read()
    dbInfoDict = {}
    dbInfoDict = json.loads(dbInfoStr)

    # get instance name
    instance_name = getInstanceName(instance)

    # backup base folder
    for each_db in dbInfoDict["dblist"]:
        if each_db["spclocation"] != "":
            if each_db["spclocation"].startswith('/'):
                tbsBaseDir = each_db["spclocation"]
            else:
                tbsBaseDir = "%s/pg_location/%s" % (
                    instance.datadir, each_db["spclocation"])
            pg_catalog_base_dir = "%s/%s_%s/%d" % (
                tbsBaseDir, DefaultValue.TABLESPACE_VERSION_DIRECTORY,
                instance_name, int(each_db["dboid"]))
        else:
            pg_catalog_base_dir = "%s/base/%d" % (
                instance.datadir, int(each_db["dboid"]))
        # for base folder, template0 need handle specially
        if each_db["dbname"] == 'template0':
            pg_catalog_base_back_dir = "%s_bak" % pg_catalog_base_dir
            cpDirectory(pg_catalog_base_dir, pg_catalog_base_back_dir)
            continue

        # handle other db's base folder
        if len(each_db["CatalogList"]) <= 0:
            raise Exception(
                ErrorCode.GAUSS_536["GAUSS_53612"] % each_db["dbname"])
        for each_catalog in each_db["CatalogList"]:
            # main/vm/fsm  -- main.1 ..
            cmd = ""
            main_file = "%s/%d" % (
                pg_catalog_base_dir, int(each_catalog['relfilenode']))
            if not os.path.isfile(main_file):
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50210"] % main_file)
            cmd = "cp -f -p '%s' '%s_bak'" % (main_file, main_file)
            seg_idx = 1
            while 1:
                seg_file = "%s/%d.%d" % (
                    pg_catalog_base_dir, int(each_catalog['relfilenode']),
                    seg_idx)
                if os.path.isfile(seg_file):
                    cmd += "&& cp -f -p '%s' '%s_bak'" % (seg_file, seg_file)
                    seg_idx += 1
                else:
                    break
            vm_file = "%s/%d_vm" % (
                pg_catalog_base_dir, int(each_catalog['relfilenode']))
            if os.path.isfile(vm_file):
                cmd += "&& cp -f -p '%s' '%s_bak'" % (vm_file, vm_file)
            fsm_file = "%s/%d_fsm" % (
                pg_catalog_base_dir, int(each_catalog['relfilenode']))
            if os.path.isfile(fsm_file):
                cmd += "&& cp -f -p '%s' '%s_bak'" % (fsm_file, fsm_file)
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(
                    ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                    + "\nOutput:%s" % output)

        # special files pg_filenode.map pg_internal.init
        cmd = ""
        pg_filenode_map_file = "%s/pg_filenode.map" % pg_catalog_base_dir
        if os.path.isfile(pg_filenode_map_file):
            if cmd == "":
                cmd = "cp -f -p '%s' '%s_bak'" % (
                    pg_filenode_map_file, pg_filenode_map_file)
            else:
                cmd += "&& cp -f -p '%s' '%s_bak'" % (
                    pg_filenode_map_file, pg_filenode_map_file)
        pg_internal_init_file = "%s/pg_internal.init" % pg_catalog_base_dir
        if os.path.isfile(pg_internal_init_file):
            if cmd == "":
                cmd = "cp -f -p '%s' '%s_bak'" % (
                    pg_internal_init_file, pg_internal_init_file)
            else:
                cmd += "&& cp -f -p '%s' '%s_bak'" % (
                    pg_internal_init_file, pg_internal_init_file)
        if cmd != 0:
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(
                    ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                    + "\nOutput:%s" % output)

    g_logger.debug(
        "Successfully backuped instance catalog physical files. "
        "Instance data dir: %s" % instance.datadir)


def __restore_base_folder(instance):
    """
    function: restore base folder
    input  : instance
    output : NA
    """
    backup_path = "%s/oldClusterDBAndRel/" % g_opts.upgrade_bak_path
    dbInfoDict = {}
    # get instance name
    instance_name = getInstanceName(instance)

    # load db and catalog info from json file
    if instance.instanceRole == INSTANCE_ROLE_COODINATOR:
        db_and_catalog_info_file_name = "%s/cn_db_and_catalog_info_%s.json" % (
            backup_path, instance_name)
    elif instance.instanceRole == INSTANCE_ROLE_DATANODE:
        if instance.instanceType == MASTER_INSTANCE:
            db_and_catalog_info_file_name = \
                "%s/master_dn_db_and_catalog_info_%s.json" \
                % (backup_path, instance_name)
        elif instance.instanceType == STANDBY_INSTANCE:
            db_and_catalog_info_file_name = \
                "%s/standby_dn_db_and_catalog_info_%s.json" \
                % (backup_path, instance_name)
        else:
            raise Exception(ErrorCode.GAUSS_529["GAUSS_52940"]
                            % instance.instanceType)
    else:
        raise Exception(ErrorCode.GAUSS_529["GAUSS_52941"]
                        % instance.instanceRole)

    with open(db_and_catalog_info_file_name, 'r') as fp:
        dbInfoStr = fp.read()
    dbInfoDict = json.loads(dbInfoStr)

    # restore base folder
    for each_db in dbInfoDict["dblist"]:
        if each_db["spclocation"] != "":
            if each_db["spclocation"].startswith('/'):
                tbsBaseDir = each_db["spclocation"]
            else:
                tbsBaseDir = "%s/pg_location/%s" % (
                    instance.datadir, each_db["spclocation"])
            pg_catalog_base_dir = "%s/%s_%s/%d" % (
                tbsBaseDir, DefaultValue.TABLESPACE_VERSION_DIRECTORY,
                instance_name, int(each_db["dboid"]))
        else:
            pg_catalog_base_dir = "%s/base/%d" % (
                instance.datadir, int(each_db["dboid"]))
        # for base folder, template0 need handle specially
        if each_db["dbname"] == 'template0':
            pg_catalog_base_back_dir = "%s_bak" % pg_catalog_base_dir
            cpDirectory(pg_catalog_base_back_dir, pg_catalog_base_dir)
            continue

        # handle other db's base folder
        if len(each_db["CatalogList"]) <= 0:
            raise Exception(
                ErrorCode.GAUSS_536["GAUSS_53612"] % each_db["dbname"])

        for each_catalog in each_db["CatalogList"]:
            # main/vm/fsm  -- main.1 ..
            cmd = ""
            main_file = "%s/%d" % (
                pg_catalog_base_dir, int(each_catalog['relfilenode']))
            if not os.path.isfile(main_file):
                g_logger.debug(
                    "Instance data dir: %s, database: %s, relnodefile: "
                    "%s does not exists."
                    % (instance.datadir, each_db["dbname"], main_file))

            cmd = "cp -f -p '%s_bak' '%s'" % (main_file, main_file)
            seg_idx = 1
            while 1:
                seg_file = "%s/%d.%d" % (
                    pg_catalog_base_dir, int(each_catalog['relfilenode']),
                    seg_idx)
                seg_file_bak = "%s_bak" % seg_file
                if os.path.isfile(seg_file):
                    if os.path.isfile(seg_file_bak):
                        cmd += "&& cp -f -p '%s' '%s'" % (
                            seg_file_bak, seg_file)
                    else:
                        cmd += "&& rm -f '%s'" % seg_file
                    seg_idx += 1
                else:
                    break

            vm_file = "%s/%d_vm" % (
                pg_catalog_base_dir, int(each_catalog['relfilenode']))
            vm_file_bak = "%s_bak" % vm_file
            if os.path.isfile(vm_file):
                if os.path.isfile(vm_file_bak):
                    cmd += "&& cp -f -p '%s' '%s'" % (vm_file_bak, vm_file)
                else:
                    cmd += "&& rm -f '%s'" % vm_file
            fsm_file = "%s/%d_fsm" % (
                pg_catalog_base_dir, int(each_catalog['relfilenode']))
            fsm_file_bak = "%s_bak" % fsm_file
            if os.path.isfile(fsm_file):
                if os.path.isfile(fsm_file_bak):
                    cmd += "&& cp -f -p '%s' '%s'" % (fsm_file_bak, fsm_file)
                else:
                    cmd += "&& rm -f '%s'" % fsm_file
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(
                    ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                    + "\nOutput:%s" % output)

        # special files pg_filenode.map pg_internal.init
        cmd = ""
        pg_filenode_map_file = "%s/pg_filenode.map" % pg_catalog_base_dir
        if os.path.isfile(pg_filenode_map_file):
            if cmd == "":
                cmd = "cp -f -p '%s_bak' '%s'" % (
                    pg_filenode_map_file, pg_filenode_map_file)
            else:
                cmd += "&& cp -f -p '%s_bak' '%s'" % (
                    pg_filenode_map_file, pg_filenode_map_file)

        pg_internal_init_file = "%s/pg_internal.init" % pg_catalog_base_dir
        if os.path.isfile(pg_internal_init_file):
            if cmd == "":
                cmd = "cp -f -p '%s_bak' '%s'" % (
                    pg_internal_init_file, pg_internal_init_file)
            else:
                cmd += "&& cp -f -p '%s_bak' '%s'" % (
                    pg_internal_init_file, pg_internal_init_file)

        if cmd != 0:
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(
                    ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                    + "\nOutput:%s" % output)


def cleanBackUpDir(backupDir):
    """
    function: clean backup dir
    input  : backupDir
    output : NA
    """
    # clean backupDir folder. First, we kill any pending backup process
    bakDir = "%s_bak" % backupDir
    backcmd = "cp -r -p %s %s" % (backupDir, bakDir)
    killCmd = DefaultValue.killInstProcessCmd(backcmd, False, 9, False)
    DefaultValue.execCommandLocally(killCmd)
    # Then do clean
    if os.path.isdir(bakDir):
        g_file.removeDirectory(bakDir)


def checkExistsVersion(instanceNames, cooInst, curCommitid):
    """
    function: check exits version
    input  : instanceNames, cooInst, curCommitid
    output : needKill False/True
    """
    needKill = False
    sql = ""
    for name in instanceNames:
        sql += "execute direct on (%s) 'select version()';" % name
    (status, output) = ClusterCommand.remoteSQLCommand(
        sql, g_opts.user,
        cooInst.hostname,
        cooInst.port, False,
        DefaultValue.DEFAULT_DB_NAME,
        IsInplaceUpgrade=True)
    g_logger.debug("Command to check version: %s" % sql)
    if status != 0 or ClusterCommand.findErrorInSql(output):
        raise Exception(
            ErrorCode.GAUSS_513["GAUSS_51300"] % sql + " Error: \n%s" % str(
                output))
    if not output:
        raise Exception(ErrorCode.GAUSS_516["GAUSS_51654"])
    resList = output.split('\n')
    pattern = re.compile(r'[(](.*?)[)]')
    for record in resList:
        versionInBrackets = re.findall(pattern, record)
        commitid = versionInBrackets[0].split(" ")[-1]
        if commitid != curCommitid:
            needKill = True
            break
    return needKill


def getTimeFormat(seconds):
    """
    format secends to h-m-s
    input:int
    output:int
    """
    seconds = int(seconds)
    if seconds == 0:
        return 0
    # Converts the seconds to standard time
    hour = seconds / 3600
    minute = (seconds - hour * 3600) / 60
    s = seconds % 60
    resultstr = ""
    if hour != 0:
        resultstr += "%dh" % hour
    if minute != 0:
        resultstr += "%dm" % minute
    return "%s%ds" % (resultstr, s)


def backupConfig():
    """
    function: backup config
    output: none
    """
    try:
        bakPath = g_opts.upgrade_bak_path
        clusterAppPath = g_clusterInfo.appPath

        # Backup cluster_static_config and cluster_dynamic_config,
        # logic_cluster_name.txt
        # cluster_static_config* at least one
        cmd = "cp -f -p '%s'/bin/*cluster_static_config* '%s'" % (
            clusterAppPath, bakPath)
        dynamic_config = "%s/bin/cluster_dynamic_config" % clusterAppPath
        logicalNameFile = "%s/bin/logic_cluster_name.txt" % clusterAppPath
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
            dynamic_config, dynamic_config, bakPath)
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
            logicalNameFile, logicalNameFile, bakPath)
        g_logger.debug("Backup command: %s" % cmd)
        DefaultValue.execCommandLocally(cmd)

        # Backup libcgroup config
        MAX_PARA_NUMBER = 20
        cgroup_file_list = []
        gs_cgroup_path = "%s/etc" % clusterAppPath
        file_name_list = os.listdir(gs_cgroup_path)
        for file_name in file_name_list:
            if file_name.endswith('.cfg'):
                gs_cgroup_config_file = "%s/%s" % (gs_cgroup_path, file_name)
                cgroup_file_list.append(gs_cgroup_config_file)

        # build cmd string list
        # Every 20 records merged into one
        i = 0
        cmdCgroup = ""
        cmdList = []
        for gs_cgroup_config_file in cgroup_file_list:
            i += 1
            cmdCgroup += " && (if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
                gs_cgroup_config_file, gs_cgroup_config_file, bakPath)
            if i % MAX_PARA_NUMBER == 0:
                cmdList.append(cmdCgroup)
                i = 0
                cmdCgroup = ""
        if cmdCgroup != "":
            cmdList.append(cmdCgroup)
        for exeCmd in cmdList:
            g_logger.debug("Backup command: %s" % cmd)
            DefaultValue.execCommandLocally(exeCmd[3:])

        # Backup libsimsearch etc files and libs files
        searchConfigFile = "%s/etc/searchletConfig.yaml" % clusterAppPath
        cmd = "(if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
            searchConfigFile, searchConfigFile, bakPath)
        searchIniFile = "%s/etc/searchServer.ini" % clusterAppPath
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
            searchIniFile, searchIniFile, bakPath)
        cmd += " && (if [ -d '%s/lib/libsimsearch' ];" \
               "then cp -r '%s/lib/libsimsearch' '%s';fi)" % (
                   clusterAppPath, clusterAppPath, bakPath)
        g_logger.debug("Backup command: %s" % cmd)
        DefaultValue.execCommandLocally(cmd)

        # Backup library file and database size file
        cmd = "cp -r '%s'/lib/postgresql/pg_plugin '%s'" % (
            clusterAppPath, bakPath)
        backup_dbsize = "%s/bin/%s" % (
            clusterAppPath, DefaultValue.DB_SIZE_FILE)
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
            backup_dbsize, backup_dbsize, bakPath)
        g_logger.debug("Backup command: %s" % cmd)
        DefaultValue.execCommandLocally(cmd)

        # sync kerberos conf files
        krbConfigFile = "%s/kerberos" % clusterAppPath
        cmd = "(if [ -d '%s' ];then cp -r '%s' '%s';fi)" % (
            krbConfigFile, krbConfigFile, bakPath)
        cmd += "&& (if [ -d '%s/var/krb5kdc' ];then mkdir %s/var;" \
               " cp -r '%s/var/krb5kdc' '%s/var/';fi)" % (
                   clusterAppPath, bakPath, clusterAppPath, bakPath)
        g_logger.debug("Grey upgrade sync command: %s" % cmd)
        DefaultValue.execCommandLocally(cmd)

        # backup obsserver.key.cipher/obsserver.key.rand and server.key.
        # cipher/server.key.rand and datasource.key.cipher/datasource.key.rand
        OBS_cipher_key_bak_file = \
            "%s/bin/obsserver.key.cipher" % clusterAppPath
        cmd = "(if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
            OBS_cipher_key_bak_file, OBS_cipher_key_bak_file, bakPath)
        OBS_rand_key_bak_file = "%s/bin/obsserver.key.rand" % clusterAppPath
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
            OBS_rand_key_bak_file, OBS_rand_key_bak_file, bakPath)
        trans_encrypt_cipher_key_bak_file = \
            "%s/bin/trans_encrypt.key.cipher" % clusterAppPath
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
            trans_encrypt_cipher_key_bak_file,
            trans_encrypt_cipher_key_bak_file,
            bakPath)
        trans_encrypt_rand_key_bak_file = \
            "%s/bin/trans_encrypt.key.rand" % clusterAppPath
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
            trans_encrypt_rand_key_bak_file, trans_encrypt_rand_key_bak_file,
            bakPath)
        trans_encrypt_cipher_ak_sk_key_bak_file = \
            "%s/bin/trans_encrypt_ak_sk.key" % clusterAppPath
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
            trans_encrypt_cipher_ak_sk_key_bak_file,
            trans_encrypt_cipher_ak_sk_key_bak_file, bakPath)
        server_cipher_key_bak_file = \
            "%s/bin/server.key.cipher" % clusterAppPath
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
            server_cipher_key_bak_file, server_cipher_key_bak_file, bakPath)
        server_rand_key_bak_file = "%s/bin/server.key.rand" % clusterAppPath
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
            server_rand_key_bak_file, server_rand_key_bak_file, bakPath)
        datasource_cipher = "%s/bin/datasource.key.cipher" % clusterAppPath
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
            datasource_cipher, datasource_cipher, bakPath)
        datasource_rand = "%s/bin/datasource.key.rand" % clusterAppPath
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
            datasource_rand, datasource_rand, bakPath)
        tde_key_cipher = "%s/bin/gs_tde_keys.cipher" % clusterAppPath
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
            tde_key_cipher, tde_key_cipher, bakPath)
        g_logger.debug("Backup command: %s" % cmd)
        DefaultValue.execCommandLocally(cmd)

        # backup utilslib
        utilslib = "%s/utilslib" % clusterAppPath
        cmd = "if [ -d '%s' ];then cp -r '%s' '%s';fi" % (
            utilslib, utilslib, bakPath)
        g_logger.debug("Backup command: %s" % cmd)
        DefaultValue.execCommandLocally(cmd)

        # backup ca.key,etcdca.crt, client.key and client.crt
        CA_key_file = "%s/share/sslcert/etcd/ca.key" % clusterAppPath
        cmd = "(if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
            CA_key_file, CA_key_file, bakPath)
        CA_cert_file = "%s/share/sslcert/etcd/etcdca.crt" % clusterAppPath
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
            CA_cert_file, CA_cert_file, bakPath)
        client_key_file = "%s/share/sslcert/etcd/client.key" % clusterAppPath
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
            client_key_file, client_key_file, bakPath)
        client_cert_file = "%s/share/sslcert/etcd/client.crt" % clusterAppPath
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
            client_cert_file, client_cert_file, bakPath)
        if int(g_opts.oldVersion) >= 92019:
            client_key_cipher_file = \
                "%s/share/sslcert/etcd/client.key.cipher" % clusterAppPath
            cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
                client_key_cipher_file, client_key_cipher_file, bakPath)
            client_key_rand_file = \
                "%s/share/sslcert/etcd/client.key.rand" % clusterAppPath
            cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
                client_key_rand_file, client_key_rand_file, bakPath)
            etcd_key_cipher_file = \
                "%s/share/sslcert/etcd/etcd.key.cipher" % clusterAppPath
            cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
                etcd_key_cipher_file, etcd_key_cipher_file, bakPath)
            etcd_key_rand_file = \
                "%s/share/sslcert/etcd/etcd.key.rand" % clusterAppPath
            cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
                etcd_key_rand_file, etcd_key_rand_file, bakPath)
        g_logger.debug("Backup command: %s" % cmd)
        DefaultValue.execCommandLocally(cmd)

        # backup java UDF
        javadir = "'%s'/lib/postgresql/java" % clusterAppPath
        cmd = "if [ -d '%s' ];then cp -r '%s' '%s';fi" % (
            javadir, javadir, bakPath)
        g_logger.debug("Backup command: %s" % cmd)
        DefaultValue.execCommandLocally(cmd)

        # backup postGIS
        cmdPostGis = ""
        for sofile in g_opts.postgisSOFileList.keys():
            absPath = os.path.join(clusterAppPath,
                                   g_opts.postgisSOFileList[sofile])
            srcFile = "'%s'/%s" % (absPath, sofile)
            cmdPostGis += " && (if [ -f %s ];then cp -f -p %s '%s';fi)" % (
                srcFile, srcFile, bakPath)
        # skip " &&"
        cmd = cmdPostGis[3:]
        g_logger.debug("Backup command: %s" % cmd)
        DefaultValue.execCommandLocally(cmd)

        # backup extension library and config files
        hadoop_odbc_connector = \
            "%s/lib/postgresql/hadoop_odbc_connector.so" % clusterAppPath
        extension_config01 = \
            "%s/share/postgresql/extension/hadoop_odbc_connector--1.0.sql" \
            % clusterAppPath
        extension_config02 = \
            "%s/share/postgresql/extension/hadoop_odbc_connector.control" \
            % clusterAppPath
        extension_config03 = \
            "%s/share/postgresql/extension/" \
            "hadoop_odbc_connector--unpackaged--1.0.sql" % clusterAppPath
        cmd = "(if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
            hadoop_odbc_connector, hadoop_odbc_connector, bakPath)
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
            extension_config01, extension_config01, bakPath)
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
            extension_config02, extension_config02, bakPath)
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s';fi)" % (
            extension_config03, extension_config03, bakPath)
        g_logger.debug("Backup command: %s" % cmd)
        DefaultValue.execCommandLocally(cmd)

        # backup dict file and grpc files
        dictFileDir = "'%s'/share/postgresql/tsearch_data" % clusterAppPath
        grpcFileDir = "'%s'/share/sslcert/grpc" % clusterAppPath
        cmd = "if [ -d '%s' ];then cp -r '%s' '%s';fi && " % (dictFileDir,
                                                              dictFileDir,
                                                              bakPath)
        cmd += "if [ -d '%s' ];then cp -r '%s' '%s';fi" % (grpcFileDir,
                                                           grpcFileDir,
                                                           bakPath)
        g_logger.debug("Backup command: %s" % cmd)
        DefaultValue.execCommandLocally(cmd)

        # backup gtm.control and gtm.sequence
        if len(g_dbNode.gtms) > 0:
            gtm_control = "%s/gtm.control" % g_dbNode.gtms[0].datadir
            gtm_sequence = "%s/gtm.sequence" % g_dbNode.gtms[0].datadir
            cmd = "(if [ -f '%s' ];" \
                  "then cp -f -p '%s' '%s/gtm.control.bak';fi)" % \
                  (gtm_control, gtm_control, bakPath)
            cmd += " && (if [ -f '%s' ];" \
                   "then cp -f -p '%s' '%s/gtm.sequence.bak';fi)" % \
                   (gtm_sequence, gtm_sequence, bakPath)
            g_logger.debug("Backup command: %s" % cmd)
            DefaultValue.execCommandLocally(cmd)
    except Exception as e:
        raise Exception(str(e))


def restoreConfig():
    """
    function: restore config
    output: none
    """
    try:
        bakPath = g_opts.upgrade_bak_path
        clusterAppPath = g_opts.newClusterAppPath
        # restore static configuration
        cmd = "cp -f -p '%s'/*cluster_static_config* '%s'/bin/" % (
            bakPath, clusterAppPath)
        # restore dynamic configuration
        dynamic_config = "%s/cluster_dynamic_config" % bakPath
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s/bin/';fi)" % (
            dynamic_config, dynamic_config, clusterAppPath)
        # no need to restore alarm.conf at here,
        # because it has been done on upgradeNodeApp
        g_logger.debug("Restore command: %s" % cmd)
        DefaultValue.execCommandLocally(cmd)

        # restore libsimsearch etc files and libsimsearch libs files
        searchConfigFile = "%s/searchletConfig.yaml" % bakPath
        cmd = "(if [ -f '%s' ];" \
              "then cp -f -p '%s' '%s/etc/searchletConfig.yaml'; fi)" % (
                  searchConfigFile, searchConfigFile, clusterAppPath)
        searchIniFile = "%s/searchServer.ini" % bakPath
        cmd += " && (if [ -f '%s' ];" \
               "then cp -f -p '%s' '%s/etc/searchServer.ini'; fi)" % (
                   searchIniFile, searchIniFile, clusterAppPath)
        cmd += " && (if [ -d '%s/libsimsearch' ];" \
               "then cp -r '%s/libsimsearch' '%s/lib/';fi)" % (
                   bakPath, bakPath, clusterAppPath)
        g_logger.debug("Restore command: %s" % cmd)
        DefaultValue.execCommandLocally(cmd)

        # restore library file,
        # database size file and initialized configuration parameters files
        cmd = "cp -r '%s/pg_plugin' '%s'/lib/postgresql" % (
            bakPath, clusterAppPath)
        backup_dbsize = os.path.join(bakPath, DefaultValue.DB_SIZE_FILE)
        cmd += " && (if [ -f '%s' ];then cp '%s' '%s/bin';fi)" % (
            backup_dbsize, backup_dbsize, clusterAppPath)
        g_logger.debug("Restore command: %s" % cmd)
        DefaultValue.execCommandLocally(cmd)

        # sync kerberos conf files
        cmd = "(if [ -d '%s/kerberos' ];then cp -r '%s/kerberos' '%s/';fi)" % (
            bakPath, bakPath, clusterAppPath)
        cmd += "&& (if [ -d '%s/var/krb5kdc' ];" \
               "then mkdir %s/var; cp -r '%s/var/krb5kdc' '%s/var/';fi)" % (
                   bakPath, clusterAppPath, bakPath, clusterAppPath)
        g_logger.debug("Restore command: %s" % cmd)
        DefaultValue.execCommandLocally(cmd)

        # restore obsserver.key.cipher/obsserver.key.rand
        # and server.key.cipher/server.key.rand
        # and datasource.key.cipher/datasource.key.rand
        OBS_cipher_key_bak_file = "%s/obsserver.key.cipher" % bakPath
        cmd = "(if [ -f '%s' ];then cp -f -p '%s' '%s/bin/';fi)" % (
            OBS_cipher_key_bak_file, OBS_cipher_key_bak_file, clusterAppPath)
        OBS_rand_key_bak_file = "%s/obsserver.key.rand" % bakPath
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s/bin/';fi)" % (
            OBS_rand_key_bak_file, OBS_rand_key_bak_file, clusterAppPath)
        trans_encrypt_cipher_key_bak_file = \
            "%s/trans_encrypt.key.cipher" % bakPath
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s/bin/';fi)" % (
            trans_encrypt_cipher_key_bak_file,
            trans_encrypt_cipher_key_bak_file,
            clusterAppPath)
        trans_encrypt_rand_key_bak_file = "%s/trans_encrypt.key.rand" % bakPath
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s/bin/';fi)" % (
            trans_encrypt_rand_key_bak_file, trans_encrypt_rand_key_bak_file,
            clusterAppPath)
        trans_encrypt_cipher_ak_sk_key_bak_file = \
            "%s/trans_encrypt_ak_sk.key" % bakPath
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s/bin/';fi)" % (
            trans_encrypt_cipher_ak_sk_key_bak_file,
            trans_encrypt_cipher_ak_sk_key_bak_file, clusterAppPath)
        server_cipher_key_bak_file = "%s/server.key.cipher" % bakPath
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s/bin/';fi)" % (
            server_cipher_key_bak_file, server_cipher_key_bak_file,
            clusterAppPath)
        server_rand_key_bak_file = "%s/server.key.rand" % bakPath
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s/bin/';fi)" % (
            server_rand_key_bak_file, server_rand_key_bak_file, clusterAppPath)
        datasource_cipher = "%s/datasource.key.cipher" % bakPath
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s/bin/';fi)" % (
            datasource_cipher, datasource_cipher, clusterAppPath)
        datasource_rand = "%s/datasource.key.rand" % bakPath
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s/bin/';fi)" % (
            datasource_rand, datasource_rand, clusterAppPath)
        tde_key_cipher = "%s/gs_tde_keys.cipher" % bakPath
        cmd += " && (if [ -f '%s' ];then cp -f -p '%s' '%s/bin/';fi)" % (
            tde_key_cipher, tde_key_cipher, clusterAppPath)
        g_logger.debug("Restore command: %s" % cmd)
        DefaultValue.execCommandLocally(cmd)

        # restore utilslib
        utilslib = "%s/utilslib" % bakPath
        cmd = "if [ -d '%s' ];then cp -r '%s' '%s'/;" % (
            utilslib, utilslib, clusterAppPath)
        # create new $GAUSSHOME/utilslib if not exist.
        # no need to do chown, it will be done at all restore finished
        cmd += " else mkdir -p '%s'/utilslib -m %s; fi " % (
            clusterAppPath, DefaultValue.DIRECTORY_MODE)
        g_logger.debug("Restore command: %s" % cmd)
        DefaultValue.execCommandLocally(cmd)

        # restore ca.key,etcdca.crt, client.key and client.crt
        CA_key_file = "%s/ca.key" % bakPath
        cmd = "(if [ -f '%s' ];" \
              "then cp -f -p '%s' '%s/share/sslcert/etcd/';fi)" % (
                  CA_key_file, CA_key_file, clusterAppPath)
        CA_cert_file = "%s/etcdca.crt" % bakPath
        cmd += " && (if [ -f '%s' ];" \
               "then cp -f -p '%s' '%s/share/sslcert/etcd/';fi)" % (
                   CA_cert_file, CA_cert_file, clusterAppPath)
        client_key_file = "%s/client.key" % bakPath
        cmd += " && (if [ -f '%s' ];" \
               "then cp -f -p '%s' '%s/share/sslcert/etcd/';fi)" % (
                   client_key_file, client_key_file, clusterAppPath)
        client_cert_file = "%s/client.crt" % bakPath
        cmd += " && (if [ -f '%s' ];" \
               "then cp -f -p '%s' '%s/share/sslcert/etcd/';fi)" % (
                   client_cert_file, client_cert_file, clusterAppPath)
        if int(g_opts.oldVersion) >= 92019:
            client_key_cipher_file = "%s/client.key.cipher" % bakPath
            cmd += " && (if [ -f '%s' ];" \
                   "then cp -f -p '%s' '%s/share/sslcert/etcd/';fi)" % (
                       client_key_cipher_file, client_key_cipher_file,
                       clusterAppPath)
            client_key_rand_file = "%s/client.key.rand" % bakPath
            cmd += " && (if [ -f '%s' ];" \
                   "then cp -f -p '%s' '%s/share/sslcert/etcd/';fi)" % (
                       client_key_rand_file, client_key_rand_file,
                       clusterAppPath)
            etcd_key_cipher_file = "%s/etcd.key.cipher" % bakPath
            cmd += " && (if [ -f '%s' ];" \
                   "then cp -f -p '%s' '%s/share/sslcert/etcd/';fi)" % (
                       etcd_key_cipher_file, etcd_key_cipher_file,
                       clusterAppPath)
            etcd_key_rand_file = "%s/etcd.key.rand" % bakPath
            cmd += " && (if [ -f '%s' ];" \
                   "then cp -f -p '%s' '%s/share/sslcert/etcd/';fi)" % (
                       etcd_key_rand_file, etcd_key_rand_file, clusterAppPath)
        g_logger.debug("Restore command: %s" % cmd)
        DefaultValue.execCommandLocally(cmd)

        # restore javaUDF
        # lib/postgresql/java/pljava.jar use new package, no need to restore.
        javadir = "%s/java" % bakPath
        desPath = "%s/lib/postgresql/" % clusterAppPath
        cmd = "if [ -d '%s' ];" \
              "then rm -f '%s/pljava.jar'&&cp -r '%s' '%s' ;fi" % (
                  javadir, javadir, javadir, desPath)
        g_logger.debug("Restore command: %s" % cmd)
        DefaultValue.execCommandLocally(cmd)

        # restore postGIS
        cmdPostGis = ""
        machineType = platform.machine()
        for sofile in g_opts.postgisSOFileList.keys():
            # To solve the dependency problem on the ARM platform,
            # the dependency library libbgcc_s.so* and libstdc++.
            # so.* is contained in the ARM package.
            # The libgcc_s.so.*
            # on the ARM platform is the database built-in library.
            # Therefore, no restoration is required.
            if machineType == "aarch64" and sofile.find('libgcc_s.so') >= 0:
                continue
            desPath = os.path.join(clusterAppPath,
                                   g_opts.postgisSOFileList[sofile])
            srcFile = "'%s'/%s" % (bakPath, sofile)
            cmdPostGis += " && (if [ -f %s ];then cp -f -p %s '%s';fi)" % (
                srcFile, srcFile, desPath)
        # skip " &&"
        cmd = cmdPostGis[3:]
        g_logger.debug("Restore command: %s" % cmd)
        DefaultValue.execCommandLocally(cmd)

        # restore extension library and config files
        hadoop_odbc_connector = \
            "%s/lib/postgresql/hadoop_odbc_connector.so" % bakPath
        extension_config01 = \
            "%s/share/postgresql/extension/hadoop_odbc_connector--1.0.sql" \
            % bakPath
        extension_config02 = \
            "%s/share/postgresql/extension/hadoop_odbc_connector.control" \
            % bakPath
        extension_config03 = \
            "%s/share/postgresql/extension/" \
            "hadoop_odbc_connector--unpackaged--1.0.sql" % bakPath
        cmd = "(if [ -f '%s' ];then cp -f -p '%s' '%s/lib/postgresql/';fi)" % (
            hadoop_odbc_connector, hadoop_odbc_connector, clusterAppPath)
        cmd += \
            " && (if [ -f '%s' ];then cp -f " \
            "-p '%s/share/postgresql/extension/' '%s';fi)" % (
                extension_config01, extension_config01, clusterAppPath)
        cmd += \
            " && (if [ -f '%s' ];then cp " \
            "-f -p '%s/share/postgresql/extension/' '%s';fi)" % (
                extension_config02, extension_config02, clusterAppPath)
        cmd += \
            " && (if [ -f '%s' ];then cp -f " \
            "-p '%s/share/postgresql/extension/' '%s';fi)" % (
                extension_config03, extension_config03, clusterAppPath)
        g_logger.debug("Restore command: %s" % cmd)
        DefaultValue.execCommandLocally(cmd)

        # restore dict file and grpc file
        dictFileDir = "'%s'/tsearch_data" % bakPath
        dictDesPath = "'%s'/share/postgresql" % clusterAppPath
        grpcFileDir = "'%s'/grpc" % bakPath
        grpcDesPath = "'%s'/share/sslcert" % clusterAppPath
        cmd = "if [ -d '%s' ];then cp -r '%s' '%s/' ;fi &&" % (
            dictFileDir, dictFileDir, dictDesPath)
        cmd += "if [ -d '%s' ];then cp -r '%s' '%s/' ;fi" % (
            grpcFileDir, grpcFileDir, grpcDesPath)
        g_logger.debug("Restore command: %s" % cmd)
        DefaultValue.execCommandLocally(cmd)
    except Exception as e:
        raise Exception(str(e))


def inplaceBackup():
    """
    function: backup config
    output: none
    """
    try:
        # backup gds files
        bakPath = g_opts.upgrade_bak_path
        gdspath = "%s/share/sslcert/gds" % g_clusterInfo.appPath
        cmd = "(if [ -d '%s' ];" \
              "then chmod 600 -R '%s'/*; cp -r '%s' '%s';fi)" % (
                  gdspath, gdspath, gdspath, bakPath)
        g_logger.debug("Inplace backup command: %s" % cmd)
        DefaultValue.execCommandLocally(cmd)
    except Exception as e:
        raise Exception(str(e))


def inplaceRestore():
    """
    function: restore config
    output: none
    """
    try:
        # restore gds files
        gdspath = "%s/share/sslcert/" % g_clusterInfo.appPath
        gdsbackup = "%s/gds" % g_opts.upgrade_bak_path
        cmd = "(if [ -d '%s' ];then cp -r '%s' '%s';fi)" % (
            gdsbackup, gdsbackup, gdspath)
        g_logger.debug("Inplace restore command: %s" % cmd)
        DefaultValue.execCommandLocally(cmd)
    except Exception as e:
        raise Exception(str(e))


def checkGucValue():
    """
    function: check guc value
    input  : NA
    output : NA
    """
    key = g_opts.gucStr.split(':')[0].strip()
    value = g_opts.gucStr.split(':')[1].strip()
    if key == "upgrade_from":
        instances = g_dbNode.cmagents
        fileName = "cm_agent.conf"
    elif key == "upgrade_mode":
        instances = g_dbNode.coordinators
        instances.extend(g_dbNode.datanodes)
        fileName = "postgresql.conf"
    else:
        raise Exception(ErrorCode.GAUSS_529["GAUSS_52942"])
    for inst in instances:
        configFile = "%s/%s" % (inst.datadir, fileName)
        cmd = "sed 's/\t/ /g' %s " \
              "| grep '^[ ]*\<%s\>[ ]*=' " \
              "| awk -F '=' '{print $2}'" % (configFile, key)
        g_logger.debug("Command for checking guc:%s" % cmd)
        retryTimes = 100
        for i in range(retryTimes):
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                time.sleep(3)
                g_logger.debug(
                    ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                    + " Output: \n%s" % output)
                continue
            if output == "":
                time.sleep(3)
                g_logger.debug(
                    ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                    + " There is no %s in %s" % (key, configFile))
                continue
            realValue = output.split('\n')[0].strip()
            if '#' in realValue:
                realValue = realValue.split('#')[0].strip()
            g_logger.debug("[key:%s]: Realvalue %s, ExpectValue %s" % (
                key, str(realValue), str(value)))
            if str(value) != str(realValue):
                raise Exception(
                    ErrorCode.GAUSS_521["GAUSS_52102"] % key
                    + " Real value %s, expect value %s"
                    % (str(realValue), str(value)))
            break


def backupInstanceHotpatchConfig(instanceDataDir):
    """
    function: backup
    input  : instanceDataDir
    output : NA
    """
    hotpatch_info_file = "%s/hotpatch/patch.info" % instanceDataDir
    hotpatch_info_file_bak = "%s/hotpatch/patch.info.bak" % instanceDataDir
    cmd = "(if [ -f '%s' ];then mv -f '%s' '%s';fi)" % (
        hotpatch_info_file, hotpatch_info_file, hotpatch_info_file_bak)
    (status, output) = subprocess.getstatusoutput(cmd)
    if status != 0:
        raise Exception(
            ErrorCode.GAUSS_514["GAUSS_51400"] % cmd + "\nOutput:%s" % output)


def backupHotpatch():
    """
    function: if the upgrade process failed in check cluster status,
        user can reenter upgrade process
    """
    if os.path.samefile(g_gausshome, g_opts.newClusterAppPath):
        g_logger.debug("Has switched to new version, no need to backup again.")
        return

    for dbInstance in g_dbNode.cmservers:
        backupInstanceHotpatchConfig(dbInstance.datadir)

    for dbInstance in g_dbNode.coordinators:
        backupInstanceHotpatchConfig(dbInstance.datadir)

    for dbInstance in g_dbNode.datanodes:
        backupInstanceHotpatchConfig(dbInstance.datadir)

    for dbInstance in g_dbNode.gtms:
        backupInstanceHotpatchConfig(dbInstance.datadir)


def rollbackInstanceHotpatchConfig(instanceDataDir):
    """
    function: rollback
    input  : instanceDataDir
    output : NA
    """
    hotpatch_info_file = "%s/hotpatch/patch.info" % instanceDataDir
    hotpatch_info_file_bak = "%s/hotpatch/patch.info.bak" % instanceDataDir
    cmd = "(if [ -f '%s' ];then mv -f '%s' '%s';fi)" % (
        hotpatch_info_file_bak, hotpatch_info_file_bak, hotpatch_info_file)
    (status, output) = subprocess.getstatusoutput(cmd)
    if status != 0:
        raise Exception(
            ErrorCode.GAUSS_514["GAUSS_51400"] % cmd + "\nOutput:%s" % output)


def rollbackHotpatch():
    """
    function: rollback
    input  : NA
    output : NA
    """
    for dbInstance in g_dbNode.cmservers:
        rollbackInstanceHotpatchConfig(dbInstance.datadir)

    for dbInstance in g_dbNode.coordinators:
        rollbackInstanceHotpatchConfig(dbInstance.datadir)

    for dbInstance in g_dbNode.datanodes:
        rollbackInstanceHotpatchConfig(dbInstance.datadir)

    for dbInstance in g_dbNode.gtms:
        rollbackInstanceHotpatchConfig(dbInstance.datadir)


def readDeleteGuc():
    """
     function: get the delete guc from file,
     input:  NA
     output: return the dict gucContent[instanceName]: guc_name
        :return:the key instancename is gtm, coordinator,
        datanode, cmserver, cmagent
    """
    deleteGucFile = os.path.join(g_opts.upgrade_bak_path,
                                 "upgrade_sql/set_guc/delete_guc")
    if not os.path.isfile(deleteGucFile):
        raise Exception(ErrorCode.GAUSS_502["GAUSS_50210"] % deleteGucFile)
    g_logger.debug("Get the delete GUC from file %s." % deleteGucFile)
    gucContent = {}
    with open(deleteGucFile, 'r') as fp:
        resList = fp.readlines()
    for oneLine in resList:
        oneLine = oneLine.strip()
        # skip blank line and comment line
        if not oneLine or oneLine.startswith('#'):
            continue
        result = oneLine.split()
        if len(result) != 2:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50222"] % deleteGucFile)
        gucName = result[0]
        instanceName = result[1]
        gucContent.setdefault(instanceName, []).append(gucName)
    g_logger.debug("Successfully get the delete GUC from file.")
    return gucContent


def cleanInstallPath():
    """
    function: clean install path
    input  : NA
    output : NA
    """
    installPath = g_opts.appPath
    if not os.path.exists(installPath):
        g_logger.debug(ErrorCode.GAUSS_502[
                           "GAUSS_50201"] % installPath + " No need to clean.")
        return
    if not os.listdir(installPath):
        g_logger.debug("The path %s is empty." % installPath)
        cmd = "(if [ -d '%s' ]; then rm -rf '%s'; fi)" % (
            installPath, installPath)
        g_logger.log("Command for cleaning install path: %s" % cmd)
        DefaultValue.execCommandLocally(cmd)
        return
    if g_opts.forceRollback and not os.path.islink(g_gausshome):
        g_logger.log(
            "Under force rollback mode, "
            "$GAUSSHOME is not symbolic link. No need to clean.")
        return
    elif os.path.samefile(installPath, g_gausshome):
        g_logger.log("The install path is $GAUSSHOME, cannot clean.")
        return
    tmpDir = DefaultValue.getTmpDirFromEnv(g_opts.user)
    if tmpDir == "":
        raise Exception(ErrorCode.GAUSS_518["GAUSS_51800"] % "$PGHOST")
    # under upgrade, we will change the mode to read and execute
    # in order to not change the dir, so we need to restore
    # the permission to original mode after we switch to new version,
    # and then we will have the permission to clean
    # appPath under commit-upgrade
    # under rollback, we also need to restore the permission
    pluginPath = "%s/lib/postgresql/pg_plugin" % installPath
    cmd = "(if [ -d '%s' ]; then chmod -R %d '%s'; fi)" % (
        pluginPath, DefaultValue.KEY_DIRECTORY_MODE, pluginPath)
    appBakPath = "%s/to_be_delete" % tmpDir
    cmd += " && (if [ ! -d '%s' ]; then mkdir -p '%s'; fi)" % (
        appBakPath, appBakPath)
    cmd += " && (if [ -d '%s' ]; then cp -r '%s/' '%s/to_be_delete/'; fi)" % (
        installPath, installPath, tmpDir)
    g_logger.debug(
        "Command for change permission and backup install path: %s" % cmd)
    DefaultValue.execCommandLocally(cmd)

    cmd = "(if [ -d '%s/bin' ]; then rm -rf '%s/bin'; fi) &&" % \
          (installPath, installPath)
    cmd += "(if [ -d '%s/etc' ]; then rm -rf '%s/etc'; fi) &&" % \
           (installPath, installPath)
    cmd += "(if [ -d '%s/include' ]; then rm -rf '%s/include'; fi) &&" % \
           (installPath, installPath)
    cmd += "(if [ -d '%s/lib' ]; then rm -rf '%s/lib'; fi) &&" % \
           (installPath, installPath)
    cmd += "(if [ -d '%s/share' ]; then rm -rf '%s/share'; fi) &&" % \
           (installPath, installPath)
    cmd += "(if [ -d '%s/logs' ]; then rm -rf '%s/logs'; fi) &&" % \
           (installPath, installPath)
    cmd += "(if [ -d '%s/utilslib' ]; then rm -rf '%s/utilslib'; fi) && " % \
           (installPath, installPath)
    cmd += "(if [ -d '%s/jdk' ]; then rm -rf '%s/jdk'; fi) && " % \
           (installPath, installPath)
    cmd += "(if [ -d '%s/kerberos' ]; then rm -rf '%s/kerberos'; fi) &&" % \
           (installPath, installPath)
    cmd += "(if [ -d '%s/var/krb5kdc' ]; then rm -rf '%s/var/krb5kdc'; fi)" % \
           (installPath, installPath)
    DefaultValue.execCommandLocally(cmd)
    if os.listdir(installPath):
        g_logger.log(
            "The path %s has personal file ot directory, please remove it."
            % installPath)
    else:
        cmd = "(if [ -d '%s' ]; then rm -rf '%s'; fi)" % (
            installPath, installPath)
        g_logger.log("Command for cleaning install path: %s" % cmd)
        DefaultValue.execCommandLocally(cmd)


def copyCerts():
    """
    function: copy certs
    input  : NA
    output : NA
    """
    g_logger.debug("Starting copy Certs")
    oldBinPath = os.path.join(g_opts.oldClusterAppPath, "bin")
    newBinPath = os.path.join(g_opts.newClusterAppPath, "bin")
    oldOmSslCerts = os.path.join(g_opts.oldClusterAppPath, "share/sslcert/om")
    newOmSslCerts = os.path.join(g_opts.newClusterAppPath, "share/sslcert/om")

    g_file.cpFile("%s/server.key.cipher" % oldBinPath, "%s/" % newBinPath)
    g_file.cpFile("%s/server.key.rand" % oldBinPath, "%s/" % newBinPath)
    for certFile in DefaultValue.SERVER_CERT_LIST:
        g_file.cpFile("%s/%s" % (oldOmSslCerts, certFile), "%s/" %
                      newOmSslCerts)

    g_file.changeMode(DefaultValue.KEY_FILE_MODE, "%s/server.key.cipher" %
                      newBinPath)
    g_file.changeMode(DefaultValue.KEY_FILE_MODE, "%s/server.key.rand" %
                      newBinPath)
    g_file.changeMode(DefaultValue.KEY_FILE_MODE, "%s/*" %
                      newOmSslCerts)


def checkAction():
    """
    function: check action
    input  : NA
    output : NA
    """
    if g_opts.action not in \
            [const.ACTION_TOUCH_INIT_FILE, const.ACTION_SYNC_CONFIG,
             const.ACTION_BACKUP_CONFIG,
             const.ACTION_RESTORE_CONFIG,
             const.ACTION_INPLACE_BACKUP,
             const.ACTION_INPLACE_RESTORE,
             const.ACTION_CHECK_GUC,
             const.ACTION_BACKUP_HOTPATCH,
             const.ACTION_ROLLBACK_HOTPATCH,
             const.ACTION_SWITCH_PROCESS,
             const.ACTION_SWITCH_BIN,
             const.ACTION_CLEAN_INSTALL_PATH,
             const.ACTION_COPY_CERTS]:
        GaussLog.exitWithError(
            ErrorCode.GAUSS_500["GAUSS_50004"] % 't'
            + " Value: %s" % g_opts.action)


def main():
    """
    function: main function
    """
    try:
        global g_opts
        g_opts = CmdOptions()
        parseCommandLine()
        checkParameter()
        initGlobals()
    except Exception as e:
        GaussLog.exitWithError(str(e) + traceback.format_exc())
    try:
        # select the object's function by type
        funcs = {
            const.ACTION_SWITCH_BIN: switchBin,
            const.ACTION_CLEAN_INSTALL_PATH: cleanInstallPath,
            const.ACTION_TOUCH_INIT_FILE: touchInstanceInitFile,
            const.ACTION_SYNC_CONFIG: syncClusterConfig,
            const.ACTION_BACKUP_CONFIG: backupConfig,
            const.ACTION_RESTORE_CONFIG: restoreConfig,
            const.ACTION_INPLACE_BACKUP: inplaceBackup,
            const.ACTION_INPLACE_RESTORE: inplaceRestore,
            const.ACTION_CHECK_GUC: checkGucValue,
            const.ACTION_BACKUP_HOTPATCH: backupHotpatch,
            const.ACTION_ROLLBACK_HOTPATCH: rollbackHotpatch,
            const.ACTION_COPY_CERTS: copyCerts}
        func = funcs[g_opts.action]
        func()
    except Exception as e:
        checkAction()
        g_logger.debug(traceback.format_exc())
        g_logger.logExit(str(e))

if __name__ == '__main__':
    main()
