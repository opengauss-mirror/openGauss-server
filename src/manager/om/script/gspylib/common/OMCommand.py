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
# Description  : OMCommand.py is utility to execute the OM command
#############################################################################
import os
import sys
import time
import re
import subprocess
from multiprocessing.dummy import Pool as ThreadPool

sys.path.append(sys.path[0] + "/../../")
from gspylib.common.DbClusterInfo import dbClusterInfo
from gspylib.common.Common import DefaultValue, ClusterCommand, \
    TempfileManagement
from gspylib.common.DbClusterStatus import DbClusterStatus
from gspylib.common.ErrorCode import ErrorCode
from gspylib.os.gsplatform import g_Platform


class OMCommand():
    """
    Descript command of om
    """

    def __init__(self):
        '''  
        Constructor
        '''

    @staticmethod
    def getLocalScript(script):
        """
        function: get local script by GPHOME
        input : script, path
        output: path
        """
        Current_Path = os.path.dirname(os.path.realpath(__file__))

        if os.getgid() != 0:
            gp_home = DefaultValue.getEnv("GPHOME")
            Current_Path = os.path.join(gp_home, "script/gspylib/common")

        LocalScript = {
            "Local_Backup": os.path.normpath(
                Current_Path + "/../../local/Backup.py"),
            "Local_Check_Config": os.path.normpath(
                Current_Path + "/../../local/CheckConfig.py"),
            "Local_Check_Install": os.path.normpath(
                Current_Path + "/../../local/CheckInstall.py"),
            "Local_Check_Uninstall": os.path.normpath(
                Current_Path + "/../../local/CheckUninstall.py"),
            "Local_Clean_Instance": os.path.normpath(
                Current_Path + "/../../local/CleanInstance.py"),
            "Local_Clean_OsUser": os.path.normpath(
                Current_Path + "/../../local/CleanOsUser.py"),
            "Local_Config_Hba": os.path.normpath(
                Current_Path + "/../../local/ConfigHba.py"),
            "Local_Config_Instance": os.path.normpath(
                Current_Path + "/../../local/ConfigInstance.py"),
            "Local_Init_Instance": os.path.normpath(
                Current_Path + "/../../local/InitInstance.py"),
            "Local_Install": os.path.normpath(
                Current_Path + "/../../local/Install.py"),
            "Local_Restore": os.path.normpath(
                Current_Path + "/../../local/Restore.py"),
            "Local_Uninstall": os.path.normpath(
                Current_Path + "/../../local/Uninstall.py"),
            "Local_PreInstall": os.path.normpath(
                Current_Path + "/../../local/PreInstallUtility.py"),
            "Local_Check_PreInstall": os.path.normpath(
                Current_Path + "/../../local/CheckPreInstall.py"),
            "Local_UnPreInstall": os.path.normpath(
                Current_Path + "/../../local/UnPreInstallUtility.py"),
            "Local_Roach": os.path.normpath(
                Current_Path + "/../../local/LocalRoach.py"),
            "Gauss_UnInstall": os.path.normpath(
                Current_Path + "/../../gs_uninstall"),
            "Gauss_Backup": os.path.normpath(
                Current_Path + "/../../gs_backup"),
            "Local_CheckOS": os.path.normpath(
                Current_Path + "/../../local/LocalCheckOS.py"),
            "Local_Check": os.path.normpath(
                Current_Path + "/../../local/LocalCheck.py"),
            "LOCAL_PERFORMANCE_CHECK": os.path.normpath(
                Current_Path + "/../../local/LocalPerformanceCheck.py"),
            "Gauss_CheckOS": os.path.normpath(
                Current_Path + "/../../gs_checkos"),
            "Gauss_PreInstall": os.path.normpath(
                Current_Path + "/../../gs_preinstall"),
            "Gauss_Replace": os.path.normpath(
                Current_Path + "/../../gs_replace"),
            "Gauss_Om": os.path.normpath(Current_Path + "/../../gs_om"),
            "UTIL_GAUSS_STAT": os.path.normpath(
                Current_Path + "/../../gspylib/common/GaussStat.py"),
            "Gauss_Check": os.path.normpath(Current_Path + "/../../gs_check"),
            "Local_Collect": os.path.normpath(
                Current_Path + "/../../local/LocalCollect.py"),
            "Local_Kerberos": os.path.normpath(
                Current_Path + "/../../local/KerberosUtility.py"),
            "Local_Execute_Sql": os.path.normpath(
                Current_Path + "/../../local/ExecuteSql.py"),
            "Local_StartInstance": os.path.normpath(
                Current_Path + "/../../local/StartInstance.py"),
            "Local_StopInstance": os.path.normpath(
                Current_Path + "/../../local/StopInstance.py"),
            "Local_Check_Upgrade": os.path.normpath(
                Current_Path + "/../../local/CheckUpgrade.py"),
            "Local_Upgrade_Utility": os.path.normpath(
                Current_Path + "/../../local/UpgradeUtility.py")
        }

        return "python3 '%s'" % LocalScript[script]

    @staticmethod
    def getSetCronCmd(user, appPath):
        """
        function: Set the crontab
        input : user, appPath
        output: cmd
        """
        log_path = DefaultValue.getOMLogPath(DefaultValue.OM_MONITOR_DIR_FILE,
                                             "", appPath)
        cronFile = "%s/gauss_cron_%d" % (
        DefaultValue.getTmpDirFromEnv(), os.getpid())
        cmd = "crontab -l > %s;" % cronFile
        cmd += "sed -i '/\\/bin\\/om_monitor/d' %s; " % cronFile
        cmd += "echo \"*/1 * * * * source /etc/profile;(if [ -f ~/.profile " \
               "];then source ~/.profile;fi);source ~/.bashrc;nohup " \
               "%s/bin/om_monitor -L %s >>/dev/null 2>&1 &\" >> %s;" % (
        appPath, log_path, cronFile)
        cmd += "crontab -u %s %s;service cron restart;" % (user, cronFile)
        cmd += "rm -f %s" % cronFile

        return cmd

    @staticmethod
    def getRemoveCronCmd(user):
        """
        function: get remove crontab command
        input : user
        output: cmd
        """
        cmd = "crontab -u %s -r;service cron restart" % user

        return cmd

    @staticmethod
    def adaptArchiveCommand(localInstDataDir, similarInstDataDir):
        """
        function: Adapt guc parameter 'archive_command' for each new instance.
                  It will be invoked by GaussReplace.py and GaussDilatation.py
        input : localInstDataDir, similarInstDataDir
        output: NA
        """
        GUC_PARAM_PATTERN = "^\\s*archive_command.*=.*$"
        pattern = re.compile(GUC_PARAM_PATTERN)
        archiveParaLine = ""
        archiveDir = "%s/pg_xlog/archive" % localInstDataDir
        archiveCommand = ""
        try:
            configFile = os.path.join(localInstDataDir, "postgresql.conf")

            with open(configFile, 'r') as fp:
                resList = fp.readlines()
            lineNum = 0
            for oneLine in resList:
                lineNum += 1
                # skip blank line
                if (oneLine.strip() == ""):
                    continue
                # skip comment line
                if ((oneLine.strip()).startswith('#')):
                    continue
                # search valid line
                result = pattern.match(oneLine)
                if (result is not None):
                    # have adapt archive_command parameter
                    archiveParaLine = oneLine.replace(similarInstDataDir,
                                                      localInstDataDir)
                    archiveList = archiveParaLine.split('#')
                    if (len(archiveList) > 0):
                        archiveCommand = archiveList[0]
                    break

            if (archiveParaLine != ""):
                if (archiveParaLine.find("%f") < 0):
                    raise Exception(ErrorCode.GAUSS_500["GAUSS_50009"]
                                    + " The parameter archive command should "
                                      "be set with %%f : %s." % archiveCommand)

                if (archiveParaLine.find("%p") < 0):
                    raise Exception(ErrorCode.GAUSS_500["GAUSS_50009"]
                                    + " The parameter archive command should"
                                      " be set with %%p: %s." % archiveCommand)

                setCmd = "sed -i \"%dc%s\" %s" % (lineNum, archiveParaLine,
                                                  configFile)
                (status, output) = subprocess.getstatusoutput(setCmd)
                if (status != 0):
                    raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"]
                                    % setCmd + " Error: \n%s" % output)

                if (os.path.exists(archiveDir) and os.path.isdir(archiveDir)):
                    return

                mkDirCmd = "mkdir -p '%s' -m %s" % (
                archiveDir, DefaultValue.KEY_DIRECTORY_MODE)
                (status, output) = subprocess.getstatusoutput(mkDirCmd)
                if (status != 0):
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50208"]
                                    % archiveDir + " Error: \n%s." % output
                                    + "The cmd is %s" % mkDirCmd)
        except Exception as e:
            raise Exception(str(e))

    @staticmethod
    def getClusterStatus(user, isExpandScene=False):
        """
        function: get cluster status
        input : user
        output: clusterStatus
        """
        userAbsolutePath = g_Platform.getUserHomePath()
        statusFile = "%s/gauss_check_status_%d.dat" % (
        userAbsolutePath, os.getpid())
        TempfileManagement.removeTempFile(statusFile)
        cmd = ClusterCommand.getQueryStatusCmd(user, "", statusFile)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            TempfileManagement.removeTempFile(statusFile)
            return None
        clusterStatus = DbClusterStatus()
        clusterStatus.initFromFile(statusFile, isExpandScene)
        TempfileManagement.removeTempFile(statusFile)
        return clusterStatus

    @staticmethod
    def getClusterDbNodeInfo(clusterUser, xmlFile=""):
        """
        function: get cluster and database node info from static config file
        input : clusterUser, xmlFile
        output: NA
        """
        try:
            clusterInfo = dbClusterInfo()
            if (os.getuid() == 0):
                clusterInfo.initFromXml(xmlFile)
            else:
                clusterInfo.initFromStaticConfig(clusterUser)
            hostName = DefaultValue.GetHostIpOrName()
            dbNodeInfo = clusterInfo.getDbNodeByName(hostName)
            if (dbNodeInfo is None):
                raise Exception(ErrorCode.GAUSS_516["GAUSS_51619"] % hostName)
            return clusterInfo, dbNodeInfo
        except Exception as e:
            raise Exception(str(e))

    @staticmethod
    def checkHostname(nodename):
        """
        function: check host name
        input : NA
        output: NA
        """
        try:
            retry = 1
            cmd = "pssh -s -H %s hostname" % (nodename)
            while True:
                (status, output) = subprocess.getstatusoutput(cmd)
                if status == 0 and output.find("%s" % nodename) >= 0:
                    break
                if retry >= 3:
                    raise Exception(ErrorCode.GAUSS_512["GAUSS_51222"]
                                    + " Command: \"%s\". Error: \n%s"
                                    % (cmd, output))
                retry += 1
                time.sleep(1)

            hostnameCmd = "pssh -s -H %s 'cat /etc/hostname'" % (nodename)
            (status, output) = subprocess.getstatusoutput(hostnameCmd)
            if status == 0 and output.strip() == nodename:
                pass
            else:
                raise Exception(ErrorCode.GAUSS_512["GAUSS_51248"] % nodename
                                + " Command: \"%s\". Error: \n%s"
                                % (hostnameCmd, output))

        except Exception as e:
            raise Exception(str(e))

    @staticmethod
    def checkHostnameMapping(clusterInfo, logFile):
        """
        function: check host name mapping
        input: NA
        output: NA 
        """
        nodes = clusterInfo.getClusterNodeNames()
        if (len(nodes) > 0):
            try:
                pool = ThreadPool(DefaultValue.getCpuSet())
                results = pool.map(OMCommand.checkHostname, nodes)
                pool.close()
                pool.join()
            except Exception as e:
                raise Exception(str(e))
