# -*- coding:utf-8 -*-
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
import os
from gspylib.common.Common import DefaultValue
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.os.gsfile import g_file
from gspylib.common.VersionInfo import VersionInfo

g_envProfileDist = {}


class CheckEnvProfile(BaseItem):
    def __init__(self):
        super(CheckEnvProfile, self).__init__(self.__class__.__name__)

    def getProcessEnv(self, ProcessNum, Process):
        abnormal_flag = False
        processEnvDist = {}
        # Get environment variables
        if (os.path.isfile("/proc/%s/environ" % ProcessNum)):
            envInfoList = g_file.readFile("/proc/%s/environ" % ProcessNum)[
                0].split('\0')
            for env in envInfoList:
                envName = env.split('=')[0].strip()
                processEnvDist[envName] = env.split('=')[-1].strip()
            for env in g_envProfileDist.keys():
                # environment variables if exist
                if (not env in processEnvDist.keys() or
                        not processEnvDist[env]):
                    abnormal_flag = True
                    self.result.val += "There is no env[%s] in " \
                                       "process %s[%s].\n " \
                                       % (env, Process, ProcessNum)
                    continue
                # environment variables is GAUSSHOME
                if (env == "GAUSSHOME"):
                    if (g_envProfileDist[env] != processEnvDist[env]):
                        abnormal_flag = True
                        self.result.val += "The env[GAUSSHOME] is " \
                                           "inconsistent in process %s[%s] " \
                                           "and system.\nProcess: %s\n" \
                                           % (Process, ProcessNum,
                                              processEnvDist[env])
                ##environment variables is PATH
                elif (env == "PATH"):
                    binPath = "%s/bin" % g_envProfileDist["GAUSSHOME"]
                    ProcessEnvList = processEnvDist[env].split(':')
                    if (binPath not in ProcessEnvList):
                        abnormal_flag = True
                        self.result.val += "There is no [%s] in " \
                                           "process %s[%s]'s environment " \
                                           "variable [%s].\n " \
                                           % (binPath, Process,
                                              ProcessNum, env)
                else:
                    libPath = "%s/lib" % g_envProfileDist["GAUSSHOME"]
                    ProcessEnvList = processEnvDist[env].split(':')
                    if (libPath not in ProcessEnvList):
                        abnormal_flag = True
                        self.result.val += "There is no [%s] in process" \
                                           " %s[%s]'s environment variable" \
                                           " [%s].\n " % (libPath, Process,
                                                          ProcessNum, env)

        return abnormal_flag

    def doCheck(self):
        g_envProfileDist["GAUSSHOME"] = DefaultValue.getEnv("GAUSSHOME")
        g_envProfileDist["PATH"] = DefaultValue.getEnv("PATH")
        g_envProfileDist["LD_LIBRARY_PATH"] = DefaultValue.getEnv(
            "LD_LIBRARY_PATH")

        self.result.val = ""
        ProcessList = []
        ProcessDisk = {}
        abnormal_flag = False
        if (g_envProfileDist["GAUSSHOME"] == ""):
            abnormal_flag = True
            self.result.val += "The environmental variable " \
                               "GAUSSHOME is empty.\n"
        else:
            self.result.val += "GAUSSHOME        %s\n" % g_envProfileDist[
                "GAUSSHOME"]

        libPath = "%s/lib" % g_envProfileDist["GAUSSHOME"]
        if (libPath not in g_envProfileDist["LD_LIBRARY_PATH"].split(':')):
            abnormal_flag = True
            self.result.val += \
                VersionInfo.PRODUCT_NAME + \
                " lib path does not exist in LD_LIBRARY_PATH.\n"
        else:
            self.result.val += "LD_LIBRARY_PATH  %s\n" % libPath
        binPath = "%s/bin" % g_envProfileDist["GAUSSHOME"]
        # Whether the environment variable bin is in path
        if (binPath not in g_envProfileDist["PATH"].split(':')):
            abnormal_flag = True
            self.result.val += VersionInfo.PRODUCT_NAME + \
                               " bin path does not exist in PATH.\n"
        else:
            self.result.val += "PATH             %s\n" % binPath

        if abnormal_flag:
            self.result.rst = ResultStatus.NG
            return

        # Gets the current node information
        nodeInfo = self.cluster.getDbNodeByName(self.host)
        # check the number of instances
        if len(nodeInfo.datanodes) > 0:
            ProcessList.append("gaussdb")

        # Query process
        for Process in ProcessList:
            cmd = "ps ux | grep '%s/bin/%s' | grep -v 'grep' |" \
                  " awk '{print $2}'" % (self.cluster.appPath, Process)
            output = SharedFuncs.runShellCmd(cmd, self.user, self.mpprcFile)
            if (output != ""):
                if (len(output.split('\n')) > 1):
                    for ProcessNum in output.split('\n'):
                        ProcessDisk[ProcessNum] = [Process]
                else:
                    ProcessDisk[output] = [Process]
            else:
                self.result.val += "The process %s is not exist.\n" % Process
                abnormal_flag = True
        for ProcessNum in ProcessDisk.keys():
            # Get the process environment variables
            result = self.getProcessEnv(ProcessNum, ProcessDisk[ProcessNum])
            if not abnormal_flag:
                abnormal_flag = result

        if abnormal_flag:
            self.result.rst = ResultStatus.NG
        else:
            self.result.rst = ResultStatus.OK
