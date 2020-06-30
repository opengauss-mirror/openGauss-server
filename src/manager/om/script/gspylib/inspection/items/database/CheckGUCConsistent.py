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
import json
import configparser
import multiprocessing
from gspylib.common.DbClusterInfo import dbClusterInfo
from gspylib.inspection.common import SharedFuncs
from gspylib.common.Common import DefaultValue
from gspylib.common.ErrorCode import ErrorCode
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus

# master 
MASTER_INSTANCE = 0
# standby 
STANDBY_INSTANCE = 1
# dummy standby 
DUMMY_STANDBY_INSTANCE = 2

# cn
INSTANCE_ROLE_COODINATOR = 3
# dn
INSTANCE_ROLE_DATANODE = 4

g_gucDist = {}
g_ignoreList = []
g_logicList = []


class CheckGUCConsistent(BaseItem):
    def __init__(self):
        super(CheckGUCConsistent, self).__init__(self.__class__.__name__)

    def preCheck(self):
        # check the threshold was set correctly
        if (not self.threshold.__contains__('version')):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53013"] % "version")
        self.version = self.threshold['version']

    def checkLogicCluster(self):
        clusterInfo = dbClusterInfo()
        staticConfigDir = os.path.join(self.cluster.appPath, "bin")
        cmd = "find %s -name *.cluster_static_config" % staticConfigDir
        output = SharedFuncs.runShellCmd(cmd)
        if output:
            for staticConfigFile in output.splitlines():
                clusterInfo.initFromStaticConfig(self.user, staticConfigFile,
                                                 True)
                lcName = os.path.splitext(os.path.basename(staticConfigFile))[
                    0]
                for dbnode in clusterInfo.dbNodes:
                    if (dbnode.name == DefaultValue.GetHostIpOrName()):
                        return [lcName, dbnode]
            return ["", None]
        else:
            return ["", None]

    def getIgnoreParameters(self, configFile, ignoreSection, logicSection):
        global g_ignoreList
        global g_logicList
        fp = configparser.RawConfigParser()
        fp.read(configFile)
        secs = fp.sections()
        if (ignoreSection not in secs):
            return
        g_ignoreList = fp.options(ignoreSection)
        if self.cluster.isSingleInstCluster():
            g_ignoreList.append("synchronous_standby_names")
        g_logicList = fp.options(logicSection)

    def checkInstanceGucValue(self, Instance, needm, lcName="",
                              logicCluster=False):
        """
        get CN/DN instance guc parameters
        """
        global g_gucDist
        LCInstanceGucDist = {}
        lcInstance = {}
        sqlcmd = "select name,setting from pg_settings;"
        InstanceGucDist = {}
        output = SharedFuncs.runSqlCmd(sqlcmd, self.user, "", Instance.port,
                                       self.tmpPath, "postgres",
                                       self.mpprcFile, needm)
        gucValueList = output.split('\n')
        for gucValue in gucValueList:
            if (len(gucValue.split('|')) == 2):
                (parameter, value) = gucValue.split('|')
                if (
                        parameter == "transaction_read_only"
                        and Instance.instanceRole == INSTANCE_ROLE_DATANODE):
                    continue
                if (parameter not in g_ignoreList):
                    if (not logicCluster):
                        InstanceGucDist[parameter] = value
                    else:
                        if (parameter not in g_logicList):
                            InstanceGucDist[parameter] = value
                        elif (lcName and parameter in g_logicList):
                            LCInstanceGucDist[parameter] = value
                        else:
                            continue
        if (lcName):
            instanceName = "%s_%s_%s" % (lcName, "DN", Instance.instanceId)
            lcInstance[instanceName] = LCInstanceGucDist
            return lcInstance
        if Instance.instanceRole == INSTANCE_ROLE_DATANODE:
            Role = "DN"
        instanceName = "%s_%s" % (Role, Instance.instanceId)
        g_gucDist[instanceName] = InstanceGucDist

    def doCheck(self):
        """
        
        """
        global g_gucDist
        # get ignore list
        dirName = os.path.dirname(os.path.realpath(__file__))
        configFile = "%s/../../config/check_list_%s.conf" % (
            dirName, self.version)
        self.getIgnoreParameters(configFile, 'guc_ignore', 'guc_logic')
        DNidList = []
        result = []
        logicCluster = False
        nodeInfo = self.cluster.getDbNodeByName(self.host)
        masterDnList = SharedFuncs.getMasterDnNum(self.user, self.mpprcFile)
        for DnInstance in nodeInfo.datanodes:
            if (DnInstance.instanceType != DUMMY_STANDBY_INSTANCE):
                DNidList.append(DnInstance)
        if len(DNidList) < 1:
            raise Exception(ErrorCode.GAUSS_512["GAUSS_51249"])
        # get information of logicCluster on current node
        (lcName, dbnode) = self.checkLogicCluster()
        if (dbnode):
            logicCluster = True
            for DnInstance in dbnode.datanodes:
                if (DnInstance.instanceType != DUMMY_STANDBY_INSTANCE):
                    if (DnInstance.instanceId in masterDnList):
                        needm = False
                    else:
                        needm = True
                    result.append(
                        self.checkInstanceGucValue(DnInstance, needm, lcName,
                                                   logicCluster))
            g_gucDist[lcName] = result
        # test database Connection
        for Instance in DNidList:
            if not Instance:
                continue
            sqlcmd = "select pg_sleep(1);"
            if Instance.instanceId in masterDnList:
                needm = False
            else:
                needm = True
            output = SharedFuncs.runSqlCmd(sqlcmd, self.user, "",
                                           Instance.port, self.tmpPath,
                                           'postgres', self.mpprcFile, needm)
            self.checkInstanceGucValue(Instance, needm, "", logicCluster)

        self.result.val = json.dumps(g_gucDist)
        self.result.raw = str(g_gucDist)
        self.result.rst = ResultStatus.OK

    def postAnalysis(self, itemResult):
        errors = []
        ngs = []
        dnGucDist = {}
        lcdnGucDist = {}
        for i in itemResult.getLocalItems():
            if (i.rst == ResultStatus.ERROR):
                errors.append("%s: %s" % (i.host, i.val))
            if (i.rst == ResultStatus.NG):
                ngs.append("%s: %s" % (i.host, i.val))
        if (len(errors) > 0):
            itemResult.rst = ResultStatus.ERROR
            itemResult.analysis = "\n".join(errors)
            return itemResult
        if (len(ngs) > 0):
            itemResult.rst = ResultStatus.NG
            itemResult.analysis = "\n".join(ngs)
            return itemResult
        for v in itemResult.getLocalItems():
            gucDist = json.loads(v.val)
            for InstanceName in gucDist.keys():
                if (InstanceName[:2] == 'DN'):
                    dnGucDist[InstanceName] = gucDist[InstanceName]
                else:
                    if InstanceName in lcdnGucDist.keys():
                        lcdnGucDist[InstanceName].extend(gucDist[InstanceName])
                    else:
                        lcdnGucDist[InstanceName] = gucDist[InstanceName]
        for parameter in dnGucDist[list(dnGucDist.keys())[0]].keys():
            InstanceName = list(dnGucDist.keys())[0]
            keyValue = dnGucDist[InstanceName][parameter]
            relultStr = "\n%s:\n%s: %s\n" % (parameter, InstanceName, keyValue)
            flag = True
            for dnInstance in list(dnGucDist.keys())[1:]:
                value = dnGucDist[dnInstance][parameter]
                relultStr += "%s: %s\n" % (dnInstance, value)
                if (value != keyValue):
                    flag = False
            if (not flag):
                itemResult.analysis += relultStr

        for lcName in lcdnGucDist.keys():
            lcInstanceResult = lcdnGucDist[lcName]
            baseDn = lcInstanceResult[0]
            baseParameter = baseDn[list(baseDn.keys())[0]]
            for parameter in baseParameter.keys():
                keyValue = baseParameter[parameter]
                relultStr = "\n%s:\n%s: %s\n" % (
                    parameter, list(baseDn.keys())[0], keyValue)
                flag = True
                for otherDn in lcInstanceResult[1:]:
                    dnInstance = list(otherDn.keys())[0]
                    value = otherDn[dnInstance][parameter]
                    relultStr += "%s: %s\n" % (dnInstance, value)
                    if (value != keyValue):
                        flag = False
                if (not flag):
                    itemResult.analysis += relultStr

        if (itemResult.analysis):
            itemResult.rst = ResultStatus.NG
        else:
            itemResult.rst = ResultStatus.OK
            itemResult.analysis = "All DN instance guc value is consistent."
        return itemResult
