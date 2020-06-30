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
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus


class CheckTableSpace(BaseItem):
    def __init__(self):
        super(CheckTableSpace, self).__init__(self.__class__.__name__)

    def getClusterDirectorys(self, dbNode):
        """
        function : Get cluster all directorys
        input : NA
        output : List
        """
        nodeDirs = []
        # including cm_server, cm_agent, cn, dn, gtm, etcd, ssd
        for dbInst in dbNode.datanodes:
            nodeDirs.append(dbInst.datadir)
            if (hasattr(dbInst, 'ssdDir') and len(dbInst.ssdDir) != 0):
                nodeDirs.append(dbInst.ssdDir)
        return nodeDirs

    def doCheck(self):
        self.result.val = ""
        nodeInfo = self.cluster.getDbNodeByName(self.host)
        clusterPathList = self.getClusterDirectorys(nodeInfo)
        clusterPathList.append(self.cluster.appPath)
        clusterPathList.append(self.cluster.logPath)
        clusterPathList.append(DefaultValue.getEnv('GPHOME'))
        clusterPathList.append(DefaultValue.getEnv('PGHOST'))

        nodeInfo = self.cluster.getDbNodeByName(self.host)
        if self.cluster.isSingleInstCluster():
            dirPath = nodeInfo.datanodes[0].datadir
        else:
            dirPath = nodeInfo.coordinators[0].datadir
        tableSpaceDir = os.path.join(dirPath, "pg_tblspc")
        tableSpaceList = os.listdir(tableSpaceDir)
        tablespacePaths = []
        if (len(tableSpaceList)):
            for filename in tableSpaceList:
                if (os.path.islink(os.path.join(tableSpaceDir, filename))):
                    linkDir = os.readlink(
                        os.path.join(tableSpaceDir, filename))
                    if (os.path.isdir(linkDir)):
                        tablespacePaths.append(linkDir)

        flag = "Normal"
        for tableSpace in tablespacePaths:
            if (tableSpace.find(' ') >= 0):
                flag = "Error"
                self.result.val += "Table space path[%s] contains spaces.\n" \
                                   % tableSpace

            # Support create tablespace in pg_location dir for V1R7
            if (tableSpace.find(os.path.join(dirPath, "pg_location")) == 0):
                continue
            tableSpaces = tableSpace.split('/')
            for clusterPath in clusterPathList:
                clusterPaths = clusterPath.split('/')
                if (tableSpace.find(clusterPath) == 0 and
                        tableSpaces[:len(clusterPaths)] == clusterPaths):
                    if (flag == "Normal"):
                        flag = "Warning"
                    self.result.val += "Table space path[%s] and cluster " \
                                       "path[%s] are nested.\n" % (
                                           tableSpace, clusterPath)
                elif (clusterPath.find(tableSpace) == 0 and
                      clusterPaths[:len(tableSpaces)] == tableSpaces):
                    flag = "Error"
                    self.result.val += "Table space path[%s] and cluster " \
                                       "path[%s] are nested.\n" % (tableSpace,
                                                                   clusterPath)
                else:
                    continue
        for tableSpace1 in tablespacePaths:
            tableSpaces1 = tableSpace1.split('/')
            for tableSpace2 in tablespacePaths:
                if (tableSpace1 == tableSpace2):
                    continue
                tableSpaces2 = tableSpace2.split('/')
                if (tableSpace1.find(tableSpace2) == 0 and
                        tableSpaces1[:len(tableSpaces2)] == tableSpaces2):
                    flag = "Error"
                    self.result.val += "Table space path[%s] and table space" \
                                       " path[%s] are nested.\n" \
                                       % (tableSpace1, tableSpace2)

        if (flag == "Error"):
            self.result.rst = ResultStatus.NG
        elif (flag == "Warning"):
            self.result.rst = ResultStatus.WARNING
        else:
            self.result.rst = ResultStatus.OK
            self.result.val = "All table space path is normal."
