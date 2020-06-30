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
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
import os
from gspylib.os.gsfile import g_file
from gspylib.os.gsplatform import g_Platform

DUMMY_STANDBY_INSTANCE = 2
INSTANCE_ROLE_COODINATOR = 3


class CheckSysTabSize(BaseItem):
    def __init__(self):
        super(CheckSysTabSize, self).__init__(self.__class__.__name__)

    def doCheck(self):
        instance = []
        allDisk = []

        # Get all disk and the avail size
        cmd_df = "df -B M"
        diskinfo = SharedFuncs.runShellCmd(cmd_df, self.user, self.mpprcFile)
        # split with \n and remove the title
        diskList_space = diskinfo.split("\n")
        diskList_space.remove(diskList_space[0])
        # loop the list, remove space and remove the size unit "MB",
        # only keep disk path and avail size
        for disk_space in diskList_space:
            disk = disk_space.split()
            disk_new = []
            disk_new.append(disk[0])
            disk_new.append(int(disk[3].replace("M", "")))
            allDisk.append(disk_new)

        # Get the port and datadir list of instance
        nodeInfo = self.cluster.getDbNodeByName(self.host)
        CN = nodeInfo.coordinators
        # check if CN exists
        if (len(CN) > 0):
            instance.append(CN[0])
        else:
            # no CN in instance, do nothing
            pass
        for DnInstance in nodeInfo.datanodes:
            if (DnInstance.instanceType != DUMMY_STANDBY_INSTANCE):
                instance.append(DnInstance)

        # check if no instances in this node
        if (len(instance) == 0):
            return
        else:
            pass

        for inst in instance:
            cmd_dir = g_Platform.getDiskFreeCmd(inst.datadir)
            result = SharedFuncs.runShellCmd(cmd_dir, self.user,
                                             self.mpprcFile)
            diskInfo_withspace = result.split("\n")
            diskInfo = diskInfo_withspace[1].split()
            for disk in allDisk:
                if (diskInfo[0] == disk[0]):
                    disk.append(inst)
        masterDnList = SharedFuncs.getMasterDnNum(self.user, self.mpprcFile)
        # Get the database in the node, remove template0
        sqldb = "select datname from pg_database;"
        needm = False
        if (instance[0].instanceRole == INSTANCE_ROLE_COODINATOR):
            needm = False
        elif (instance[0].instanceId in masterDnList):
            needm = False
        else:
            needm = True
        output = SharedFuncs.runSqlCmd(sqldb, self.user, "", instance[0].port,
                                       self.tmpPath, "postgres",
                                       self.mpprcFile, needm)
        dbList = output.split("\n")
        dbList.remove("template0")

        # loop all database with port list
        value = ""
        Flag = []
        for disk in allDisk:
            sumSize = 0
            for inst in disk[2:]:
                for db in dbList:
                    # Calculate the size with sql cmd
                    cmd = "select sum(pg_total_relation_size(oid)/1024)/1024" \
                          " from pg_class where oid<16384 and relkind='r';"
                    needm = False
                    if (inst.instanceRole == INSTANCE_ROLE_COODINATOR):
                        needm = False
                    elif (inst.instanceId in masterDnList):
                        needm = False
                    else:
                        needm = True
                    output = SharedFuncs.runSqlCmd(cmd, self.user, "",
                                                   inst.port, self.tmpPath, db,
                                                   self.mpprcFile, needm)
                    sumSize = sumSize + float(output)
                # Calculate the size of datadir
                strdir = inst.datadir
                clog = g_file.getDirSize(os.path.join(strdir, 'pg_clog'), "M")
                size_clog = int(clog[0].replace("M", ""))
                xlog = g_file.getDirSize(os.path.join(strdir, 'pg_xlog'), "M")
                size_xlog = int(xlog[0].replace("M", ""))
                sumSize = sumSize + size_clog + size_xlog
            if (sumSize == 0):
                continue
            # Compare system table size with avail disk size
            if (sumSize < disk[1]):
                Flag.append(True)
                FileSystem = "FileSystem: %s" % disk[0]
                SystemTableSize = "SystemTableSize: %sM" % sumSize
                DiskAvailSize = "DiskAvailSize: %sM" % disk[1]
                value += FileSystem.ljust(35) + SystemTableSize.ljust(35) \
                         + DiskAvailSize.ljust(35) + "Status: OK; \n"
            elif (sumSize >= disk[1]):
                Flag.append(False)
                FileSystem = "FileSystem: %s" % disk[0]
                SystemTableSize = "SystemTableSize: %sM" % sumSize
                DiskAvailSize = "DiskAvailSize: %sM" % disk[1]
                value += FileSystem.ljust(35) + SystemTableSize.ljust(35) \
                         + DiskAvailSize.ljust(35) + "Status: NG; \n"
        self.result.val = value
        if (False not in Flag):
            self.result.rst = ResultStatus.OK
        else:
            self.result.rst = ResultStatus.NG
