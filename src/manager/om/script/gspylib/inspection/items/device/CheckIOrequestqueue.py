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
import glob
import platform
import os
import subprocess
from gspylib.common.Common import DefaultValue
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.os.gsfile import g_file

g_needRepair = []
expectedScheduler = "32768"


class CheckIOrequestqueue(BaseItem):
    def __init__(self):
        super(CheckIOrequestqueue, self).__init__(self.__class__.__name__)

    def obtainDataDir(self, nodeInfo):
        dataDirList = []
        for inst in nodeInfo.datanodes:
            dataDirList.append(inst.datadir)

        dataDirList.append(DefaultValue.getEnv("PGHOST"))
        dataDirList.append(DefaultValue.getEnv("GPHOME"))
        dataDirList.append(DefaultValue.getEnv("GAUSSHOME"))
        dataDirList.append(DefaultValue.getEnv("GAUSSLOG"))
        dataDirList.append("/tmp")
        return dataDirList

    def obtainDisk(self):
        """
        function: get disk name by partition
        input: partition list
        return: disk dict
        """
        devices = {}
        cmd = "fdisk -l 2>/dev/null | grep \"Disk /dev/\" " \
              "| grep -v \"/dev/mapper/\" | awk '{ print $2 }' " \
              "| awk -F'/' '{ print $NF }' | sed s/:$//g"
        output = SharedFuncs.runShellCmd(cmd)
        for disk in output.splitlines():
            cmd = "fdisk -l 2>/dev/null | grep \"%s\" " \
                  "| grep -v \"Disk\" | grep -v \"/dev/mapper/\" " \
                  "| awk '{ print $1 }'" % disk
            output = SharedFuncs.runShellCmd(cmd)
            if output:
                devices[disk] = output.splitlines()
            else:
                devices[disk] = "/dev/" + disk
        return devices

    def obtainDiskDir(self):
        cmd = "df -h -P /data* | grep -v 'Mounted' | awk '{print $6}'"
        output = SharedFuncs.runShellCmd(cmd)
        if output.lower().find("no such") >= 0:
            allDiskPath = ["/"]
        else:
            allDiskPath = output.split('\n')
        return allDiskPath

    def collectIORequest(self):
        """
        function : Collector ioRequest
        input    : NA
        output   : Dict
        """
        devices = []
        pathList = []

        if (self.cluster):
            pathList = self.obtainDataDir(
                self.cluster.getDbNodeByName(self.host))
        else:
            pathList = self.obtainDiskDir()
        diskDict = self.obtainDisk()
        for path in pathList:
            cmd = "df -h %s" % path
            output = SharedFuncs.runShellCmd(cmd)
            partitionInfo = output.split('\n')[-1]
            partitionName = partitionInfo.split()[0]
            if (partitionName in devices):
                continue
            else:
                devices.append(partitionName)
        result = {}
        for d in devices:
            for item in diskDict.items():
                if d in item[1]:
                    request = g_file.readFile(
                        "/sys/block/%s/queue/nr_requests" % item[0])[0]
                    result[item[0]] = request.strip()

        return result

    def doCheck(self):
        global g_needRepair
        data = self.collectIORequest()
        flag = True
        resultList = []
        if len(data) == 0:
            resultList.append("Not find IO Request file.")
        for i in data.keys():
            request = data[i]
            self.result.raw += "%s %s\n" % (i, request)
            if (i.startswith('loop') or i.startswith('ram')):
                continue
            if int(request) != int(expectedScheduler):
                g_needRepair.append(i)
                resultList.append(
                    "On device (%s) 'IO Request' RealValue '%s' "
                    "ExpectedValue '%s'" % (
                        i, request.strip(), expectedScheduler))
                flag = False
        self.result.val = "\n".join(resultList)
        if flag:
            self.result.rst = ResultStatus.OK
            self.result.val = "All disk IO request are normal."
        else:
            self.result.rst = ResultStatus.WARNING

    def doSet(self):
        resultStr = ""
        for dev in g_needRepair:
            cmd = 'echo 32768 > /sys/block/%s/queue/nr_requests' % dev
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                resultStr += "Failed to set dev %s.\n Error : %s." % (
                    dev, output)
                resultStr += "The cmd is %s " % cmd
        if (len(resultStr) > 0):
            self.result.val = resultStr
        else:
            self.result.val = "Set IOrequestqueue successfully."
