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
import platform
import os
from gspylib.common.Common import DefaultValue
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.os.gsfile import g_file

deviceNeedRepair = []


class CheckIOConfigure(BaseItem):
    def __init__(self):
        super(CheckIOConfigure, self).__init__(self.__class__.__name__)

    def obtainDataDir(self, nodeInfo):
        dataDirList = []
        for inst in nodeInfo.datanodes:
            dataDirList.append(inst.datadir)
        for inst in nodeInfo.cmservers:
            dataDirList.append(inst.datadir)
        for inst in nodeInfo.coordinators:
            dataDirList.append(inst.datadir)
        for inst in nodeInfo.gtms:
            dataDirList.append(inst.datadir)
        if (hasattr(nodeInfo, 'etcds')):
            for inst in nodeInfo.etcds:
                dataDirList.append(inst.datadir)

        dataDirList.append(DefaultValue.getEnv("PGHOST"))
        dataDirList.append(DefaultValue.getEnv("GPHOME"))
        dataDirList.append(DefaultValue.getEnv("GAUSSHOME"))
        dataDirList.append(DefaultValue.getEnv("GAUSSLOG"))
        dataDirList.append("/tmp")
        return dataDirList

    def obtainDiskDir(self):
        cmd = "df -h -P /data* | grep -v 'Mounted' | awk '{print $6}'"
        output = SharedFuncs.runShellCmd(cmd)
        if output.lower().find("no such") >= 0:
            allDiskPath = ["/"]
        else:
            allDiskPath = output.split('\n')
        return allDiskPath

    def getDevices(self):
        pathList = []
        devices = []
        diskName = ""
        diskDic = {}
        diskDic = self.getDisk()
        if (self.cluster):
            pathList = self.obtainDataDir(
                self.cluster.getDbNodeByName(self.host))
        else:
            pathList = self.obtainDiskDir()
        for path in pathList:
            if path.find('No such file or directory') >= 0 or path.find(
                    'no file systems processed') >= 0:
                self.result.rst = ResultStatus.ERROR
                self.result.val += \
                    "There are no cluster and no /data* directory."
                return
            cmd = "df -P -i %s" % path
            output = SharedFuncs.runShellCmd(cmd)
            # Filesystem      Inodes  IUsed   IFree IUse% Mounted on
            # /dev/xvda2     2363904 233962 2129942   10% /
            diskName = output.split('\n')[-1].split()[0]
            for disk in diskDic.keys():
                if diskName in diskDic[disk] and disk not in devices:
                    devices.append(disk)
        return devices

    def getDisk(self):
        diskDic = {}
        cmd = "fdisk -l 2>/dev/null " \
              "| grep 'Disk /dev/' | grep -v '/dev/mapper/' " \
              "| awk '{ print $2 }'| awk -F'/' '{ print $NF }'| sed s/:$//g"
        output = SharedFuncs.runShellCmd(cmd)
        for disk in output.splitlines():
            cmd = "fdisk -l 2>/dev/null | grep '%s'" \
                  "| grep -v '/dev/mapper/'| grep -v 'Disk /dev/'" \
                  "| awk -F ' ' ' {print $1}'" % disk
            output = SharedFuncs.runShellCmd(cmd)
            if output:
                diskDic[disk] = output.splitlines()
            else:
                diskDic[disk] = "/dev/" + disk
        return diskDic

    def collectIOschedulers(self):
        devices = set()
        data = dict()
        files = self.getDevices()
        for f in files:
            fname = "/sys/block/%s/queue/scheduler" % f
            words = fname.split("/")
            if len(words) != 6:
                continue
            devices.add(words[3].strip())

        for d in devices:
            if (not d):
                continue
            device = {}
            scheduler = g_file.readFile("/sys/block/%s/queue/scheduler" % d)[0]
            words = scheduler.split("[")
            if len(words) != 2:
                continue
            words = words[1].split("]")
            if len(words) != 2:
                continue
            device["request"] = words[0].strip()
            for dead in scheduler.split():
                if dead.find("deadline") >= 0:
                    device["deadvalue"] = dead.split("[")[-1].split("]")[0]
                else:
                    continue
            data[d] = device
        return data

    def doCheck(self):
        global deviceNeedRepair
        deviceNeedRepair = []
        expectedScheduler = "deadline"
        data = self.collectIOschedulers()
        flag = True
        resultStr = ""
        for i in data.keys():
            result = ()
            expectedScheduler = data[i]["deadvalue"]
            request = data[i]["request"]
            if (request != expectedScheduler):
                result = (i, expectedScheduler)
                deviceNeedRepair.append(result)
                resultStr += \
                    "On device (%s) 'IO Request' RealValue '%s' " \
                    "ExpectedValue '%s'" % (
                        i, request.strip(), expectedScheduler)
                flag = False
        self.result.val = resultStr
        if flag:
            self.result.rst = ResultStatus.OK
            self.result.val = "All disk IO Request is deadline."
        else:
            self.result.rst = ResultStatus.NG

    def doSet(self):
        for dev, expectedScheduler in deviceNeedRepair:
            self.SetIOSchedulers(dev, expectedScheduler)

    def SetIOSchedulers(self, devname, expectedScheduler):
        """
        function : Set IO Schedulers
        input  : String
        output : NA
        """
        (THPFile, initFile) = SharedFuncs.getTHPandOSInitFile()
        cmd = " echo %s >> /sys/block/%s/queue/scheduler" % (
            expectedScheduler, devname)
        cmd += " && echo \"echo %s >> /sys/block/%s/queue/scheduler\" >> %s" \
               % (
                   expectedScheduler, devname, initFile)
        SharedFuncs.runShellCmd(cmd)
