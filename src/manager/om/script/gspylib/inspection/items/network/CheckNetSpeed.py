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
import pwd
import subprocess
import _thread as thread
import time
import psutil
import platform
import multiprocessing
from multiprocessing.pool import ThreadPool
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.os.gsOSlib import g_OSlib
from gspylib.os.gsnetwork import g_network
from gspylib.common.ErrorCode import ErrorCode

DEFAULT_PARALLEL_NUM = 12
DEFAULT_LISTEN_PORT = 20000
DEFINE_DELAY_WARNING = 1000
DEFINE_SPEED_WARNING = 600000
DEFINE_DROP_WARNING = 0.005
g_lock = thread.allocate_lock()
MaxDelayFailFlag = None
errorMsg = []
speedMsg = ""
serviceIP = []


class CheckNetSpeed(BaseItem):
    def __init__(self):
        super(CheckNetSpeed, self).__init__(self.__class__.__name__)

    def makeIpList(self):
        ip_list = []
        for hostname in self.nodes:
            ip_list.append(SharedFuncs.getIpByHostName(hostname))

        return ip_list

    def runServer(self, serIP):
        base_listen_port = DEFAULT_LISTEN_PORT
        path = self.context.basePath

        server_count = 0
        max_server = 10
        while server_count < max_server:
            listen_port = base_listen_port + server_count
            try:
                p = subprocess.Popen([path + "/lib/checknetspeed/speed_test",
                                      "recv", serIP, str(listen_port), "tcp"],
                                     shell=False,
                                     stdout=open('/dev/null', 'w'))
            except Exception as e:
                raise Exception("[GAUSS-52200] :speed_test RuntimeException")
            server_count += 1

        return

    def runClient(self, self_index, ipList):
        base_listen_port = DEFAULT_LISTEN_PORT
        max_server = 10
        group = self_index // max_server
        path = self.context.basePath
        port = base_listen_port + self_index % max_server
        for ip in ipList:
            index = ipList.index(ip)
            if (index == self_index):
                continue
            if (index // max_server != group):
                continue
            try:
                p = subprocess.Popen([path + "/lib/checknetspeed/speed_test",
                                      "send", ip, str(port), "tcp"],
                                     shell=False,
                                     stdout=open('/dev/null', 'w'))
            except Exception as e:
                raise Exception("[GAUSS-52200] :speed_test RuntimeException")
                
        return

    def getCpuSet(self):
        """
        get cpu set of current board
        cat /proc/cpuinfo |grep processor
        """
        # do this function to get the parallel number
        cpuSet = multiprocessing.cpu_count()
        if (cpuSet > 1):
            return cpuSet
        else:
            return DEFAULT_PARALLEL_NUM

    def checkMaxDelay(self, ip):
        global MaxDelayFailFlag
        global errorMsg
        global serviceIP
        cmd = "ping -s 8192 -c 10 -i 0.3 %s|awk -F / '{print $7}'|" \
              "awk '{print $1}'" % ip
        output = SharedFuncs.runShellCmd(cmd)
        if (output.strip() != ""):
            try:
                max_delay = float(output.strip())
            except Exception as e:
                errorMsg.append(output.strip())
                return errorMsg
        else:
            MaxDelayFailFlag = True
            return
        if (max_delay > DEFINE_DELAY_WARNING):
            g_lock.acquire()
            string = "%s ping %s max delay is %.3fms" % (
                serviceIP, ip, max_delay)
            errorMsg.append(string)
            g_lock.release()

        return errorMsg

    def checkSar(self, ethName):
        global errorMsg
        global serviceIP
        global speedMsg
        cmd = "sar -n DEV 1 10|grep %s|grep Average|awk '{print $6}'" \
              % ethName
        output = SharedFuncs.runShellCmd(cmd)
        if (output.strip() != ""):
            try:
                average = float(output.strip())
            except Exception as e:
                errorMsg.append(output.strip())
                return errorMsg
        else:
            errorMsg.append(
                "get %s RX average failed. commands: %s" % (serviceIP, cmd))
            return errorMsg

        string = "%s RX average is %dkB/s" % (serviceIP, average)
        if (average < DEFINE_SPEED_WARNING):
            g_lock.acquire()
            errorMsg.append(string)
            g_lock.release()
        else:
            speedMsg = string
        return errorMsg

    def checkDrop(self, ethName, before_recv, before_drop):
        global errorMsg
        global serviceIP
        try:
            after_recv = psutil.net_io_counters(True)[ethName].packets_recv
            after_drop = psutil.net_io_counters(True)[ethName].dropin
        except Exception as e:
            self.doClean()
            self.result.rst = ResultStatus.NG
            self.result.val = "get %s RX drop percentage failed." % ethName
            raise Exception(ErrorCode.GAUSS_506["GAUSS_50620"])
        self.doClean()
        if (after_drop == before_drop):
            return

        percentage = (after_drop - before_drop) / (after_recv - before_recv)
        if (percentage > DEFINE_DROP_WARNING):
            g_lock.acquire()
            string = "%s RX droped percentage is %.4f" % (
                serviceIP, percentage * 100)
            errorMsg.append(string)
            g_lock.release()
        return errorMsg

    def doClean(self):
        currentUser = pwd.getpwuid(os.getuid())[0]
        while True:
            g_OSlib.killallProcess(currentUser, 'speed_test', '9')
            ProcList = g_OSlib.getProcPidList('speed_test')
            if (len(ProcList) == 0):
                break
            time.sleep(1)
        return

    def getTestFile(self):
        machine = platform.machine()
        testSpeedFile = "%s/lib/checknetspeed/speed_test" \
                        % self.context.basePath
        if machine == "x86_64":
            cmd = "cp -p %s_x86 %s" % (testSpeedFile, testSpeedFile)
        # debian: deepin    Maipo: NOE Kylin
        elif machine == "aarch64":
            cmd = "cp -p %s_arm %s" % (testSpeedFile, testSpeedFile)
        else:
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53017"] % machine)
        SharedFuncs.runShellCmd(cmd)

    def doCheck(self):
        global errorMsg
        global serviceIP
        global MaxDelayFailFlag
        self.getTestFile()
        serviceIP = SharedFuncs.getIpByHostName(self.host)
        for network in g_network.getAllNetworkInfo():
            if (network.ipAddress == serviceIP):
                networkCardNum = network.NICNum
                break
        if (not networkCardNum):
            raise Exception(ErrorCode.GAUSS_506["GAUSS_50619"])

        ethName = networkCardNum
        ipList = self.makeIpList()

        index = ipList.index(serviceIP)

        self.runServer(serviceIP)
        self.runClient(index, ipList)
        try:
            before_recv = psutil.net_io_counters(True)[ethName].packets_recv
            before_drop = psutil.net_io_counters(True)[ethName].dropin
        except Exception as e:
            self.doClean()
            self.result.rst = ResultStatus.NG
            self.result.val = "get %s RX drop percentage failed." % ethName
            raise Exception(ErrorCode.GAUSS_506["GAUSS_50621"]
                            + "Error: %s" % str(e))

        time.sleep(10)
        MaxDelayMsg = "Failde to get max delay."
        MaxDelayFailFlag = False
        pool = ThreadPool(self.getCpuSet())
        results = pool.map(self.checkMaxDelay, ipList)
        pool.close()
        pool.join()

        if MaxDelayFailFlag:
            errorMsg.append(MaxDelayMsg)
        self.checkSar(ethName)
        self.checkDrop(ethName, before_recv, before_drop)

        if errorMsg == []:
            self.result.rst = ResultStatus.OK
            self.result.val = "Check passed.\n%s" % speedMsg
        else:
            self.result.rst = ResultStatus.WARNING
            self.result.val = "Check not passed:\n" + "\n".join(
                errorMsg) + "\n%s" % speedMsg
