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
#############################################################################
import sys
import os
import socket
import time

sys.path.append(sys.path[0] + "/../../")
from gspylib.common.ErrorCode import ErrorCode
from gspylib.common.Common import DefaultValue
from gspylib.os.gsfile import g_file
from gspylib.os.gsOSlib import g_OSlib
from gspylib.os.gsnetwork import g_network

TIME_OUT = 2
RETRY_TIMES = 100


class BaseComponent(object):
    '''
    The class is used to define base component.
    '''

    def __init__(self):
        '''
        function: initialize the parameters
        input : NA
        output: NA
        '''
        self.logger = None
        self.instInfo = None
        self.version = ""
        self.pkgName = ""
        self.initParas = {}
        self.binPath = ""
        self.dwsMode = False
        self.level = 1
        self.clusterType = DefaultValue.CLUSTER_TYPE_SINGLE_INST

    def install(self):
        pass

    def setGucConfig(self, setMode='set', paraDict=None):
        pass

    def getGucConfig(self, paraList):
        pass

    def setPghbaConfig(self):
        pass

    def start(self):
        pass

    def stop(self):
        pass

    def uninstall(self):
        pass

    def killProcess(self):
        """
        function: kill process
        input:  process flag
        output: NA
        """
        pass

    def fixPermission(self):
        pass

    def upgrade(self):
        pass

    def createPath(self):
        pass

    def perCheck(self):
        """
        function: 1.Check instance port  
                  2.Check instance IP
        input : NA
        output: NA
        """
        ipList = self.instInfo.listenIps
        ipList.extend(self.instInfo.haIps)
        portList = []
        portList.append(self.instInfo.port)
        portList.append(self.instInfo.haPort)

        ipList = DefaultValue.Deduplication(ipList)
        portList = DefaultValue.Deduplication(portList)
        # check port
        for port in portList:
            self.__checkport(port, ipList)
        # check ip
        failIps = g_network.checkIpAddressList(ipList)
        if (len(failIps) > 0):
            raise Exception(ErrorCode.GAUSS_506["GAUSS_50600"] +
                            " The IP is %s." % ",".join(failIps))

    def __checkport(self, port, ipList):
        """
        function: check Port
        input : NA
        output: NA
        """
        tmpDir = DefaultValue.getTmpDirFromEnv()
        if (not os.path.exists(tmpDir)):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] %
                            tmpDir + " Please create it first.")
        pgsqlFiles = os.listdir(tmpDir)

        self.__checkRandomPortRange(port)

        pgsql = ".s.PGSQL.%d" % port
        pgsql_lock = ".s.PGSQL.%d.lock" % port
        if (pgsql in pgsqlFiles):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50200"] %
                            "socket file" + " Port:%s." % port)

        if (pgsql_lock in pgsqlFiles):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50200"] %
                            "socket lock file" + " Port:%s." % port)

        # Verify that the port is occupied
        for ip in ipList:
            sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sk.settimeout(TIME_OUT)

            # retry  port 4 times
            retryFlag = True
            retryTime = 0
            while (retryFlag):
                try:
                    sk.bind((ip, port))
                    sk.close()
                    break
                except socket.error as e:
                    retryTime += 1
                    time.sleep(1)
                    if (retryTime > RETRY_TIMES):
                        retryFlag = False
                        try:
                            portProcessInfo = g_OSlib.getPortProcessInfo(port)
                            self.logger.debug("The ip [%s] port [%s] is "
                                              "occupied. \nBind error "
                                              "msg:\n%s\nDetail msg:\n%s" % \
                                              (ip, port, str(e),
                                               portProcessInfo))
                        except Exception as e:
                            self.logger.debug("Failed to get the process "
                                              "information of the port [%s], "
                                              "output:%s." % (port, str(e)))
                        raise Exception(ErrorCode.GAUSS_506["GAUSS_50601"] %
                                        port)

    def __checkRandomPortRange(self, port):
        """
        function: Check if port is in the range of random port
        input : port
        output: NA
        """
        res = []
        try:
            rangeFile = "/proc/sys/net/ipv4/ip_local_port_range"
            output = g_file.readFile(rangeFile)
            res = output[0].split()
        except Exception as e:
            self.logger.debug(
                "Warning: Failed to get the range of random port."
                " Detail: \n%s" % str(e))
            return
        if (len(res) != 2):
            self.logger.debug("Warning: The range of random port is invalid. "
                              "Detail: \n%s" % str(output))
            return
        minPort = int(res[0])
        maxPort = int(res[1])
        if (port >= minPort and port <= maxPort):
            self.logger.debug("Warning: Current instance port is in the "
                              "range of random port(%d - %d)." % (minPort,
                                                                  maxPort))

    def postCheck(self):
        pass
