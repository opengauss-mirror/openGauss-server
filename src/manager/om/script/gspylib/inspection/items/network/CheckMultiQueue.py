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
import subprocess
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.common.ErrorCode import ErrorCode
from gspylib.common.Common import DefaultValue
from gspylib.os.gsnetwork import g_network
from gspylib.os.gsfile import g_file
from gspylib.os.gsplatform import findCmdInPath

needRepairNetworkCardNum = []
networkCardNums = []
netWorkLevel = 10000


class CheckMultiQueue(BaseItem):
    def __init__(self):
        super(CheckMultiQueue, self).__init__(self.__class__.__name__)

    def doCheck(self):
        global needRepairNetworkCardNum
        global networkCardNums
        flag = "Normal"

        self.result.val = ""
        self.result.raw = ""
        if self.cluster:
            LocalNodeInfo = self.cluster.getDbNodeByName(self.host)
            backIP = LocalNodeInfo.backIps[0]
        elif self.ipAddr:
            backIP = self.ipAddr
        else:
            backIP = SharedFuncs.getIpByHostName(self.host)
        # Get the network card number
        allNetworkInfo = g_network.getAllNetworkInfo()
        for network in allNetworkInfo:
            if network.ipAddress == backIP:
                networkNum = network.NICNum
                BondMode = network.networkBondModeInfo
                confFile = network.networkConfigFile
                break

        if not networkNum or not BondMode or not confFile:
            if DefaultValue.checkDockerEnv():
                return
            raise Exception(ErrorCode.GAUSS_506["GAUSS_50619"])
        if BondMode != "BondMode Null":
            bondFile = '/proc/net/bonding/%s' % networkNum
            bondInfoList = g_file.readFile(bondFile, "Slave Interface")
            for bondInfo in bondInfoList:
                networkNum = bondInfo.split(':')[-1].strip()
                networkCardNums.append(networkNum)
        else:
            networkCardNums.append(networkNum)

        for networkCardNum in networkCardNums:
            cmdGetSpeedStr = "/sbin/ethtool %s | grep 'Speed:'" \
                             % networkCardNum
            (status, output) = subprocess.getstatusoutput(cmdGetSpeedStr)
            if len(output.split('\n')) > 1:
                for line in output.split('\n'):
                    if line.find("Speed:") >= 0:
                        output = line
                        break
            if output.find("Speed:") >= 0 and output.find("Mb/s") >= 0:
                netLevel = int(output.split(':')[1].strip()[:-4])
                if netLevel >= int(netWorkLevel):
                    cmd = "for i in `cat /proc/interrupts | grep '%s-' |" \
                          " awk -F ' ' '{print $1}' | " \
                          "awk -F ':' '{print $1}'`; " \
                          "do cat /proc/irq/$i/smp_affinity ; done" \
                          % networkCardNum
                    (status, output) = subprocess.getstatusoutput(cmd)
                    if status != 0:
                        self.result.val += "Failed to obtain network card" \
                                           " [%s] interrupt value. " \
                                           "Commands for getting interrupt" \
                                           " value: %s.\n" % (networkCardNum,
                                                              cmd)
                        if networkCardNum not in needRepairNetworkCardNum:
                            needRepairNetworkCardNum.append(networkCardNum)
                        flag = "Error"
                        continue

                    # cpu core number followed by 1 2 4 8,every 4 left shift 1
                    Mapping = {0: "1", 1: "2", 2: "4", 3: "8"}
                    for index, eachLine in enumerate(output.split()):
                        # Remove the ','
                        eachLine = eachLine.replace(",", "")
                        # Replace 0000,00001000 to 1,Remove invalid content
                        validValue = eachLine.replace("0", "")
                        # Convert the row index to the expected value
                        expandNum = Mapping[index % 4]
                        # Convert line index to expected position
                        expandBit = index / 4 * -1 - 1
                        # value and position is correct
                        if (len(eachLine) * -1) > expandBit:
                            self.result.val += "Network card [%s] " \
                                               "multi-queue support is not" \
                                               " enabled.\n" % networkCardNum
                            flag = "Error"
                            break
                        if (eachLine[expandBit] == expandNum and
                                validValue == expandNum):
                            continue
                        else:
                            self.result.val += "Network card [%s] " \
                                               "multi-queue support is " \
                                               "not enabled.\n" \
                                               % networkCardNum
                            if (networkCardNum not in
                                    needRepairNetworkCardNum):
                                needRepairNetworkCardNum.append(
                                    networkCardNum)
                            flag = "Error"
                            break

                    self.result.raw += "%s: \n %s \n" \
                                       % (networkCardNum, output)
                else:
                    self.result.val += "Warning: The speed of current card" \
                                       " \"%s\" is less than %s Mb/s.\n" \
                                       % (networkCardNum, netWorkLevel)
            else:
                if output.find("Speed:") >= 0:
                    if (networkCardNum not in
                            needRepairNetworkCardNum):
                        needRepairNetworkCardNum.append(networkCardNum)
                    flag = "Error"
                    self.result.val += "Failed to obtain the network card" \
                                       " [%s] speed value. Maybe the network" \
                                       " card is not working.\n" \
                                       % networkCardNum
                else:
                    self.result.val += "Failed to obtain the network" \
                                       " card [%s] speed value. Commands" \
                                       " for obtain the network card speed:" \
                                       " %s. Error:\n%s\n" \
                                       % (networkCardNum, cmdGetSpeedStr,
                                          output)
        if flag == "Normal":
            self.result.rst = ResultStatus.OK
        elif flag == "Warning":
            self.result.rst = ResultStatus.WARNING
        else:
            self.result.rst = ResultStatus.NG

    def doSet(self):
        self.result.val = ""
        cmd = "ps ax | grep -v grep | grep -q irqbalance; echo $?"
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0 and output.strip() == "0":
            subprocess.getstatusoutput("%s irqbalance" %
                                       findCmdInPath("killall"))
        for networkCardNum in networkCardNums:
            cmd = "cat /proc/interrupts | grep '%s-' | wc -l" % networkCardNum
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                self.result.val += " Failed to obtain network" \
                                   " card interrupt count numbers. "
            if not str(output.strip()).isdigit():
                count = 0
            else:
                count = int(output.strip())
            i = 0
            while i < count:
                # the dev name type like this:
                # eth1-1, eth1-rx-1, eth1-tx-1, eth1-TxRx-1
                # eth1-rx1, eth-tx1 in arm, get all network name interrupt
                cmd_IRQ = "cat /proc/interrupts | grep '%s.*-' |" \
                          " awk -F ' ' '{print $1}' | " \
                          "awk -F ':' '{print $1}'|awk 'NR==%s'" \
                          % (networkCardNum, str(i + 1))
                (status, output) = subprocess.getstatusoutput(cmd_IRQ)
                if status != 0 or output.strip() == "":
                    self.result.val = "Failed to obtain network card" \
                                      " interrupt value. Commands for " \
                                      "getting interrupt value: %s." % cmd_IRQ
                else:
                    IRQ = output.strip()
                    self.result.raw += "The network '%s' interrupt" \
                                       " configuration path:" \
                                       " /proc/irq/%s/smp_affinity." \
                                       % (networkCardNum, IRQ)
                    num = 2 ** i
                    # Under SuSE platform, when the length is greater than 8,
                    # the ',' must be used.
                    value = str(hex(num))[2:]
                    # Decimal 63 or more long number sending in L
                    if len(value) > 16 and value[-1] == 'L':
                        value = value[:-1]
                    result_value = ''
                    while len(value) > 8:
                        result_value = ",%s%s" % (value[-8:], result_value)
                        value = value[:-8]
                    result_value = "%s%s" % (value, result_value)

                    cmd_set = "echo '%s'> /proc/irq/%s/smp_affinity" % (
                        result_value, IRQ)
                    (status, output) = subprocess.getstatusoutput(cmd_set)
                    if status != 0:
                        self.result.val += "Failed to set network '%s' IRQ." \
                                           " Commands for setting: %s." \
                                           % (networkCardNum, cmd_set)
                    else:
                        self.result.val += "Set network card '%s' IRQ" \
                                           " to \"%s\"." % (networkCardNum,
                                                            result_value)
                i += 1
