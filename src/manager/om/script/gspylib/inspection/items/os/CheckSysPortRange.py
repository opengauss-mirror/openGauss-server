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
import subprocess
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.common.ErrorCode import ErrorCode
from gspylib.os.gsfile import g_file

PORT_RANGE = (26000, 65535)
SYSCTL_FILE = "/etc/sysctl.conf"


class CheckSysPortRange(BaseItem):
    def __init__(self):
        super(CheckSysPortRange, self).__init__(self.__class__.__name__)

    def doCheck(self):
        output = g_file.readFile('/proc/sys/net/ipv4/ip_local_port_range')[0]
        smallValue = output.split()[0].strip()
        bigValue = output.split()[1].strip()
        if (int(bigValue) > PORT_RANGE[1] or int(smallValue) < PORT_RANGE[0]):
            self.result.val = "The value of net.ipv4.ip_local_port_range " \
                              "is %d %d. it should be %d %d" % (
                                  int(smallValue), int(bigValue),
                                  int(PORT_RANGE[0]),
                                  int(PORT_RANGE[1]))
            self.result.rst = ResultStatus.NG
        else:
            self.result.rst = ResultStatus.OK
            self.result.val = "The value of net.ipv4." \
                              "ip_local_port_range is %d %d." % (
                                  int(smallValue), int(bigValue))
        self.result.raw = output

    def doSet(self):
        cmd = "sed -i '/net.ipv4.ip_local_port_range/d' %s" % SYSCTL_FILE
        cmd += " && echo 'net.ipv4.ip_local_port_range " \
               "= 26000 65535' >> %s" % SYSCTL_FILE
        cmd += " && sysctl -p"
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0 and output.find(
                '/proc/sys/net/ipv4/ip_local_port_range: N'
                'o such file or directory') >= 0):
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53020"]
                            % "net.ipv4.ip_local_port_range."
                            + "The cmd is %s " % cmd)
