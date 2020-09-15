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
import platform
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.common.Common import DefaultValue
from gspylib.os.gsfile import g_Platform


class CheckSctpService(BaseItem):
    def __init__(self):
        super(CheckSctpService, self).__init__(self.__class__.__name__)

    def doCheck(self):

        parRes = ""
        flag = "Normal"
        cmd = "ls -l /lib/modules/`uname -r`/kernel/net/sctp/sctp.ko*"
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0 or output == "" or output.find(
                "No such file or directory") > 0):
            if DefaultValue.checkDockerEnv():
                return
            flag = "Error"
            parRes += "There is no sctp service."
        else:
            cmd = "modprobe sctp;"
            cmd += "lsmod |grep sctp"
            (status, output) = subprocess.getstatusoutput(cmd)
            if (output == ""):
                flag = "Error"
                parRes += "sctp service is not loaded."

        cmd = "cat %s | grep '^insmod.*sctp.ko'" % DefaultValue.getOSInitFile()
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0 or output == ""):
            if (flag == "Normal"):
                flag = "Warning"
            parRes += "Sctp service is not set to boot from power on."

        self.result.val = parRes
        self.result.raw = output
        if (flag == "Error"):
            self.result.rst = ResultStatus.NG
        elif (flag == "Warning"):
            self.result.rst = ResultStatus.WARNING
        else:
            self.result.rst = ResultStatus.OK
            self.result.val = "Sctp service is Normal."

    def doSet(self):
        self.result.val = ""
        parRes = ""
        sctpFile = ""
        initFileSuse = "/etc/init.d/boot.local"
        initFileRedhat = "/etc/rc.d/rc.local"
        cmd = "ls -l /lib/modules/`uname -r`/kernel/net/sctp/sctp.ko*"
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0 or output == "" or output.find(
                "No such file or directory") > 0):
            parRes = "There is no sctp service.\n"
        else:
            sctpFile = output.split()[-1]
            cmd = "modprobe sctp;"
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                cmd = "insmod %s >/dev/null 2>&1;lsmod |grep sctp" % sctpFile
                (status, output) = subprocess.getstatusoutput(cmd)
                if status != 0 or output == "":
                    parRes = "Failed to load sctp service.\n"
        distname, version, idnum = g_Platform.dist()
        if (distname in ["redhat", "centos", "euleros", "openEuler"]):
            cmd = "cat %s | grep sctp" % initFileRedhat
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0 or output == ""):
                cmd = "echo 'modprobe sctp' >> /etc/rc.d/rc.local;"
                cmd += "echo" \
                       " 'insmod %s >/dev/null 2>&1' >> /etc/rc.d/rc.local " \
                       % sctpFile
                (status, output) = subprocess.getstatusoutput(cmd)
                if (status != 0):
                    parRes += "Failed to add sctp service to boot.\n"
        else:
            cmd = "cat %s | grep stcp" % initFileSuse
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0 or output == ""):
                cmd = "echo 'modprobe sctp' >> /etc/init.d/boot.local;"
                cmd += "echo '%s >/dev/null 2>&1' >> /etc/init.d/boot.local " \
                       % sctpFile
                (status, output) = subprocess.getstatusoutput(cmd)
                if (status != 0):
                    parRes += "Failed to add sctp service to boot."
        self.result.val = parRes
