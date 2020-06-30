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
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.os.gsOSlib import g_OSlib
from gspylib.os.gsfile import g_file


class CheckMaxHandle(BaseItem):
    def __init__(self):
        super(CheckMaxHandle, self).__init__(self.__class__.__name__)

    def doCheck(self):
        flag = True
        parRes = ""
        # Determine if it is an ELK environment
        elk_env = DefaultValue.getEnv("ELK_SYSTEM_TABLESPACE")
        if (elk_env):
            expand_value = 640000
        else:
            expand_value = 1000000
        # Check system open files parameter
        output = g_OSlib.getUserLimits('open files')
        self.result.raw = output
        if (output != ""):
            self.result.val += output + "\n"
            resList = output.split(' ')
            limitValue = resList[-1].strip()
            # Unlimited check is passed
            if limitValue == 'unlimited':
                pass
            # Open file parameter value is less than 640000 will not pass
            if int(limitValue) < int(expand_value):
                flag = False
            else:
                pass
            # Write check results
            parRes += "Max open files: %s\n" % limitValue
        else:
            #
            flag = False
            parRes += "Failed to get system open files parameter.\n"

        # Check cluster process open files parameter
        if (self.cluster):
            pidList = g_OSlib.getProcess(
                os.path.join(self.cluster.appPath, 'bin/gaussdb'))
            for pid in pidList:
                if (not os.path.isfile(
                        "/proc/%s/limits" % pid) or not os.access(
                    "/proc/%s/limits" % pid, os.R_OK)):
                    continue
                openFileInfo = \
                    g_file.readFile('/proc/%s/limits' % pid, 'Max open files')[
                        0]
                if (openFileInfo):
                    value = openFileInfo.split()[3]
                if (int(value.strip()) < expand_value):
                    flag = False
                    parRes += "The value of " \
                              "max open files is %s on pid %s. " \
                              "it must not be less than %d.\n" % (
                                  value.strip(), pid, expand_value)
        if (flag):
            self.result.rst = ResultStatus.OK
        else:
            self.result.rst = ResultStatus.NG
        self.result.val = parRes

    def doSet(self):
        self.result.val = ""
        self.result.raw = ""
        limitPath = '/etc/security/limits.d/'
        if (os.path.isfile(os.path.join(limitPath, '91-nofile.conf'))):
            limitFile = '91-nofile.conf'
        else:
            limitFile = '90-nofile.conf'

        elk_env = DefaultValue.getEnv("ELK_SYSTEM_TABLESPACE")
        if (elk_env):
            expand_value = 640000
        else:
            expand_value = 1000000

        errMsg = SharedFuncs.SetLimitsConf(["soft", "hard"], "nofile",
                                           expand_value,
                                           os.path.join(limitPath, limitFile))
        if errMsg != "Success":
            self.result.val = "%s\n" % errMsg
        else:
            self.result.val = "Success to set openfile to %d\n" % expand_value
