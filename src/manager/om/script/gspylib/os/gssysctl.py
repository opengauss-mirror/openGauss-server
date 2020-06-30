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
import sys
import subprocess

sys.path.append(sys.path[0] + "/../../")
from gspylib.common.ErrorCode import ErrorCode
from gspylib.os.gsplatform import g_Platform


class SysctlInfo:
    """
    class: SysctlInfo
    """

    def __init__(self):
        """
        function: constructor
        """
        self.sysctlFile = g_Platform.getSysctlFile()

    def GetSysPara(self, paraList):
        """
        function : Get system parameters by paraList
        input  : paraList   parameters list
        output : para_dict parameters dict
        """
        para_dict = {}
        fullParaDict = {}
        try:
            cmd = "'%s' -a" % g_Platform.getSysctlCmd()
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                                " Error: \n%s" % str(output))
            line_list = output.split('\n')
            for line in line_list:
                words = line.split('=')
                if (len(words) < 2):
                    continue
                fullParaDict[words[0].strip()] = words[1].strip()
            # chose para
            for para in paraList:
                if (para in fullParaDict.keys()):
                    para_dict[para] = fullParaDict[para]
        except Exception as e:
            raise Exception(str(e))
        return para_dict

    def SetSysPara(self, paraDict):
        """
        function : Set system parameters by dict
        input  : paraDict   parameters dict
        output : null
        """
        try:
            # write or change configure file
            configure_file = self.sysctlFile
            with open(configure_file, 'r') as fp:
                full_line = fp.readlines()
            with open(configure_file, 'w') as fp:
                for current_line in full_line:
                    isFind = False
                    for key in paraDict.keys():
                        if current_line.find(key) >= 0 \
                                and current_line.strip()[0] != '#':
                            new_line = "#" + current_line
                            fp.write(current_line.replace(current_line,
                                                          new_line))
                            isFind = True
                    if not isFind:
                        fp.write(current_line.replace(current_line,
                                                      current_line))

                for key in paraDict.keys():
                    new_line = "\n" + key + " =" + paraDict[key]
                    fp.write(new_line)
            # restart server
            cmd = "'%s' -p" % g_Platform.getSysctlCmd()
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd +
                                " Error: \n%s" % str(output))
        except Exception as e:
            raise Exception(str(e))


g_sysctl = SysctlInfo()
