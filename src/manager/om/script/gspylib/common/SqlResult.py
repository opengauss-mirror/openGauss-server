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
# ----------------------------------------------------------------------------
# Description  : Result.py is a utility to store search result from database
#############################################################################
import os
import sys
from ctypes import *

sys.path.append(sys.path[0] + "/../../")
from gspylib.common.Common import DefaultValue


class sqlResult():
    """
    Class for storing search result from database
    """

    def __init__(self, result):
        """
        Constructor
        """
        self.resCount = 0
        self.resSet = []
        self.result = result

    def parseResult(self):
        """
        function : get resCount and resSet from result
        input:NA
        output:NA
        """
        try:
            libpath = os.path.join(DefaultValue.getEnv("GAUSSHOME"), "lib")
            sys.path.append(libpath)
            libc = cdll.LoadLibrary("libpq.so.5.5")
            libc.PQntuples.argtypes = [c_void_p]
            libc.PQntuples.restype = c_int
            libc.PQnfields.argtypes = [c_void_p]
            libc.PQnfields.restype = c_int
            libc.PQgetvalue.restype = c_char_p
            ntups = libc.PQntuples(self.result)
            nfields = libc.PQnfields(self.result)
            libc.PQgetvalue.argtypes = [c_void_p, c_int, c_int]
            self.resCount = ntups
            for i in range(ntups):
                tmpString = []
                for j in range(nfields):
                    paramValue = libc.PQgetvalue(self.result, i, j)
                    if (paramValue is not None):
                        tmpString.append(string_at(paramValue).decode())
                    else:
                        tmpString.append("")
                self.resSet.append(tmpString)
        except Exception as e:
            raise Exception("%s" % str(e))
