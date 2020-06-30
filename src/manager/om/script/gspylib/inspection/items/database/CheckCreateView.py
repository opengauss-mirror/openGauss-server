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
from gspylib.common.Common import DefaultValue

SQLPATH = os.path.realpath(
    os.path.join(os.path.split(os.path.realpath(__file__))[0],
                 "../../lib/checkcreateview/"))
OUTPUTPATH = os.path.realpath(
    os.path.join(os.path.split(os.path.realpath(__file__))[0],
                 "../../output/"))


class CheckCreateView(BaseItem):
    def __init__(self):
        super(CheckCreateView, self).__init__(self.__class__.__name__)

    def doCheck(self):
        flag = 1
        resultStr = ""
        databaseListSql = "select datname from pg_database where datname != " \
                          "'template0';"
        output = SharedFuncs.runSqlCmd(databaseListSql, self.user, "",
                                       self.port, self.tmpPath, "postgres",
                                       self.mpprcFile)
        dbList = output.split("\n")
        sqlFileName = os.path.join(SQLPATH, "check_viewdef.sql")
        cmd = "chmod %s %s" % (DefaultValue.KEY_DIRECTORY_MODE, sqlFileName)
        SharedFuncs.runShellCmd(cmd)
        for databaseName in dbList:
            sqlFile = "%s/viewdef_%s.sql" % (OUTPUTPATH, databaseName)
            cmd = "gsql -d %s -p %s -q -t -f %s -o %s/viewdef_%s.sql" % (
                databaseName, self.port, sqlFileName, OUTPUTPATH, databaseName)
            if (os.getuid() == 0):
                cmd = "su - %s -c \"source %s;%s\"" % (
                    self.user, self.mpprcFile, cmd)
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                resultStr += "execute %s error. Error:%s\n" % (cmd, output)
                flag = 0
                continue
            cmd = "gsql -d %s -p %s -f %s -o %s/viewdef_%s.out " % (
                databaseName, self.port, sqlFile, OUTPUTPATH, databaseName)
            if (os.getuid() == 0):
                cmd = "su - %s -c \"source %s;%s\"" % (
                    self.user, self.mpprcFile, cmd)
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                resultStr += "execute %s error. Error:%s\n" % (cmd, output)
                flag = 0
                continue
            else:
                for line in output.split("\n"):
                    line = line.split(":")
                    for word in line:
                        if ("ERROR" == word.strip()):
                            flag = 0
                            errorNum = line[line.index(' ERROR') - 1]
                            with open(sqlFile, 'r') as fp:
                                sqlLines = fp.readlines()
                            view = sqlLines[int(errorNum) - 1].split()[
                                -1].strip(';')
                            viewList = view.strip('\"').split('.')
                            resultStr = "view %s needs to be fixed" % \
                                        sqlLines[int(errorNum) - 1].split()[
                                            -1].strip(';')
                            cmd = "gs_dump %s -p %s -t '\"%s\"'.'\"%s\"' -f " \
                                  "'%s/%s.sql'" % (
                                      databaseName, self.port, viewList[0],
                                      viewList[1],
                                      OUTPUTPATH, view.strip('\"'))
                            if (os.getuid() == 0):
                                cmd = "su - %s -c \"source %s;%s\"" % (
                                    self.user, self.mpprcFile, cmd)
                            (status, output) = subprocess.getstatusoutput(cmd)
                            if (status != 0):
                                self.result.val += "execute %s error\n" % cmd
                        else:
                            continue
        if (flag == 0):
            self.result.rst = ResultStatus.NG
            self.result.val = resultStr
        else:
            self.result.rst = ResultStatus.OK
            self.result.val = " No view needs to be fixed."
