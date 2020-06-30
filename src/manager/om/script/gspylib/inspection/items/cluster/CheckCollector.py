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
from gspylib.inspection.common.CheckItem import BaseItem
from gspylib.inspection.common import SharedFuncs
from gspylib.inspection.common.CheckResult import ResultStatus
from gspylib.os.gsfile import g_file
from gspylib.common.Common import DefaultValue

SHELLPATH = os.path.realpath(
    os.path.join(os.path.split(os.path.realpath(__file__))[0],
                 "../../lib/checkcollector/"))
# file permission
FILE_MODE = 700


class CheckCollector(BaseItem):
    def __init__(self):
        super(CheckCollector, self).__init__(self.__class__.__name__)

    def checkFilePermission(self, filename):
        """
        Function : check file: 1.exist 2. isfile 3. permission
        Note     : 1.You must check that the file exist and is a file.
                   2.You can choose whether to check the file's
                    permission:executable.
        """
        # Check if the file exists
        if (not os.path.exists(filename)):
            raise Exception("The file %s does not exist." % filename)
        # Check whether the file
        if (not os.path.isfile(filename)):
            raise Exception("%s is not file." % filename)
        # Check the file permissions
        # Modify the file permissions
        if (not os.access(filename, os.X_OK)):
            g_file.changeMode(DefaultValue.KEY_DIRECTORY_MODE, filename)

    def genhostfile(self, nodenames):
        """
        Function : generate host file
        """
        iphostInfo = ""
        nodenameFile = "hostfile"
        # the path of script
        recordFile = os.path.join(SHELLPATH, nodenameFile)
        for nodename in nodenames:
            iphostInfo += '%s\n' % nodename

        g_file.createFile(recordFile, True, DefaultValue.KEY_DIRECTORY_MODE)

        # Write IP information to file
        g_file.writeFile(recordFile, [iphostInfo])

    def doCheck(self):
        parRes = ""
        # generate hostfile file, server node name
        self.genhostfile(self.nodes)
        # shell name
        shellName = "getClusterInfo.sh"
        # the path of script
        shellName = os.path.join(SHELLPATH, shellName)
        # judge permission
        self.checkFilePermission(shellName)

        g_file.replaceFileLineContent('omm', self.user, shellName)
        g_file.replaceFileLineContent(
            '\/opt\/huawei\/Bigdata\/mppdb\/.mppdbgs_profile',
            self.mpprcFile.replace('/', '\/'), shellName)
        # the shell command
        exectueCmd = "cd %s && sh %s -p %s" % (
            SHELLPATH, shellName, self.port)
        self.result.raw = exectueCmd
        # Call the shell script
        SharedFuncs.runShellCmd(exectueCmd, self.user, self.mpprcFile)
        self.result.rst = ResultStatus.OK
        pacakageName = os.path.join(self.outPath, "checkcollector_%s"
                                    % self.context.checkID)
        # crate tar package
        g_file.compressZipFiles(pacakageName, os.path.join(SHELLPATH, 'out'))
        # Check the result information
        parRes += "The inspection(checkcollector) has been completed!\n"
        parRes += "Please perform decompression firstly." \
                  " The log is saved in '%s.zip'" % (pacakageName)
        self.result.val = parRes
