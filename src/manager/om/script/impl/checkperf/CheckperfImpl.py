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
import sys
from gspylib.common.Common import DefaultValue
from gspylib.os.gsfile import g_file


class CheckperfImpl():
    """
    Class: check perf impl
    """

    def __init__(self):
        """
        function: constructor
        """
        pass

    def CheckPMKPerf(self, outputInfo):
        """
        function: check pmk perf
        """
        pass

    def CheckSSDPerf(self, outputInfo):
        """
        function: check ssd perf
        input  : outputInfo
        output : NA
        """
        pass

    def run(self):
        """
        function: the real interface that execute the check method
        input : NA
        output: NA
        """
        try:
            outputInfo = None
            # check output file
            if self.opts.outFile != "":
                self.opts.outFile_tmp = os.path.join(
                    DefaultValue.getTmpDirFromEnv(self.opts.user),
                    (os.path.split(self.opts.outFile)[1]
                     + "_tmp_%s" % os.getpid()))
                outputInfo = self.setOutFile()
            else:
                outputInfo = sys.stdout
            # check check item
            for key in self.opts.checkItem:
                if key == "PMK":
                    # check PMK
                    self.CheckPMKPerf(outputInfo)
                elif key == "SSD":
                    # check SSD
                    self.CheckSSDPerf(outputInfo)

            # Follow-up
            self.closeFile(outputInfo)
        except Exception as e:
            # close file handle if outputInfo is out file
            if self.opts.outFile and outputInfo:
                outputInfo.flush()
                outputInfo.close()
            if os.path.isfile(self.opts.outFile_tmp):
                g_file.removeFile(self.opts.outFile_tmp)
            # modify the log file's owner
            g_file.changeOwner(self.opts.user, self.logger.logFile)
            self.logger.error(str(e))
            sys.exit(1)

    def setOutFile(self):
        """
        function: set out file
        input  : NA
        output : NA
        """
        # get directory component of a pathname
        dirName = os.path.dirname(self.opts.outFile)
        # judge if directory
        if not os.path.isdir(dirName):
            g_file.createDirectory(dirName, True,
                                   DefaultValue.KEY_DIRECTORY_MODE)
        # create output file and modify permission
        g_file.createFile(self.opts.outFile, True, DefaultValue.KEY_FILE_MODE)
        g_file.changeOwner(self.opts.user, self.opts.outFile)
        self.logger.log(
            "Performing performance check. "
            "Output the checking result to the file %s." % self.opts.outFile)
        # write file
        self.opts.outFile_tmp = os.path.join(
            DefaultValue.getTmpDirFromEnv(self.opts.user),
            (os.path.split(self.opts.outFile)[1] + "_tmp_%s" % os.getpid()))
        if not os.path.isfile(self.opts.outFile_tmp):
            g_file.createFile(self.opts.outFile_tmp, True,
                              DefaultValue.KEY_FILE_MODE)
        g_file.changeOwner(self.opts.user, self.opts.outFile_tmp)
        fp = open(self.opts.outFile_tmp, "w")
        outputInfo = fp
        return outputInfo

    def closeFile(self, fp):
        """
        function: close file
        input  : fp
        output : NA
        """
        if self.opts.outFile and fp:
            # close file handle if outputInfo is out file
            fp.flush()
            fp.close()
            g_file.moveFile(self.opts.outFile_tmp, self.opts.outFile)
            self.logger.log("Performance check is completed.")
