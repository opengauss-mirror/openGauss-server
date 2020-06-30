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
import pwd
import time
import subprocess
import logging.handlers
import os
from gspylib.common.Common import DefaultValue
from gspylib.common.ErrorCode import ErrorCode

# max log file size
# 16M
MAXLOGFILESIZE = 16 * 1024 * 1024
KEY_FILE_MODE = 600


class LoggerFactory():
    def __init__(self):
        pass

    @staticmethod
    def getLogger(module, logFile, user=""):
        """
        function : config log handler
        input : module, logFileName, logLevel
        output : log
        """
        afilename = LoggerFactory.getLogFileName(os.path.realpath(logFile))
        if (not os.path.exists(afilename)):
            dirName = os.path.dirname(afilename)
            cmd = "if [ ! -d %s ]; then mkdir %s -p -m %s;fi" % (
                dirName, dirName, DefaultValue.KEY_DIRECTORY_MODE)
            cmd += ";touch %s && chmod %s %s" % (
                afilename, KEY_FILE_MODE, afilename)
            # The user exists and is not the current user
            if (user and pwd.getpwnam(user).pw_uid != os.getuid()):
                cmd = "su - %s -c \"%s\" " % (user, cmd)
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50206"]
                                % ("log file [%s]" % afilename) +
                                "Error:\n%s" % output +
                                "The cmd is %s " % cmd)
        log = logging.getLogger(module)
        LoggerFactory._addFileHandle(log, afilename)
        LoggerFactory._addConsoleHandle(log)
        return (log, afilename)

    @staticmethod
    def getLogFileName(oldLogFile):
        """
        function : Increase the time stamp and check the file size
        input : logFileName
        output : String
        """
        # get current time
        currentTime = time.strftime("%Y-%m-%d_%H%M%S")
        # Check log file correctness
        dirName = os.path.dirname(oldLogFile)
        originalFileName = os.path.basename(oldLogFile)
        resList = originalFileName.split(".")
        if (len(resList) > 2):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50235"] % oldLogFile)
        (prefix, suffix) = os.path.splitext(originalFileName)
        if (suffix != ".log"):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50212"]
                            % (oldLogFile, ".log"))
        # The log file have time stamped in -L mode
        if (len(originalFileName) > 21):
            timeStamp = originalFileName[-21:-4]
            if (LoggerFactory.is_valid_date(timeStamp)):
                return oldLogFile

        # Defaults log file
        newLogFile = dirName + "/" + prefix + "-" + currentTime + suffix
        if (os.path.isdir(dirName)):
            # Check old log file list
            cmd = "ls %s | grep '^%s-' | grep '%s$'" % (
                dirName, prefix, suffix)
            (status, output) = subprocess.getstatusoutput(cmd)
            if (status == 0):
                filenameList = []
                for echoLine in output.split("\n"):
                    filename = echoLine.strip()
                    existedResList = filename.split(".")
                    if (len(existedResList) > 2):
                        continue
                    existedSuffix = os.path.splitext(filename)[1]
                    if (existedSuffix != ".log"):
                        continue
                    if (len(originalFileName) + 18 != len(filename)):
                        continue
                    timeStamp = filename[-21:-4]
                    # check log file name
                    if (LoggerFactory.is_valid_date(timeStamp)):
                        pass
                    else:
                        continue
                    # Add the valid log file
                    filenameList.append(filename)

                if (len(filenameList)):
                    fileName = max(filenameList)
                    logFile = dirName + "/" + fileName.strip()
                    # check if need switch to an new log file
                    size = os.path.getsize(logFile)
                    if (size <= MAXLOGFILESIZE):
                        newLogFile = logFile
        return newLogFile

    @staticmethod
    def is_valid_date(datastr):
        '''
        Judge if date valid
        '''
        try:
            time.strptime(datastr, "%Y-%m-%d_%H%M%S")
            return True
        except Exception:
            return False

    @staticmethod
    def getScriptLogger():
        filePath = os.path.split(os.path.realpath(__file__))[0]
        afilename = "%s/../output/log/script_%s.log" % (
            filePath, DefaultValue.GetHostIpOrName())

        log = logging.getLogger()
        LoggerFactory._addFileHandle(log, afilename)
        return log

    @staticmethod
    def _addFileHandle(log, fileName):
        # create log file
        if not os.path.exists(os.path.dirname(fileName)):
            dir_permission = 0o700
            os.makedirs(os.path.dirname(fileName), mode=dir_permission)
        else:
            if oct(os.stat(fileName).st_mode)[-3:] != '600':
                os.chmod(fileName, DefaultValue.KEY_FILE_PERMISSION)

        fmt = logging.Formatter(
            '[%(asctime)s][%(filename)s][line:%(lineno)d][%(levelname)s] '
            '%(message)s',
            '%Y-%m-%d %H:%M:%S')
        # output the log to a file
        # 16M takes precedence over 20M, Here cut the file does not trigger
        rthandler = logging.handlers.RotatingFileHandler(
            fileName,
            maxBytes=20 * 1024 * 1024,
            backupCount=2)
        rthandler.setFormatter(fmt)
        rthandler.setLevel(logging.DEBUG)
        log.handlers = []
        log.addHandler(rthandler)

    @staticmethod
    def _addConsoleHandle(log):
        fmt = logging.Formatter('%(message)s')
        # output the log to screen the same time
        console = logging.StreamHandler()
        console.setFormatter(fmt)
        console.setLevel(logging.INFO)
        log.addHandler(console)
        log.setLevel(logging.DEBUG)
