# -*- coding:utf-8 -*-
#############################################################################
# Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
# Portions Copyright (c) 2007 Agendaless Consulting and Contributors.
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
# Description  : GaussLog.py is utility to handle the log
#############################################################################
import os
import sys
import time
import datetime
import subprocess
import _thread as thread
import re
import logging
import logging.handlers as _handlers
import time
import io
import traceback
import codecs

sys.path.append(sys.path[0] + "/../../")

from gspylib.os.gsfile import g_file
from gspylib.common.Common import DefaultValue
from gspylib.common.ErrorCode import ErrorCode
from gspylib.common.ErrorCode import OmError as _OmError

# import typing for comment.
try:
    from typing import Dict
    from typing import List
except ImportError:
    Dict = dict
    List = list

# max log file size
# 16M
MAXLOGFILESIZE = 16 * 1024 * 1024
# The list of local action in preinstall
PREINSTALL_ACTION = ["prepare_path", "check_os_Version", "create_os_user",
                     "check_os_user", "create_cluster_paths",
                     "set_os_parameter", "set_finish_flag", "set_warning_env",
                     "prepare_user_cron_service", "prepare_user_sshd_service",
                     "set_library", "set_sctp", "set_virtualIp",
                     "clean_virtualIp", "check_hostname_mapping",
                     "init_gausslog", "check_envfile", "check_dir_owner",
                     "set_user_env", "set_tool_env", "gs_preinstall"]

LOG_DEBUG = 1
LOG_INFO = 2
LOG_WARNING = 2.1
LOG_ERROR = 3
LOG_FATAL = 4

#
# _srcfile is used when walking the stack to check when we've got the first
# caller stack frame.
#
if hasattr(sys, 'frozen'):  # support for py2exe
    _srcfile = "logging%s__init__%s" % (os.sep, __file__[-4:])
elif __file__[-4:].lower() in ['.pyc', '.pyo']:
    _srcfile = __file__[:-4] + '.py'
else:
    _srcfile = __file__
_srcfile = os.path.normcase(_srcfile)


class GaussLog:
    """
    Class to handle log file
    """

    def __init__(self, logFile, module="", expectLevel=LOG_DEBUG):
        """
        function: Constructor
        input : NA
        output: NA
        """
        self.logFile = ""
        self.expectLevel = expectLevel
        self.moduleName = module
        self.fp = None
        self.size = 0
        self.suffix = ""
        self.prefix = ""
        self.dir = ""
        self.pid = os.getpid()
        self.step = 0
        self.lock = thread.allocate_lock()
        self.tmpFile = None
        self.ignoreErr = False

        logFileList = ""
        try:
            dirName = os.path.dirname(logFile)
            # check log path
            if (not os.path.exists(dirName)):
                try:
                    topDirPath = DefaultValue.getTopPathNotExist(dirName)
                    self.tmpFile = '%s/topDirPath.dat' % dirName
                    if (not os.path.isdir(dirName)):
                        os.makedirs(dirName,
                                    DefaultValue.KEY_DIRECTORY_PERMISSION)
                except Exception as e:
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50208"] %
                                    dirName + " Error:\n%s" % str(e))
                cmd = "echo %s > '%s/topDirPath.dat' 2>/dev/null && chmod " \
                      "600 '%s/topDirPath.dat'" % (
                          topDirPath, dirName, dirName)
                (status, output) = subprocess.getstatusoutput(cmd)
                if (status != 0):
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50206"]
                                    % "the top path" + " Error:\n%s." % output
                                    + "The cmd is %s" % cmd)
            self.dir = dirName
            originalFileName = os.path.basename(logFile)
            resList = originalFileName.split(".")
            if (len(resList) > 2):
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50224"] +
                                " Error: The file name [%s] can not contain "
                                "more than one '.'." % logFile)
            # check suffix
            (self.prefix, self.suffix) = os.path.splitext(originalFileName)
            if (self.suffix != ".log"):
                raise Exception(
                    ErrorCode.GAUSS_502["GAUSS_50212"] % (logFile, ".log"))

            # get log file list
            logFileList = "%s/logFileList_%s.dat" % (self.dir, self.pid)
            cmd = "ls %s | grep '^%s-' | grep '%s$' > %s" % (
                self.dir, self.prefix, self.suffix, logFileList)
            (status, output) = subprocess.getstatusoutput(cmd)
            if status == 0:
                with open(logFileList, "r") as fp:
                    filenameList = []
                    while True:
                        # get real file name
                        filename = (fp.readline()).strip()
                        if not filename:
                            break
                        existedResList = filename.split(".")
                        if len(existedResList) > 2:
                            continue
                        (existedPrefix, existedSuffix) = \
                            os.path.splitext(filename)
                        if existedSuffix != ".log":
                            continue
                        if len(originalFileName) + 18 != len(filename):
                            continue
                        timeStamp = filename[-21:-4]
                        # check log file name
                        if self.is_valid_date(timeStamp):
                            pass
                        else:
                            continue
                        filenameList.append(filename)

                if len(filenameList):
                    fileName = max(filenameList)
                    self.logFile = self.dir + "/" + fileName.strip()
                    g_file.createFileInSafeMode(self.logFile)
                    self.fp = open(self.logFile, "a")
                    DefaultValue.cleanTmpFile(logFileList)
                    return

            DefaultValue.cleanTmpFile(logFileList)
            # create new log file
            self.__openLogFile()
        except Exception as ex:
            DefaultValue.cleanTmpFile(logFileList)
            print(str(ex))
            sys.exit(1)

    def __del__(self):
        """
        function: Delete tmp file
        input : NA
        output: NA
        """
        if (self.tmpFile is not None and os.path.isfile(self.tmpFile)):
            if self.moduleName not in PREINSTALL_ACTION:
                # If the moduleName is local action in preinstall,
                # we do not delete the file, preinstall will use it to
                # change path owner later.
                if os.access(self.tmpFile, os.R_OK | os.W_OK):
                    os.remove(self.tmpFile)

    def __openLogFile(self):
        """
        function: open log file
        input : NA
        output: NA
        """
        try:
            # get current time
            currentTime = time.strftime("%Y-%m-%d_%H%M%S")
            # init log file
            self.logFile = self.dir + "/" + self.prefix + "-" + currentTime \
                           + self.suffix
            # Re-create the log file to add a retry 3 times mechanism,
            # in order to call concurrently between multiple processes
            retryTimes = 3
            count = 0
            while (True):
                (status, output) = self.__createLogFile()
                if status == 0:
                    break
                count = count + 1
                time.sleep(1)
                if (count > retryTimes):
                    raise Exception(output)
            # open log file
            self.fp = open(self.logFile, "a")
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50206"]
                            % self.logFile + " Error:\n%s" % str(e))

    def __createLogFile(self):
        """
        function: create log file
        input : NA
        output: (status, output)
        """
        try:
            if (not os.path.exists(self.logFile)):
                os.mknod(self.logFile, DefaultValue.KEY_FILE_PERMISSION)
            return (0, "")
        except Exception as e:
            return (1, str(e))

    def is_valid_date(self, datastr):
        """
        function: Judge if date valid
        input : datastr
        output: bool
        """
        try:
            time.strptime(datastr, "%Y-%m-%d_%H%M%S")
            return True
        except Exception as ex:
            return False

    def closeLog(self):
        """
        function: Function to close log file
        input : NA
        output: NA
        """
        try:
            if (self.fp):
                self.fp.flush()
                self.fp.close()
                self.fp = None
        except Exception as ex:
            raise Exception(str(ex))

    # print the flow message to console window and log file
    # AddInfo: constant represent step constant, addStep represent step
    # plus, None represent no step
    def log(self, msg, stepFlag=""):
        """
        function:print the flow message to console window and log file
        input:   msg,stepFlag
        control: when stepFlag="", the OM background log does not display
        step information.
                 when stepFlag="addStep", the OM background log step will
                 add 1.
                 when stepFlag="constant", the OM background log step
                 defaults to the current step.
        output:  NA
        """
        if (LOG_INFO >= self.expectLevel):
            print(msg)
            self.__writeLog("LOG", msg, stepFlag)

    # print the flow message to log file only
    def debug(self, msg, stepFlag=""):
        """
        function:print the flow message to log file only
        input:   msg,stepFlag
        control: when stepFlag="", the OM background log does not display
        step information.
                 when stepFlag="addStep", the OM background log step will
                 add 1.
                 when stepFlag="constant", the OM background log step
                 defaults to the current step.
        output:  NA
        """
        if (LOG_DEBUG >= self.expectLevel):
            self.__writeLog("DEBUG", msg, stepFlag)

    def warn(self, msg, stepFlag=""):
        """
        function:print the flow message to log file only
        input:   msg,stepFlag
        control: when stepFlag="", the OM background log does not display
        step information.
                 when stepFlag="addStep", the OM background log step will
                 add 1.
                 when stepFlag="constant", the OM background log step
                 defaults to the current step.
        output:  NA
        """
        if (LOG_WARNING >= self.expectLevel):
            print(msg)
            self.__writeLog("WARNING", msg, stepFlag)

    # print the error message to console window and log file
    def error(self, msg):
        """
        function: print the error message to console window and log file
        input : msg
        output: NA
        """
        if (LOG_ERROR >= self.expectLevel):
            print(msg)
            self.__writeLog("ERROR", msg)

    # print the error message to console window and log file,then exit
    def logExit(self, msg):
        """
        function: print the error message to console window and log file,
        then exit
        input : msg
        output: NA
        """
        if (LOG_FATAL >= self.expectLevel):
            print(msg)
            try:
                self.__writeLog("ERROR", msg)
            except Exception as ex:
                print(str(ex))
        self.closeLog()
        sys.exit(1)

    def Step(self, stepFlag):
        """
        function: return Step number info
        input: add
        output: step number
        """
        if (stepFlag == "constant"):
            return self.step
        else:
            self.step = self.step + 1
            return self.step

    def __writeLog(self, level, msg, stepFlag=""):
        """
        function: Write log to file
        input: level, msg, stepFlag
        output: NA
        """
        if (self.fp is None):
            return

        try:
            self.lock.acquire()
            # if the log file does not exits, create it
            if (not os.path.exists(self.logFile)):
                self.__openLogFile()
            else:
                LogPer = oct(os.stat(self.logFile).st_mode)[-3:]
                if (not LogPer == "600"):
                    os.chmod(self.logFile, DefaultValue.KEY_FILE_PERMISSION)
            # check if need switch to an new log file
            self.size = os.path.getsize(self.logFile)
            if (self.size >= MAXLOGFILESIZE and os.getuid() != 0):
                self.closeLog()
                self.__openLogFile()

            replace_reg = re.compile(r'-W[ ]*[^ ]*[ ]*')
            msg = replace_reg.sub('-W *** ', str(msg))

            if (msg.find("gs_redis") >= 0):
                replace_reg = re.compile(r'-A[ ]*[^ ]*[ ]*')
                msg = replace_reg.sub('-A *** ', str(msg))

            strTime = datetime.datetime.now()
            if (stepFlag == ""):
                print("[%s][%d][%s][%s]:%s" % (
                    strTime, self.pid, self.moduleName, level, msg), 
                    file=self.fp)
            else:
                stepnum = self.Step(stepFlag)
                print("[%s][%d][%s][%s][Step%d]:%s" % (
                    strTime, self.pid, self.moduleName, level, stepnum, msg),
                      file=self.fp)
            self.fp.flush()
            self.lock.release()
        except Exception as ex:
            self.lock.release()
            if self.ignoreErr:
                return
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50205"]
                            % (("log file %s") % self.logFile) +
                            " Error:\n%s" % str(ex))

    @staticmethod
    def exitWithError(msg, status=1):
        """
        function: Exit with error message
        input: msg, status=1
        output: NA
        """
        sys.stderr.write("%s\n" % msg)
        sys.exit(status)

    @staticmethod
    def printMessage(msg):
        """
        function: Print the String message
        input: msg
        output: NA
        """
        sys.stdout.write("%s\n" % msg)


class FormatColor(object):
    """
    Formatting string for displaying colors on the screen.
    """

    def __init__(self):
        """
        Initialize the format color class.
        """
        pass

    @staticmethod
    def withColor(_string, _foregroundColorID, _backgroundColorID=49):
        """
        Given foreground/background ANSI color codes, return a string that,
        when printed, will format the supplied
         string using the supplied colors.

        :param _string:             The input string.
        :param _foregroundColorID:  The foreground color identify number.
        :param _backgroundColorID:  The background color identify number.

        :type _string:              str
        :type _foregroundColorID:   int
        :type _backgroundColorID:   int

        :return:    Return the string with color.
        :rtype:     str
        """
        return "\x1b[%dm\x1b[%dm%s\x1b[39m\x1b[49m" % (
            _foregroundColorID, _backgroundColorID, _string)

    @staticmethod
    def bold(_string):
        """
        Returns a string that, when printed, will display the supplied
        string in ANSI bold.

        :param _string: The input string.
        :type _string:  str

        :return:    Return the bold string.
        :rtype:     str
        """
        return "\x1b[1m%s\x1b[22m" % _string

    @staticmethod
    def default(_string):
        """
        Get the string with default color.

        :param _string: The input string.
        :type _string:  str

        :return:    Return the string with color.
        :rtype:     str
        """
        return FormatColor.withColor(_string, 30)

    @staticmethod
    def defaultWithBold(_string):
        """
        Get the string with default color.

        :param _string: The input string.
        :type _string:  str

        :return:    Return the string with color.
        :rtype:     str
        """
        return FormatColor.default(FormatColor.bold(_string))

    @staticmethod
    def red(_string):
        """
        Get the string with red color.

        :param _string: The input string.
        :type _string:  str

        :return:    Return the string with color.
        :rtype:     str
        """
        return FormatColor.withColor(_string, 31)

    @staticmethod
    def redWithBold(_string):
        """
        Get the string with red color.

        :param _string: The input string.
        :type _string:  str

        :return:    Return the string with color.
        :rtype:     str
        """
        return FormatColor.red(FormatColor.bold(_string))

    @staticmethod
    def green(_string):
        """
        Get the string with green color.

        :param _string: The input string.
        :type _string:  str

        :return:    Return the string with color.
        :rtype:     str
        """
        return FormatColor.withColor(_string, 32)

    @staticmethod
    def greenWithBold(_string):
        """
        Get the string with green color.

        :param _string: The input string.
        :type _string:  str

        :return:    Return the string with color.
        :rtype:     str
        """
        return FormatColor.green(FormatColor.bold(_string))

    @staticmethod
    def yellow(_string):
        """
        Get the string with yellow color.

        :param _string: The input string.
        :type _string:  str

        :return:    Return the string with color.
        :rtype:     str
        """
        return FormatColor.withColor(_string, 33)

    @staticmethod
    def yellowWithBold(_string):
        """
        Get the string with yellow color.

        :param _string: The input string.
        :type _string:  str

        :return:    Return the string with color.
        :rtype:     str
        """
        return FormatColor.yellow(FormatColor.bold(_string))

    @staticmethod
    def blue(_string):
        """
        Get the string with blue color.

        :param _string: The input string.
        :type _string:  str

        :return:    Return the string with color.
        :rtype:     str
        """
        return FormatColor.withColor(_string, 34)

    @staticmethod
    def blueWithBold(_string):
        """
        Get the string with blue color.

        :param _string: The input string.
        :type _string:  str

        :return:    Return the string with color.
        :rtype:     str
        """
        return FormatColor.blue(FormatColor.bold(_string))

    @staticmethod
    def magenta(_string):
        """
        Get the string with magenta color.

        :param _string: The input string.
        :type _string:  str

        :return:    Return the string with color.
        :rtype:     str
        """
        return FormatColor.withColor(_string, 35)

    @staticmethod
    def magentaWithBold(_string):
        """
        Get the string with magenta color.

        :param _string: The input string.
        :type _string:  str

        :return:    Return the string with color.
        :rtype:     str
        """
        return FormatColor.magenta(FormatColor.bold(_string))

    @staticmethod
    def cyan(_string):
        """
        Get the string with cyan color.

        :param _string: The input string.
        :type _string:  str

        :return:    Return the string with color.
        :rtype:     str
        """
        return FormatColor.withColor(_string, 36)

    @staticmethod
    def cyanWithBold(_string):
        """
        Get the string with cyan color.

        :param _string: The input string.
        :type _string:  str

        :return:    Return the string with color.
        :rtype:     str
        """
        return FormatColor.cyan(FormatColor.bold(_string))

    @staticmethod
    def white(_string):
        """
        Get the string with white color.

        :param _string: The input string.
        :type _string:  str

        :return:    Return the string with color.
        :rtype:     str
        """
        return FormatColor.withColor(_string, 37)

    @staticmethod
    def whiteWithBold(_string):
        """
        Get the string with white color.

        :param _string: The input string.
        :type _string:  str

        :return:    Return the string with color.
        :rtype:     str
        """
        return FormatColor.white(FormatColor.bold(_string))

    @staticmethod
    def hasColors(_stream):
        """
        Returns boolean indicating whether or not the supplied stream
        supports ANSI color.

        :param _stream: The stream instance.
        :type _stream:  file | io.TextIOWrapper

        :return:    Return whether the supplied stream supports ANSI color.
        :rtype:     bool
        """
        if not hasattr(_stream, "isatty") or not _stream.isatty():
            return False

        try:
            # noinspection PyUnresolvedReferences
            import curses as _curses
        except ImportError:
            pass
        else:
            try:
                # Set the current terminal type to the value of the
                # environment variable "TERM".
                _curses.setupterm(None, _stream.fileno())

                return _curses.tigetnum("colors") > 2
            except _curses.error:
                pass

        retCode, output = subprocess.getstatusoutput("tput colors")
        if retCode == 0:
            try:
                return int(output.strip()) > 2
            except (NameError, TypeError, EOFError, SyntaxError):
                return False
        else:
            return False

    @staticmethod
    def screenWidth(_steam):
        """
        Get the command line interface width.

        :param:     The steam instance, such as sys.stdout.
        :type:      file

        :return:    Return the command line interface width.
        :rtype:     int
        """
        try:
            # noinspection PyUnresolvedReferences
            import curses as _curses
        except ImportError:
            pass
        else:
            try:
                _curses.setupterm(None, _steam.fileno())

                return _curses.tigetnum("cols")
            except _curses.error:
                pass

        # Ignore the standard error, sometimes it will cause the tput
        # command to return a wrong value.
        retCode, output = subprocess.getstatusoutput("tput cols")
        if retCode == 0:
            try:
                return int(output.strip())
            except (NameError, TypeError, EOFError, SyntaxError):
                return -1
        else:
            return -1


#
# Progress logger config.
#
# Progress Handler Status.
_PROGRESS_STATUS_START = "STARTING"
_PROGRESS_STATUS_STOP = "STOPPING"
_PROGRESS_STATUS_CHECK = "CHECKING"
_PROGRESS_STATUS_PENDING = "PENDING"
_PROGRESS_STATUS_SUCCESS = "SUCCESS"
_PROGRESS_STATUS_FAILURE = "FAILURE"
_PROGRESS_STATUS_PASSED = "PASSED"
_PROGRESS_STATUS_CANCELED = "CANCELED"
#
# Progress Handler Color.
#
# The color of the status information.
PROGRESS_COLOR_RED = "red"
PROGRESS_COLOR_GREEN = "green"
PROGRESS_COLOR_YELLOW = "yellow"
PROGRESS_COLOR_BLUE = "blue"
PROGRESS_COLOR_MAGENTA = "magenta"
PROGRESS_COLOR_CYAN = "cyan"
PROGRESS_COLOR_WHITE = "white"
PROGRESS_COLOR_DEFAULT = "default"
# The color with bold of the status information.
PROGRESS_COLOR_RED_BOLD = "redWithBold"
PROGRESS_COLOR_GREEN_BOLD = "greenWithBold"
PROGRESS_COLOR_YELLOW_BOLD = "yellowWithBold"
PROGRESS_COLOR_BLUE_BOLD = "blueWithBold"
PROGRESS_COLOR_MAGENTA_BOLD = "magentaWithBold"
PROGRESS_COLOR_CYAN_BOLD = "cyanWithBold"
PROGRESS_COLOR_WHITE_BOLD = "whiteWithBold"
PROGRESS_COLOR_DEFAULT_BOLD = "defaultWithBold"
PROGRESS_COLOR_MAP = {
    PROGRESS_COLOR_RED: FormatColor.red,
    PROGRESS_COLOR_GREEN: FormatColor.green,
    PROGRESS_COLOR_YELLOW: FormatColor.yellow,
    PROGRESS_COLOR_BLUE: FormatColor.blue,
    PROGRESS_COLOR_MAGENTA: FormatColor.magenta,
    PROGRESS_COLOR_CYAN: FormatColor.cyan,
    PROGRESS_COLOR_WHITE: FormatColor.white,
    PROGRESS_COLOR_DEFAULT: FormatColor.default,
    PROGRESS_COLOR_RED_BOLD: FormatColor.redWithBold,
    PROGRESS_COLOR_GREEN_BOLD: FormatColor.greenWithBold,
    PROGRESS_COLOR_YELLOW_BOLD: FormatColor.yellowWithBold,
    PROGRESS_COLOR_BLUE_BOLD: FormatColor.blueWithBold,
    PROGRESS_COLOR_MAGENTA_BOLD: FormatColor.magentaWithBold,
    PROGRESS_COLOR_CYAN_BOLD: FormatColor.cyanWithBold,
    PROGRESS_COLOR_WHITE_BOLD: FormatColor.whiteWithBold,
    PROGRESS_COLOR_DEFAULT_BOLD: FormatColor.defaultWithBold
}
# Default std format.
_LOGGER_DEFAULT_STD_FORMAT = "%(message)s"


class LogColorFormatter(logging.Formatter, object):
    """
    Print colour log information in terminal interface.

    It will only be used in the "StreamHandler" of the logger.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the formatter class.

        :param args:        The additional for default Formatter class.
        :param kwargs:      The additional for default Formatter class.

        :type _isColorful:  bool
        :type args:         str | None
        :type kwargs:       str | None
        """
        logging.Formatter.__init__(self, *args, **kwargs)

    def format(self, _record):
        """
        Format the specified record as text.

        :param _record: The log record instance.
        :type _record:  _logging.LogRecord

        :return:    Return the formatted string.
        :rtype:     str
        """
        try:
            _record.message = _record.getMessage()
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_500["GAUSS_50009"]
                            + " Exception is %r, format string is %r",
                            e, _record.__dict__)

        if FormatColor.hasColors(sys.stderr):
            if _record.levelname == "DEBUG" or _record.levelno == \
                    logging.DEBUG:
                _record.levelname = FormatColor.blue(_record.levelname)
            elif _record.levelname == "INFO" or _record.levelno == \
                    logging.INFO:
                _record.levelname = FormatColor.green(_record.levelname)
            elif _record.levelname == "WARNING" or _record.levelno == \
                    logging.WARN:
                _record.levelname = FormatColor.yellow(_record.levelname)
            elif _record.levelname == "ERROR" or _record.levelno == \
                    logging.ERROR:
                _record.levelname = FormatColor.red(_record.levelname)
            elif _record.levelname == "CRITICAL" or _record.levelno == \
                    logging.CRITICAL:
                _record.levelname = FormatColor.redWithBold(_record.levelname)

        return logging.Formatter.format(self, _record)


class ProgressHandler(logging.StreamHandler, object):
    """
    Print progress handler.

    This class was used to print progress information on the screen.
    Status flag can be displayed colorful.

    For example:
    In a process, the first step is to display it on the screen.
        [STARTING] Starting the cluster.
    Next, the transaction in the process is processed.
    When processing succeed, refresh information on the same line.
        [SUCCESS ] Starting the cluster.
    When processing failure, refresh information on the same line.
        [FAILURE ] Starting the cluster.
    """

    def __init__(self):
        """
        This class was used to print progress information on the screen.
        """
        logging.StreamHandler.__init__(self, sys.stdout)

        # The registered status.
        self.__status_list = {}  # type: Dict[str, str]
        # Width of middle brackets.
        self.__wide = 0
        # The start flag.
        self._isStart = False
        # The storage flag.
        self._storeMessage = []

    def registerStatus(self, status, color):
        """
        Register the status information and the color of the status
        information.

        Repeated addition of status strings does not refresh status string
        color information

        :param status:  The status information.
        :param color:   the color of the status information.

        :type status:   str
        :type color:    str
        """
        if color in PROGRESS_COLOR_MAP.keys():
            handler = PROGRESS_COLOR_MAP.get(color)
            self.__status_list.setdefault(status, handler(status))
            if len(status) > self.__wide:
                self.__wide = len(status)

    # noinspection PyBroadException
    def emit(self, record):
        """
        Emit a record.

        The following progress information can be printed:
            Progress information of start/end type.
                Start the progress: Logger.start(self, message, status =
                None, *args, **kwargs)
                End the progress:   Logger.stop(self, message, status =
                None, *args, **kwargs)
            Progress information of current-step/total-step type.
            Progress information of percentage type.

        :param record:  log record instance.
        :type record:   logging.LogRecord
        """
        try:
            # If the current step text does not exists, save it, otherwise
            # delete the saved copy.
            fs = self.format(record)
            # Get the screen width, and clip the string to adapt the screen.
            screenWidth = FormatColor.screenWidth(self.stream)

            if hasattr(record, "progress_handler_status_start"):
                status = record.progress_handler_status_start
                length = self.__wide - len(status)
                if status in self.__status_list:
                    status = self.__status_list[status]
                fs = "[%s] %s" % (status.center(len(status) + length), fs)
                if 0 < screenWidth <= len(fs):
                    fs = fs[:screenWidth - 4] + "..."

                self.stream.write(fs)
                self._isStart = True
            elif hasattr(record, "progress_handler_status_stop"):
                status = record.progress_handler_status_stop
                length = self.__wide - len(status)
                if status in self.__status_list:
                    status = self.__status_list[status]
                fs = "\r[%s] %s\n" % (status.center(len(status) + length), fs)
                if 0 < screenWidth <= len(fs):
                    fs = fs[:screenWidth - 4] + "...\n"

                self.stream.write(fs)
                self._isStart = False
            elif self._isStart:
                # Use the other formatter
                formatter = LogColorFormatter(_LOGGER_DEFAULT_STD_FORMAT)
                fs = formatter.format(record) + "\n"
                # Store the message.
                self._storeMessage.append(fs)
            else:
                # Use the other formatter
                formatter = LogColorFormatter(_LOGGER_DEFAULT_STD_FORMAT)
                fs = formatter.format(record) + "\n"

                self.stream.write(fs)

            if not self._isStart:
                # Flush the stop message.
                self.flush()
                # Print the warning or error message.
                for message in self._storeMessage:
                    self.stream.write(message)
                # Clean the storage message.
                del self._storeMessage[:]

            self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            self.handleError(record)


class RotatingFileHandler(_handlers.RotatingFileHandler, object):
    """
    Handler for logging to a set of files, which switches from one file to
    the next when the current file reaches
     a certain size.
    """

    def __init__(self, filename, mode='a', maxBytes=0, backupCount=0,
                 encoding=None, delay=False):
        """
        Open the specified file and use it as the stream for logging.

        By default, the file grows indefinitely. You can specify particular
        values of maxBytes and backupCount to allow the file to rollover at
        a predetermined size.

        Rollover occurs whenever the current log file is nearly maxBytes in
        length. If backupCount is >= 1, the system will successively create
        new files with the same pathname as the base file, but with extensions
        ".1", ".2" etc. appended to it. For example, with a backupCount of 5
        and a base file name of "app.log", you would get "app.log",
        "app.log.1", "app.log.2", ... through to "app.log.5". The file being
        written to is always "app.log" - when it gets filled up, it is closed
        and renamed to "app.log.1", and if files "app.log.1", "app.log.2" etc.
        exist, then they are renamed to "app.log.2", "app.log.3" etc.
        respectively.

        If maxBytes is zero, rollover never occurs.

        :param filename:    The base file name for the logger.
        :param mode:        The file open mode, default is "a".
        :param maxBytes:    The max size of the log file.
        :param backupCount: The back up count of the log file.
        :param encoding:    The encoding type of the log file.
        :param delay:       Whether to delay to open the file.

        :type filename:     str
        :type mode:         str
        :type maxBytes:     int
        :type backupCount:  int
        :type encoding:     str | None
        :type delay:        bool
        """
        # Store the base file name before the parent initialization.
        self.baseFilename = filename.strip() if str else filename
        # The real log file name is equal with the base log file name.
        self._currentFileName = self.baseFilename

        # Check the log file list, get the last log file.
        self.__getNewestFile()

        _handlers.RotatingFileHandler.__init__(self, filename, mode, maxBytes,
                                               backupCount, encoding, delay)

    def doRollover(self):
        """
        Do a rollover, as described in __init__().
        """
        # Close the output stream.
        if self.stream:
            self.stream.close()
            self.stream = None

        # Change the log file.
        self.__getNewestFile()

        # Open an new output stream.
        if sys.version_info <= (2, 6) or not self.delay:
            self.stream = self._open()

    def _open(self):
        """
        Open the file stream.

        :return:    Return the file descriptor.
        :rtype:     file
        """
        # Get the log dir.
        logDir = os.path.dirname(self.baseFilename)
        # Check whether the log file directory exist.
        try:
            if not os.path.exists(logDir):
                # Create the log dir.
                os.makedirs(logDir, DefaultValue.KEY_DIRECTORY_PERMISSION)
        except OSError:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50208"], logDir)

        # Open the file.
        if self.encoding is None:
            stream = open(self._currentFileName, self.mode)
        else:
            stream = codecs.open(self._currentFileName, self.mode,
                                 self.encoding)

        return stream

    def __getNewestFile(self):
        """
        Get the newest log file.

        :rtype: None
        """

        def getNewFileName(_filePath):
            """
            Get the real file name from the base file name.

            :param _filePath:    The input base file path.
            :type _filePath:     str

            :return:    Return the real file name from the base file name.
            :rtype:     str
            """
            # Get tht log directory.
            _dirName = os.path.dirname(_filePath)
            _fileName = os.path.basename(_filePath)

            # Get the prefix and the suffix.
            _prefix, _suffix = os.path.splitext(os.path.basename(_fileName))

            # Get the new file name.
            _newFileName = "%(_prefix)s_%(timeStamp)s%(suffix)s" \
                           % {"_prefix": _prefix.lower(),
                              "timeStamp": time.strftime("%Y-%m-%d_%H%M%S"),
                              "suffix": _suffix}
            return os.path.join(_dirName, _newFileName)

        # Initialize the file log handler.
        if self._currentFileName != self.baseFilename:
            self._currentFileName = getNewFileName(self.baseFilename)
            return

        dirName = os.path.dirname(self.baseFilename)
        # If the log file path does not exist, create it, and generate the
        # real log file name.
        try:
            # Check whether the log file directory exist.
            if not os.path.exists(dirName):
                # Create the log dir.
                os.makedirs(dirName, DefaultValue.KEY_DIRECTORY_PERMISSION)
                # Save the real log file name.
                self._currentFileName = getNewFileName(self.baseFilename)
                return
        except OSError:
            raise _OmError(ErrorCode.GAUSS_502["GAUSS_50208"], dirName)

        # Get the prefix and the suffix.
        prefix, suffix = os.path.splitext(os.path.basename(self.baseFilename))
        # The file name list file.
        fileList = "%(dirName)s/logFileList_%(pid)s.dat" % {"dirName": dirName,
                                                            "pid": os.getpid()}
        try:
            with open(fileList, "w") as fp:
                try:
                    for filename in os.listdir(dirName):
                        if re.match(prefix + "-", filename) and \
                                re.search(suffix + "$", filename):
                            fp.write(filename + '\n')
                except OSError:
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50208"],
                                    dirName)
        except Exception as e:
            raise Exception("open %s in 'w' mode err." % fileList)
        # Get the file list.
        with open(fileList, "r") as fp:
            lines = [line.strip() for line in fp.readlines() if
                     line and line.strip()]

        # Remove the file list file.
        if os.path.exists(fileList):
            os.remove(fileList)

        fileNameList = []
        for fileName in lines:
            # Get the matched file.
            pattern = r"%(prefix)s-(%(timeStamp)s)%(suffix)s" \
                      % {"prefix": prefix,
                         "timeStamp": "\\d{4}-\\d{2}-\\d{2}_\\d{6}",
                         "suffix": suffix}
            match = re.match(pattern, fileName)
            if match:
                timeStamp = match.groups()[0]

                # Check whether the time stamp is valid.
                # noinspection PyBroadException
                try:
                    time.strptime(timeStamp, "%Y-%m-%d_%H%M%S")
                except Exception:
                    print("Time stamp %s type error." % timeStamp)
                    continue
            else:
                continue

            # Add to file list.
            fileNameList.append(fileName)

        # If the log directory does not contain the specified log file.
        if not fileNameList:
            self._currentFileName = getNewFileName(self.baseFilename)
            return

        # Get the newest log file.
        fileName = os.path.join(dirName, max(fileNameList))
        if os.path.getsize(fileName) >= MAXLOGFILESIZE:
            self._currentFileName = getNewFileName(self.baseFilename)
        else:
            self._currentFileName = fileName


class _Logger(logging.Logger, object):
    """
    The logger class, expected to replace GaussLog class.

    Inheriting the "object" class for adapting Python 2.6.
    """

    def __init__(self, name, level=logging.NOTSET):
        """
        Init the logger class.

        :param name:    The logger name.
        :param level:   The default logger level.

        :type name:     str
        :type level:    int
        """
        logging.Logger.__init__(self, name, level)

    def start(self, message, status=None, *args, **kwargs):
        """
        Start a new progress.

        Logger does not check whether progress is over, It requires the user
        to control the beginning and end of the
         progress.

        :param message: Progress messages that need to be recorded.
        :param status:  Progress status information.
        :param args:    A list of parameters for formatting into progress
        messages.
        :param kwargs:  Include two parameters: "exc_info" and "extra".
            exc_info:   Exception information.
            extra:      Additional parameters will be used to initialize log
            record instance.

        :type message:  str
        :type status:   str
        :type args:     *
        :type kwargs:   *
        """
        kwargs.setdefault("extra", {})
        kwargs["extra"].setdefault("progress_handler", True)
        if status is not None and self.hasHandler(ProgressHandler):
            kwargs["extra"].setdefault("progress_handler_status_start", status)

        self.info(message, *args, **kwargs)

    def stop(self, message, status=None, *args, **kwargs):
        """
        End a progress.

        Logger does not check whether progress is over, It requires the user
        to control the beginning and end of the
         progress.

        :param message: Progress messages that need to be recorded.
        :param status:  Progress status information.
        :param args:    A list of parameters for formatting into progress
        messages.
        :param kwargs:  Include two parameters: "exc_info" and "extra".
            exc_info:   Exception information.
            extra:      Additional parameters will be used to initialize log
            record instance.

        :type message:  str
        :type status:   str
        :type args:     *
        :type kwargs:   *
        """
        kwargs.setdefault("extra", {})
        kwargs["extra"].setdefault("progress_handler", False)
        if status is not None and self.hasHandler(ProgressHandler):
            kwargs["extra"].setdefault("progress_handler_status_stop", status)

        self.info(message, *args, **kwargs)

    def hasHandler(self, handler_type):
        """
        Check whether the list contains a handler instance of the specified
        type.

        :param handler_type:    The type of handler.
        :type handler_type:     type

        :return:    If the list contains a handler instance of specified type.
        """
        for handler in self.handlers:
            if handler.__class__ == handler_type:
                return True

        return False

    def addHandler(self, hdlr):
        """
        Add a new log handler.

        StreamHandler and ProgressHandler are conflict.
        StreamHandler will be overwritten by ProgressHandler, but not the
        reverse.
        The instance of progressHandler is unique in a logger instance.

        :param hdlr:    log handler instance
        :type hdlr:     ProgressHandler | _logging.Handler
        """
        for i in range(len(self.handlers) - 1, -1, -1):
            handler = self.handlers[i]

            # StreamHandler will be overwritten by ProgressHandler.
            if handler.__class__ == logging.StreamHandler and hdlr.__class__ \
                    == ProgressHandler:
                self.removeHandler(handler)
            # If ProgressHandler instance is exist, StreamHandler instance
            # will not be inserted into the list.
            elif handler.__class__ == ProgressHandler and hdlr.__class__ == \
                    logging.StreamHandler:
                return

        # call the parent function.
        logging.Logger.addHandler(self, hdlr)

    def findCaller(self, stack_info=False):
        """
        Find the stack frame of the caller so that we can note the source
        file name, line number and function name.

        :rtype: (str, int, str) | (str, int, str, str)
        """
        f = logging.currentframe()

        # On some versions of IronPython, currentframe() returns None if
        # IronPython isn't run with -X:Frames.
        if f is not None:
            f = f.f_back

        rv = "(unknown file)", 0, "(unknown function)"
        while hasattr(f, "f_code"):
            co = f.f_code
            filename = os.path.normcase(co.co_filename)
            # noinspection PyProtectedMember
            if filename in [logging._srcfile, _srcfile]:
                f = f.f_back
                continue

            if sys.version_info[:2] >= (3, 0):
                sInfo = None
                if stack_info:
                    sio = io.StringIO()
                    sio.write('Stack (most recent call last):\n')
                    traceback.print_stack(f, file=sio)
                    sInfo = sio.getvalue()
                    if sInfo[-1] == '\n':
                        sInfo = sInfo[:-1]

                rv = (co.co_filename, f.f_lineno, co.co_name, sInfo)
            else:
                rv = (co.co_filename, f.f_lineno, co.co_name)
            break

        return rv

    def callHandlers(self, record):
        """
        Call the log processing routine to process log information.

        :param record:  Log record instance.
        :type record:   logging.LogRecord
        """
        c = self
        found = 0
        while c:
            for hdlr in c.handlers:
                found += 1
                if record.levelno >= hdlr.level and isinstance(
                        hdlr, ProgressHandler):
                    hdlr.handle(record)
                elif record.levelno >= hdlr.level and not \
                        isinstance(hdlr, ProgressHandler) and \
                        (not hasattr(record, "progress_handler") or
                         getattr(record, "progress_handler") is True):
                    hdlr.handle(record)
            if not c.propagate:
                c = None
            else:
                c = c.parent

        if found == 0:
            raise Exception(ErrorCode.GAUSS_500["GAUSS_50015"]
                            % "You have to register at lease one "
                              "handler for the logger")


class Logger(object):
    """
    Logger management class.

    The main functions are as follows:
        Initialize a unique logger.
        addStreamHandler    Add a command line interface recording routine.
        addProgressHandler  Add a progress information printed routine.
        addFileHandler      Add a file recording routine.
        addFileErrorHandler Add a file error-recording routine.
        addSyslogHandler    Add a syslog recording routine.
        getLogger           Get the unique logger instance.
        setLevel            Set global log level.
    """
    # Logger management instance.
    __instance = None

    # Logger level.
    # Debug level.
    DEBUG = logging.DEBUG
    # Info level.
    INFO = logging.INFO
    # Warning level.
    WARN = logging.WARN
    WARNING = logging.WARNING
    # Error level.
    ERROR = logging.ERROR
    # Fatal level.
    FATAL = logging.FATAL
    CRITICAL = logging.CRITICAL
    # log level not set.
    NOTSET = logging.NOTSET
    # log level list.
    # noinspection PyProtectedMember
    LOG_LEVEL_LIST = [level for level in logging._nameToLevel.keys() if
                      isinstance(level, str)]

    # The default logger format.
    # %(name)s              Name of the logger (logging channel)
    # %(levelno)s           Numeric logging level for the message (DEBUG,
    # INFO, WARNING, ERROR, CRITICAL)
    # %(levelname)s         Text logging level for the message ("DEBUG",
    # "INFO", "WARNING", "ERROR", "CRITICAL")
    # %(pathname)s          Full pathname of the source file where the
    # logging call was issued (if available)
    # %(filename)s          Filename portion of pathname
    # %(module)s            Module (name portion of filename)
    # %(lineno)d            Source line number where the logging call was
    # issued (if available)
    # %(funcName)s          Function name
    # %(created)f           Time when the LogRecord was created (time.time()
    # return value)
    # %(asctime)s           Textual time when the LogRecord was created
    # %(msecs)d             Millisecond portion of the creation time
    # %(relativeCreated)d   Time in milliseconds when the LogRecord was
    # created, relative to the time the logging module
    #                       was loaded (typically at application startup time)
    # %(thread)d            Thread ID (if available)
    # %(threadName)s        Thread name (if available)
    # %(process)d           Process ID (if available)
    # %(message)s           The result of record.getMessage(), computed just
    # as the record is emitted
    LOGGER_DEFAULT_STD_FORMAT = _LOGGER_DEFAULT_STD_FORMAT
    LOGGER_DEFAULT_FORMAT = "%(asctime)s %(module)s [%(levelname)s] %(" \
                            "message)s"
    LOGGER_DEBUG_FORMAT = "%(asctime)s %(module)s - %(funcName)s - line %(" \
                          "lineno)d [%(levelname)s]" \
                          " %(message)s"

    def __init__(self):
        """
        Create logger management class.

        Initialization is performed only when the logger management instance
        is first initialized.
        """
        if Logger.__instance is None:
            # set the unique logger manager instance.
            Logger.__instance = self

            # Set default logger class to support ProgressHandler.
            logging.setLoggerClass(_Logger)

            # set the unique logger instance.
            self._logger = logging.getLogger(os.path.basename(sys.argv[0]))

            # set the default loglevel to info.
            Logger.setLevel(Logger.INFO)
            # add a default stream handler.
            Logger.addStreamHandler()

    def __new__(cls):
        """
        Used to create a single instance class.

        :return:    Returns an uninitialized logger manager instance.
            If the logger manager instance is exist, return it, then we will
            not re-init it.
            If the logger manager instance is not exist, return a new object
            instance, then we will init it to a logger
             manager instance.
        :rtype:     Logger
        """
        if cls.__instance is None:
            return object.__new__(cls)
        else:
            return cls.__instance

    @staticmethod
    def addStreamHandler(loglevel=INFO, fmt=None, date_fmt=None):
        """
        Add a command line interface recording routine.

        :param loglevel:    Log level.
        :param fmt:         Log formatting type.
        :param date_fmt:    Date formatting type.

        :type loglevel:     int
        :type fmt:          basestring
        :type date_fmt:     basestring
        """
        if fmt is None:
            fmt = Logger.LOGGER_DEFAULT_STD_FORMAT

        # create formatter.
        formatter = LogColorFormatter(fmt, date_fmt)

        # create stream handler
        stream = logging.StreamHandler()
        stream.setLevel(loglevel)
        stream.setFormatter(formatter)

        # add stream handler to logger instance.
        logger = Logger.getLogger()
        if not logger.hasHandler(logging.StreamHandler):
            logger.addHandler(stream)

    @staticmethod
    def addProgressHandler():
        """
        Add a progress information printed routine.
        """
        # create formatter.
        formatter = logging.Formatter("%(message)s")

        # create progress handler. Default log level is info.
        progress = ProgressHandler()
        progress.setFormatter(formatter)
        progress.setLevel(Logger.INFO)

        # add progress handler to logger instance.
        logger = Logger.getLogger()
        # ProgressHandler instance is unique.
        if not logger.hasHandler(ProgressHandler):
            logger.addHandler(progress)

    @staticmethod
    def addFileHandler(filename, logLevel=DEBUG, fmt=LOGGER_DEFAULT_FORMAT,
                       date_fmt=None, mode='a',
                       maxBytes=MAXLOGFILESIZE, encoding=None, delay=False):
        """
        Add a file recording routine.

        :param filename:    The log file path.
        :param logLevel:    Log level.
        :param fmt:         Log formatting type.
        :param date_fmt:    Date formatting type.
        :param mode:        Open mode of file.
        :param maxBytes:    The max size of the log file.
        :param encoding:    Encoding format of file.
        :param delay:       Whether to delay to open the file.

        :type filename:     str
        :type logLevel:     int | str
        :type fmt:          str | None
        :type date_fmt:     str | None
        :type mode:         str
        :type maxBytes:     int
        :type encoding:     str | None
        :type delay:        bool
        """
        if fmt is None:
            fmt = Logger.LOGGER_DEFAULT_FORMAT

        # create a formatter.
        formatter = logging.Formatter(fmt, date_fmt)

        # create a file handler
        file_handler = RotatingFileHandler(filename, mode, maxBytes,
                                           encoding=encoding, delay=delay)
        file_handler.setLevel(logLevel)
        file_handler.setFormatter(formatter)

        # add file handler to the logger instance.
        logger = Logger.getLogger()
        logger.addHandler(file_handler)

    @staticmethod
    def addFileErrorHandler(filename, fmt=None, date_fmt=None, mode='a',
                            encoding=None, delay=False):
        """

        Add a file error-recording routine.

        :param filename:    File path.
        :param fmt:         Log formatting type.
        :param date_fmt:    Date formatting type.
        :param mode:        Open mode of file.
        :param encoding:    Encoding format of file.
        :param delay:       Whether to delay to open the file.

        :type filename:     str
        :type fmt:          str | None
        :type date_fmt:     str | None
        :type mode:         str
        :type encoding:     str | None
        :type delay:        bool
        """
        Logger.addFileHandler(filename, logLevel=Logger.ERROR, fmt=fmt,
                              date_fmt=date_fmt, mode=mode,
                              encoding=encoding, delay=delay)

    @staticmethod
    def addSyslogHandler(loglevel=DEBUG, address="/dev/log",
                         facility=_handlers.SysLogHandler.LOG_USER,
                         sockType=None, fmt=None, date_fmt=None):
        """
        Add a syslog recording routine.

        :param loglevel:    Log level.
        :param address:     The target address sent by syslog.
            If address is specified as a string, a UNIX socket is used. To
            log to a local syslogd,
             "SysLogHandler(address="/dev/log")" can be used.
            If address is specified as a tuple, a "sockType" socket is used.
            To log to target host,
             "SysLogHandler(address = (IP or hostname, port))" can be use.
        :param facility:    Syslog facility names
        :param sockType:    Socket type.
            If address is specified as a string, this parameter is not used.
            If address is specified as a tuple,  this parameter is default
            to "socket.SOCK_DGRAM".
        :param fmt:         Log formatting type.
        :param date_fmt:    Date formatting type.

        :type loglevel:     int
        :type address:      str | tuple
        :type facility:     str
        :type sockType:     int
        :type fmt:          str
        :type date_fmt:     str
        """
        if fmt is None:
            fmt = Logger.LOGGER_DEFAULT_FORMAT

        # create a formatter.
        formatter = logging.Formatter(fmt, date_fmt)

        # create a syslog handler.
        if sys.version_info >= (2, 7):
            # noinspection PyArgumentList
            syslog = _handlers.SysLogHandler(address, facility, sockType)
        else:
            syslog = _handlers.SysLogHandler(address, facility)
        syslog.setLevel(loglevel)
        syslog.setFormatter(formatter)

        # add syslog handler to logger instance.
        logger = Logger.getLogger()
        logger.addHandler(syslog)

    @staticmethod
    def getLogger():
        """
        Get the unique logger instance.

        :return:    return the logger instance.
        :rtype:     _Logger
        """
        return Logger()._logger

    @staticmethod
    def setLevel(loglevel):
        """
        Set global log level.

        :param loglevel:    Log level.
        :type loglevel:     int | str

        :return:    Return whether the log level is legal.
        :rtype:     bool
        """
        logger = Logger.getLogger()

        # noinspection PyProtectedMember
        if isinstance(loglevel,
                      str) and loglevel.upper() in logging._levelToName:
            logger.setLevel(loglevel)
            return True
        elif isinstance(loglevel, int) and loglevel in logging._levelToName:
            logger.setLevel(loglevel)
            return True
        else:
            return False

    @staticmethod
    def setFormat(_fmt, _date_fmt=None):
        """
        Setting the log format and the date format.

        :param _fmt:        Log formatting type.
        :param _date_fmt:   Date formatting type.

        :type _fmt:         str
        :type _date_fmt:    str | None
        """
        logger = Logger.getLogger()
        for handler in logger.handlers:
            # Process handler can not accept the other type of the print
            # format.
            # create a formatter.
            formatter = logging.Formatter(_fmt, _date_fmt)
            # Set the formatter.
            handler.setFormatter(formatter)

    @staticmethod
    def registerProgressStatus(status, color):
        """
        Register the status information and the color of the status
        information, which is used to progress handler.

        Repeated addition of status strings does not refresh status string
        color information.

        :param status:  The status information.
        :param color:   the color of the status information.

        :type status:   str
        :type color:    str
        """
        logger = Logger.getLogger()

        for handler in logger.handlers:
            if isinstance(handler, ProgressHandler):
                handler.registerStatus(status, color)


class ProgressLogger(object):
    """
    Progress log printing decorator class.

    Used to simplify progress log printing code.

    This class is callable.
    """
    PROGRESS_STATUS_START = _PROGRESS_STATUS_START
    PROGRESS_STATUS_STOP = _PROGRESS_STATUS_STOP
    PROGRESS_STATUS_CHECK = _PROGRESS_STATUS_CHECK
    PROGRESS_STATUS_PENDING = _PROGRESS_STATUS_PENDING
    PROGRESS_STATUS_SUCCESS = _PROGRESS_STATUS_SUCCESS
    PROGRESS_STATUS_FAILURE = _PROGRESS_STATUS_FAILURE
    PROGRESS_STATUS_PASSED = _PROGRESS_STATUS_PASSED
    PROGRESS_STATUS_CANCELED = _PROGRESS_STATUS_CANCELED

    def __init__(self, message, status=None):
        """
        Init the progress log printing decorator class.

        :param message: The log message.
        :param status:  The progress status message.

        :type message:  str
        :type status:   List[str] | None
        """
        # Store the progress status message.
        if status is None or len(status) <= 1:
            self._status = [_PROGRESS_STATUS_START, _PROGRESS_STATUS_SUCCESS]
        else:
            self._status = status
        # Store the log message.
        self._message = message

    def __call__(self, func):
        """
        Call the original function.

        :param func:    The original function.
        :type func:     function

        :return:    Return the wrapper function.
        :rtype:     function
        """

        def wrapper(*args, **kwargs):
            """
            Decorator function for wrapping log progress information.

            :param args:    The additional parameters of the original function.
            :param kwargs:  The additional parameters of the original function.

            :type args:     *
            :type kwargs:   *

            :return:    Return the result of the original function.
            :rtype:     *
            """
            # Get the logger.
            logger = Logger.getLogger()

            # Start the progress.
            logger.start(self._message, self._status[0])

            try:
                # Call the function.
                ret = func(*args, **kwargs)
            except (SystemExit, KeyboardInterrupt):
                logger.stop(self._message, _PROGRESS_STATUS_CANCELED)
                raise
            except BaseException:
                logger.stop(self._message, _PROGRESS_STATUS_FAILURE)
                raise

            # End the progress.
            logger.stop(self._message, self._status[1])
            return ret

        return wrapper

    @staticmethod
    def initProgressHandler():
        """
        Create a log progress routine for the logger and register
         log progress status type.
        """
        # Add a new progress handler.
        Logger.addProgressHandler()

        # Register progress status that will be used in this program.
        Logger.registerProgressStatus(_PROGRESS_STATUS_START,
                                      PROGRESS_COLOR_BLUE)
        Logger.registerProgressStatus(_PROGRESS_STATUS_SUCCESS,
                                      PROGRESS_COLOR_GREEN)
        Logger.registerProgressStatus(_PROGRESS_STATUS_FAILURE,
                                      PROGRESS_COLOR_RED)
        Logger.registerProgressStatus(_PROGRESS_STATUS_CHECK,
                                      PROGRESS_COLOR_BLUE)
        Logger.registerProgressStatus(_PROGRESS_STATUS_PASSED,
                                      PROGRESS_COLOR_GREEN)
        Logger.registerProgressStatus(_PROGRESS_STATUS_CANCELED,
                                      PROGRESS_COLOR_YELLOW)
