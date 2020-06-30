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
# Description  : Signal.py is utility to process signal
#############################################################################
import sys
import signal

sys.path.append(sys.path[0] + "/../../")
from gspylib.common.ErrorCode import ErrorCode


class Signal(object):

    ##########################################################################
    # init signal handler
    ##########################################################################

    def __init__(self, logger):
        """
        function: initialize signal handler
        input : object logger
        output: NA
        """
        self.logger = logger
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGQUIT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGALRM, signal.SIG_IGN)
        signal.signal(signal.SIGHUP, signal.SIG_IGN)
        signal.signal(signal.SIGUSR1, signal.SIG_IGN)
        signal.signal(signal.SIGUSR2, signal.SIG_IGN)

    def setSignalEvent(self, functionName=None):
        """
        function: initialize signal handler
        input : function
        output: NA
        """
        if (functionName is not None):
            signal.signal(signal.SIGINT, functionName)
        else:
            signal.signal(signal.SIGINT, signal.SIG_IGN)

    def print_signal_stack(self, frame):
        """
        function: Function to print signal stack
        input : frame
        output: NA
        """
        if (self.logger is None):
            return
        try:
            import inspect
            stacks = inspect.getouterframes(frame)
            for curr in range(len(stacks)):
                stack = stacks[curr]
                self.logger.debug("Stack level: %d. File: %s. Function: %s. "
                                  "LineNo: %d." % (curr, stack[1], stack[3],
                                                   stack[2]))
                self.logger.debug("Code: %s." %
                                  (stack[4][0].strip().strip("\n")))
        except Exception as e:
            self.logger.debug("Failed to print signal stack. Error: \n%s"
                              % str(e))

    def raise_handler(self, signal_num, frame):
        """
        function: Function to raise handler
        input : signal_num, frame
        output: NA
        """
        if (self.logger is not None):
            self.logger.debug("Received signal[%d]." % (signal_num))
            self.print_signal_stack(frame)
        raise Exception(ErrorCode.GAUSS_516["GAUSS_51614"] % (signal_num))

    def setupTimeoutHandler(self):
        """
        function: Function to set up time out handler
        input : NA
        output: NA
        """
        signal.signal(signal.SIGALRM, self.timeout_handler)

    def setTimer(self, timeout):
        """
        function: Function to set timer
        input : timeout
        output: NA
        """
        self.logger.debug("Set timer. The timeout: %d." % timeout)
        signal.signal(signal.SIGALRM, self.timeout_handler)
        signal.alarm(timeout)

    def resetTimer(self):
        """
        function: Reset timer
        input : NA
        output: NA
        """
        signal.signal(signal.SIGALRM, signal.SIG_IGN)
        self.logger.debug("Reset timer. Left time: %d." % signal.alarm(0))

    def timeout_handler(self, signal_num, frame):
        """
        function: Received the timeout signal
        input : signal_num, frame
        output: NA
        """
        if (self.logger is not None):
            self.logger.debug("Received the timeout signal: [%d]."
                              % (signal_num))
            self.print_signal_stack(frame)
        raise Timeout("Time out.")


class Timeout(Exception):
    pass
