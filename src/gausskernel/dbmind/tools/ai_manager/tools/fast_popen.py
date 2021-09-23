#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c): 2012-2021, Huawei Tech. Co., Ltd.
# FileName     : fast_popen.py
# Version      : V1.0.0
# Date         : 2021-03-01
# Description  : hiden
#############################################################################

import subprocess
import os

SELF_FD_DIR = "/proc/self/fd"
MAXFD = os.sysconf("SC_OPEN_MAX")


class FastPopen(subprocess.Popen):
    """
    optimization subprocess.Popen when close_fds=True,
    only close the currently opend file,
    reduce the execution time when ulimit is too large
    """

    def __init__(self, cmd, bufsize=0,
                 stdout=None, stderr=None,
                 preexec_fn=None, close_fds=False,
                 cwd=None, env=None, universal_newlines=False,
                 startupinfo=None, creationflags=0, logger=None):

        subprocess.Popen.logger = None
        subprocess.Popen.__init__(self, ["sh", "-"], bufsize=bufsize, executable=None,
                                  stdin=subprocess.PIPE, stdout=stdout, stderr=stderr,
                                  preexec_fn=preexec_fn, close_fds=close_fds, shell=None,
                                  cwd=cwd, env=env, universal_newlines=universal_newlines,
                                  startupinfo=startupinfo, creationflags=creationflags)
        self.logger = logger
        self.cmd = cmd

    def communicate(self, input_cmd=None, timeout=None):
        if input_cmd:
            self.cmd = input_cmd

        if not isinstance(self.cmd, str):
            self.cmd = subprocess.list2cmdline(self.cmd)
        self.cmd = self.cmd.encode()

        std_out, std_err = subprocess.Popen.communicate(self, self.cmd)
        return std_out, std_err

    def _close_fds(self, but):
        if not os.path.exists(SELF_FD_DIR):
            if hasattr(os, 'closerange'):
                os.closerange(3, but)
                os.closerange(but + 1, MAXFD)
            else:
                for i in range(3, MAXFD):
                    if i == but:
                        continue
                    try:
                        os.close(i)
                    except BaseException as bex:
                        if self.logger:
                            self.logger.info("WARNING:%s" % str(bex))
            return

        fd_list = os.listdir(SELF_FD_DIR)
        for fd_h in fd_list:
            if int(fd_h) < 3 or int(fd_h) == but:
                continue
            try:
                os.close(int(fd_h))
            except BaseException as bex:
                if self.logger:
                    self.logger.info("WARNING:%s" % str(bex))