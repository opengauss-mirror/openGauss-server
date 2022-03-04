# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
import os

from dbmind.common import platform


class Process:
    """A cross-platform class that can be used to obtain process information, including:

        - current working directory
        - command line
        - executable file path
        - alive

    """

    def __init__(self, pid):
        if pid <= 0:
            raise ValueError('Invalid process ID %d' % pid)
        self._pid = pid
        self._cmdline = None
        self._path = None
        self._cwd = None

    def _alive(self):
        if platform.WIN32:
            if not platform.win32_is_process_running(self._pid):
                return False

            self._cmdline = platform.win32_get_process_cmdline(self._pid)
            self._path = platform.win32_get_process_path(self._pid)
            self._cwd = platform.win32_get_process_cwd(self._pid)
            return True
        else:
            # unix-like operation systems are ok.
            if not os.path.exists('/proc/%d' % self._pid):
                return False

            with open('/proc/%d/cmdline' % self._pid, mode='rb') as fp:
                self._cmdline = fp.readline().replace(b'\x00', b' ').decode()
            self._path = os.readlink('/proc/%d/exe' % self._pid)
            self._cwd = os.readlink('/proc/%d/cwd' % self._pid)
            return True

    @property
    def alive(self):
        return self._alive()

    @property
    def cmdline(self):
        if self._cmdline is None:
            self._alive()
        return self._cmdline

    @property
    def path(self):
        if self._path is None:
            self._alive()
        return self._path

    @property
    def cwd(self):
        # `cwd` can change so call `self.alive()` momentarily.
        if self._alive():
            return self._cwd
        else:
            return None