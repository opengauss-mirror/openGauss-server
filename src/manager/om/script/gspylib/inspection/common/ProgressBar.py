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
import re
import sys
import threading

CLEAR_TO_END = "\033[K"
UP_ONE_LINE = "\033[F"


class ProgressBar(object):
    def __init__(self, width=25, title=''):
        self.width = width
        self.title = ProgressBar.filter_str(title)
        self._lock = threading.Lock()

    @property
    def lock(self):
        return self._lock

    def update(self, progress=0):
        pass

    @staticmethod
    def filter_str(pending_str):
        """remove strings like \r \t \n"""
        return re.sub(pattern=r'\r|\t|\n', repl='', string=pending_str)


class LineProgress(ProgressBar):
    def __init__(self, total=100, symbol='#', width=25, title=''):
        """
         @param total : count of progress bar
         @param symbol : symbol to show
         @param width : width of progress bar
         @param title : text before progress bar
        """
        super(LineProgress, self).__init__(width=width, title=title)
        self.total = total
        self.symbol = symbol
        self._current_progress = 0

    def update(self, progress=0):
        """
        @param progress : current value
        """
        with self.lock:
            if progress > 0:
                self._current_progress = float(progress)
            sys.stdout.write('\r' + CLEAR_TO_END)
            hashes = '=' * int(
                self._current_progress // self.total * self.width)
            spaces = ' ' * (self.width - len(hashes))
            sys.stdout.write("\r%-25s [%s] %d/%d" % (
                self.title, hashes + spaces, self._current_progress,
                self.total))


class MultiProgressManager(object):
    def __new__(cls, *args, **kwargs):
        """singleton"""
        if not hasattr(cls, '_instance'):
            cls._instance = super(MultiProgressManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        self._progress_dict = {}
        self._lock = threading.Lock()

    def put(self, key, progress_bar):
        with self._lock:
            if key and progress_bar:
                self._progress_dict[key] = progress_bar
                progress_bar.index = len(self._progress_dict) - 1

    def clear(self):
        with self._lock:
            self._progress_dict.clear()

    def update(self, key, progress):
        """
        @param key : progress bar key
        @param progress : value
        """
        with self._lock:
            if not key:
                return
            delta_line = len(self._progress_dict)
            sys.stdout.write(
                UP_ONE_LINE * delta_line if delta_line > 0 else '')
            for tmp_key in self._progress_dict.keys():
                progress_bar = self._progress_dict.get(tmp_key)
                tmp_progress = 0
                if key == tmp_key:
                    tmp_progress = progress
                progress_bar.update(tmp_progress)
                sys.stdout.write('\n')
