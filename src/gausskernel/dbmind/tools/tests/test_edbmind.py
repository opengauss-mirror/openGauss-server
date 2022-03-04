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
import shutil
from unittest import mock

from dbmind import constants
from dbmind.cmd import edbmind
from dbmind.common.dispatcher import task_scheduler


def mock_setup_directory(confpath):
    os.makedirs(confpath, exist_ok=True)
    src_confile = os.path.join(constants.MISC_PATH, constants.CONFILE_NAME)
    dst_confile = os.path.join(confpath, constants.CONFILE_NAME)
    shutil.copyfile(src_confile, dst_confile)


def test_startup():
    confpath = 'tmp'
    mock_setup_directory(confpath)
    task_scheduler.TimedTaskManager.start = mock.Mock()
    assert len(task_scheduler.TimedTaskManager.timers) == 0
    _dbmind = edbmind.DBMindMain(confpath)
    _dbmind.daemonize = mock.Mock()
    _dbmind.run = mock.Mock()
    _dbmind.start()
    _dbmind.daemonize.assert_called_once_with()

    # TODO: check whether DBMind bound timed app.
