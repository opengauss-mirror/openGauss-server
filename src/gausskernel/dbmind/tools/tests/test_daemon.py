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

import multiprocessing
import os
import shutil
import time

import dbmind.common.process
from dbmind.common.daemon import Daemon
from dbmind.common.platform import WIN32

BASEPATH = os.path.realpath(os.path.dirname(__file__))
PID_NAME = 'tester.pid'


def test_process():
    proc = dbmind.common.process.Process(os.getpid())
    assert proc.alive is True
    assert 'python' in proc.path
    assert os.path.samefile(proc.cwd, os.getcwd())


class DaemonTester(Daemon):
    def __init__(self, q, pid_file):
        self.q = q
        super().__init__(pid_file)

    def clean(self):
        self.q.put(3)
        print('I am cleaning garbage and closing resources.')

    def run(self):
        working_dir = os.path.dirname(self.pid_file)
        os.chdir(working_dir)
        self.q.put(2)
        while True:
            # blocking
            print('I am running.')
            time.sleep(1)

    def start(self):
        self.q.put(1)
        super().start()


def test_daemon():
    old_path = os.getcwd()
    working_dir = os.path.join(BASEPATH, 'tmp')
    PID_PATH = os.path.join(working_dir, PID_NAME)
    os.makedirs(working_dir, exist_ok=True)
    os.chdir(working_dir)
    q = multiprocessing.Queue()

    start_one = DaemonTester(q, PID_PATH)
    stop_one = DaemonTester(None, PID_PATH)

    process = multiprocessing.Process(target=start_one.start)
    process.start()
    start_success = q.get()
    assert start_success == 1
    # run_success = q.get()
    # assert run_success == 2
    assert process.pid != os.getpid()

    while not os.path.exists(PID_PATH):
        time.sleep(0.1)

    file_pid = 0
    with open(PID_PATH) as fp:
        file_pid = int(fp.readline().strip())

    running_proc = dbmind.common.process.Process(file_pid)
    assert running_proc.alive
    assert os.path.samefile(running_proc.cwd, os.getcwd())

    if WIN32:
        # There is a bug on Windows,
        # which raises segmentfault while calling WIN32 API.
        # So we have to bypass the stop() test case.
        process.terminate()
        process.join()
        assert not process.is_alive()
    else:
        stop_one.stop()
        assert not running_proc.alive

    q.close()

    try:
        shutil.rmtree(working_dir)
    except:
        pass
    os.chdir(old_path)
