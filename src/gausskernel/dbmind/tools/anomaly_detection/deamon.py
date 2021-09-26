"""
Copyright (c) 2020 Huawei Technologies Co.,Ltd.

openGauss is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:

         http://license.coscl.org.cn/MulanPSL2

THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
"""

import atexit
import os
import signal
import sys

from utils import abnormal_exit


def read_pid_file(filepath):
    """
    Return the pid of the running process recorded in the file,
    and return 0 if the acquisition fails.
    """
    if not os.path.exists(filepath):
        return 0

    try:
        with open(filepath, mode='r') as f:
            pid = int(f.read())
        if os.path.exists('/proc/%d' % pid):
            return pid
        else:
            return 0
    except PermissionError:
        return 0
    except ValueError:
        return 0


def handle_sigterm(signo, frame):
    sys.exit(0)


class Daemon:
    """
    This class implements the function of running a process in the background."""

    def __init__(self):
        self.args = None
        self.kwargs = None
        self.function = None
        self.stdout = None
        self.stderr = None
        self.pid_file = None

    def set_pid_file(self, pid_file='./tmp/anomal_detection.pid'):
        self.pid_file = pid_file
        return self

    def set_stdout(self, stdout='/dev/null'):
        self.stdout = stdout
        return self

    def set_stderr(self, stderr='/dev/null'):
        self.stderr = stderr
        return self

    def set_function(self, function, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.function = function
        return self

    def daemon_process(self):
        # Verify that the pid file is valid.
        read_pid = read_pid_file(self.pid_file)
        if read_pid > 0:
            if os.readlink('/proc/{pid}/cwd'.format(pid=read_pid)) == os.path.dirname(os.path.realpath(__file__)):
                raise RuntimeError("The process is already running.")
            else:
                os.remove(self.pid_file)

        try:
            if os.fork() > 0:
                sys.exit(0)
        except OSError as e:
            raise RuntimeError('Process fork failed: %s.' % e)

        os.setsid()
        os.umask(0o0077)
        try:
            if os.fork() > 0:
                sys.exit(0)
        except OSError as e:
            raise RuntimeError('Process fork failed: %s.' % e)

        sys.stdout.flush()
        sys.stderr.flush()

        for path in (self.stdout, self.stderr, self.pid_file):
            dirname = os.path.dirname(path)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
        with open(self.stdout, mode='ab', buffering=0) as f:
            os.dup2(f.fileno(), sys.stdout.fileno())
        with open(self.stderr, mode='ab', buffering=0) as f:
            os.dup2(f.fileno(), sys.stderr.fileno())
        with open(self.pid_file, mode='w') as f:
            f.write(str(os.getpid()))

        atexit.register(lambda: os.remove(self.pid_file))
        signal.signal(signal.SIGTERM, handle_sigterm)

    def start(self):
        try:
            self.daemon_process()
        except RuntimeError as msg:
            abnormal_exit(msg)

        self.function(*self.args, **self.kwargs)

    def stop(self):
        if not os.path.exists(self.pid_file):
            abnormal_exit("Process not running.")

        read_pid = read_pid_file(self.pid_file)
        if read_pid > 0:
            os.kill(read_pid, signal.SIGTERM)
        # Clean invalid pid file.
        if read_pid_file(self.pid_file) < 0:
            os.remove(self.pid_file)
