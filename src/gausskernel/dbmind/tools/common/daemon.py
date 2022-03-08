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

import abc
import atexit
import os
import signal
import sys
import time

import dbmind.common.process
from .platform import WIN32

write_info = sys.stdout.write
write_error = sys.stderr.write


def read_dbmind_pid_file(filepath):
    """Return the running process's pid from file.
    If the acquisition fails, return 0.

    Note
    ~~~~~~~~

    The func only can read the pid file for DBMind due to specific/fine-grained verification.
    """
    try:
        if not os.path.exists(filepath):
            return 0
        with open(filepath, mode='r') as fp:
            pid = int(fp.readline().strip())
        proc = dbmind.common.process.Process(pid)

        if proc.alive:
            return pid
        else:
            return 0
    except PermissionError:
        return 0
    except ValueError:
        return 0
    except FileNotFoundError:
        return 0


class Daemon:
    """A generic daemon class for DBMind."""

    class STATUS:
        PENDING = 0
        RUNNING = 1

    def __init__(self, pid_file, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        self.pid_file = os.path.realpath(pid_file)
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.status = Daemon.STATUS.PENDING

    def daemonize(self):
        if not WIN32:
            """UNIX-like OS has the double-fork magic."""
            try:
                if os.fork() > 0:
                    sys.exit(0)  # the first parent exits.
            except OSError as e:
                write_error('[Daemon Process]: cannot fork the first process: %s.\n' % e.strerror)
                sys.exit(1)
            # modify env
            os.chdir('/')
            os.setsid()
            os.umask(0)
            try:
                if os.fork() > 0:
                    sys.exit(0)
            except OSError as e:
                write_error('[Daemon Process]: cannot fork the second process: %s.\n' % e.strerror)
                sys.exit(1)

            # redirect standard fd
            sys.stdout.flush()
            sys.stderr.flush()
            os.dup2(sys.stdin.fileno(), open(self.stdin, 'r').fileno())
            os.dup2(sys.stdout.fileno(), open(self.stdout, 'r').fileno())
            os.dup2(sys.stderr.fileno(), open(self.stderr, 'r').fileno())

            atexit.register(self.clean)

        # Write daemon pid file.
        with open(self.pid_file, 'w+') as fp:
            fp.write('%d\n' % os.getpid())

    def start(self):
        """Start the daemon process"""
        # Verify that the pidfile is valid and check if the daemon already runs.
        pid = read_dbmind_pid_file(self.pid_file)
        if pid > 0:
            write_error('[Daemon Process]: process (%d) already exists.\n' % pid)
            sys.exit(1)

        self.daemonize()
        self.status = Daemon.STATUS.RUNNING
        write_info('The process has been started.\n')
        self.run()

    def stop(self, level='low'):
        level_mapper = {'low': signal.SIGTERM, 'mid': signal.SIGQUIT, 'high': signal.SIGKILL}

        """Stop the daemon process"""
        pid = read_dbmind_pid_file(self.pid_file)
        if pid <= 0:
            write_error('[Daemon Process]: process not running.\n')
            return

        # If the pid is valid, try to kill the daemon process.
        try:
            while True:
                # retry to kill
                write_error('Closing the process...\n')
                os.kill(pid, level_mapper[level])
                time.sleep(1)
        except OSError as e:
            if 'No such process' in e.strerror and os.path.exists(self.pid_file):
                os.remove(self.pid_file)

    @abc.abstractmethod
    def clean(self):
        """Cleanup before exit"""

    @abc.abstractmethod
    def run(self):
        """Subclass should override the run() method."""

