# -*- coding=utf-8 -*-
# ############################################################################
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
# Description  : TaskPool.py is a utility to manage tasks.
# ############################################################################

import os
import signal
import subprocess
import sys
import stat
import threading
import time
from threading import Timer


class WriterThread(threading.Thread):
    """
    class writer.
    Thread that processes the result content from TaskThread
     and writes the result content to a file.
    """

    def __init__(self, f_out, f_std):
        super(WriterThread, self).__init__()
        self.out_file = f_out
        self.err_file = f_std

        self.stdout = None
        self.stderr = None

    def run(self):
        """
        Writing the result content to a file.
        """
        if self.out_file:
            if not os.path.exists(self.out_file):
                try:
                    os.mknod(self.out_file, stat.S_IWUSR | stat.S_IRUSR)
                except IOError as e:
                    raise Exception("[GAUSS-50206]  : Failed to create file"
                                    " or directory. Error:\n%s." % str(e))
            with open(self.out_file, 'wb', buffering=1) as fp_out:
                fp_out.write(self.stdout.encode('utf-8'))

        if self.err_file:
            if not os.path.exists(self.err_file):
                try:
                    os.mknod(self.err_file, stat.S_IWUSR | stat.S_IRUSR)
                except IOError as e:
                    raise Exception("[GAUSS-50206]  : Failed to create file"
                                    " or directory. Error:\n%s." % str(e))
            with open(self.err_file, 'wb', buffering=1) as fp_err:
                fp_err.write(self.stderr.encode('utf-8'))


class TaskThread(threading.Thread):
    """
    class task
    Starts a task thread.
    """

    def __init__(self, host, cmd, f_out="", f_err="",
                 detail=False, timeout=0, shell_mode=False, inline=False):
        super(TaskThread, self).__init__()
        self.setDaemon(True)

        self.host = host
        self.cmd = cmd
        self.detail = bool(detail)
        self.timeout = timeout
        self.shell_mode = shell_mode
        self.inline = inline

        self.status = 0
        self.stdout, self.stderr = "", ""
        self.failures = []
        self.proc = None
        self.timestamp = time.time()
        self.isKill = False
        self.writer = WriterThread(f_out, f_err) if (f_out or f_err) else None

    def kill(self):
        """
        Kill the process of cmd.
        :param : NA
        :return: NA
        """
        self.failures.append("Timed out")
        # kill process
        if self.proc:
            self.proc.kill()
        self.isKill = True
        # Set the status
        self.status = -1 * signal.SIGKILL
        self.failures.append("Killed by signal %s" % signal.SIGKILL)

    def get_elapsed_time(self):
        """
         Getting elapsed timestamp.
        :return: timestamp
        """
        return time.time() - self.timestamp

    def check_timeout(self):
        """
        check timed-out process 
        """
        if self.isKill or self.timeout <= 0:
            return False
        timeleft = self.timeout - self.get_elapsed_time()
        if timeleft <= 0:
            return True
        return False

    def run(self):
        """
        Execute the cmd on host.
        :return: NA
        """
        self.timestamp = time.time()
        self.proc = subprocess.Popen(self.cmd, shell=False,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE)

        stdout, stderr = self.proc.communicate()
        self.stdout += stdout.decode('utf-8')
        self.stderr += stderr.decode('utf-8')
        self.status = self.proc.returncode

    def __print_out(self):
        if not self.stdout and not self.stderr:
            return
        if self.shell_mode:
            sys.stderr.write("%s" % self.stderr)
            sys.stdout.write("%s" % self.stdout)
        else:
            if self.stdout:
                sys.stdout.write("%s: %s" % (self.host, self.stdout))
        # Use [-1] replace of .endswith, can avoid the problem about
        # coding inconsistencies
        if self.stdout and self.stdout[-1] != os.linesep:
            sys.stdout.write(os.linesep)
        if self.shell_mode and self.stderr and self.stderr[-1] != os.linesep:
            sys.stderr.write(os.linesep)

    def __print_result(self, index):
        """
        Print the result into sys.stdout
        :return: NA
        """
        if self.shell_mode:
            str_ = ""
        else:
            str_ = "[%s] %s [%s] %s" % (
                index,
                time.asctime().split()[3],
                "SUCCESS" if not self.status else "FAILURE",
                self.host
            )
            if self.status > 0:
                str_ += " Exited with error code %s" % self.status

        if self.failures:
            failures_msg = ", ".join(self.failures)
            str_ = str_ + " " + failures_msg

        if str_:
            print(str_)
        if self.inline:
            sys.stdout.write("%s" % self.stdout)

    def write(self, index):
        """
        Write the output into sys.stdout and files.
        :return: object of writer or None
        """
        # Print the stdout into sys.stdout
        if self.detail:
            self.__print_out()
        # Print the status
        self.__print_result(index)

        # Write the self.stdout and self.stderr into files.
        if self.writer:
            self.writer.stdout = self.stdout
            self.writer.stderr = self.stderr
            self.writer.start()
        return self.writer


class TaskPool(object):
    """
    class manager
    """

    def __init__(self, opts):
        """
        Initialize
        """
        self.out_path = opts.outdir
        self.err_path = opts.errdir
        self.detail = True
        self.parallel_num = opts.parallel
        self.timeout = opts.timeout
        self.shell_mode = opts.shellmode
        self.inline = opts.inline

        self.tasks = []
        self.running_tasks = []
        self.writers = []
        self.task_status = {}

    def __get_task_files(self, host):
        """
        Obtain the result file of the task.
        """
        std_path = ""
        if self.out_path:
            std_path = os.path.join(self.out_path, host)

        err_path = ""
        if self.err_path:
            err_path = os.path.join(self.err_path, host)

        return std_path, err_path

    def add_task(self, host, cmd):
        """
        Adding a Task to the Task Pool
        """

        f_out, f_err = self.__get_task_files(host)
        task = TaskThread(host, cmd, f_out, f_err, self.detail, self.timeout,
                          self.shell_mode, self.inline)
        self.tasks.append(task)

    def __get_writing_task(self):
        """
        Check the task status and obtain the running tasks.
        """
        still_running = []
        not_running = []

        # Check whether the task times out. If the task times out,
        # stop the task.
        for task in self.running_tasks:
            if task.check_timeout():
                task.kill()

        # filter the still running tasks and not running tasks
        for task in self.running_tasks:
            if task.isAlive():
                still_running.append(task)
            else:
                self.task_status[task.host] = task.status
                not_running.append(task)

        # Start the writing thread of completed tasks
        for task in not_running:
            index = len(self.writers) + 1
            writer = task.write(index)
            if writer:
                self.writers.append(writer)

        self.running_tasks = still_running

    def __start_limit_task(self):
        """
        Starts the tasks within a specified number of parallel.
        """
        while self.tasks and len(self.running_tasks) < self.parallel_num:
            task = self.tasks.pop(0)
            self.running_tasks.append(task)
            task.start()

    def start(self):
        """
        Start to execute all tasks.
        """
        # Create the path of stdout and stderr
        dir_permission = 0o700
        if self.out_path and not os.path.exists(self.out_path):
            os.makedirs(self.out_path, mode=dir_permission)
        if self.err_path and not os.path.exists(self.err_path):
            os.makedirs(self.err_path, mode=dir_permission)

        # Do cmd
        while self.tasks or self.running_tasks:
            self.__get_writing_task()
            self.__start_limit_task()

        # Waiting for writing files complete.
        for writer in self.writers:
            writer.join()

        return list(self.task_status.values())


def read_host_file(host_file):
    """
    Reads the host file.
    Lines are of the form: host.
    Returns a list of host triples.
    """
    hosts = []
    try:
        if not os.path.isfile(host_file):
            raise Exception("[GAUSS-50201] : The %s does not exist." %
                            host_file)
        with open(host_file) as fp:
            for line in fp:
                line = line.strip()
                if line or not line.startswith('#'):
                    hosts.append(line)
    except (OSError, IOError) as err:
        sys.stderr.write('Could not open hosts file: %s\n' % err)
        sys.exit(1)

    return hosts
