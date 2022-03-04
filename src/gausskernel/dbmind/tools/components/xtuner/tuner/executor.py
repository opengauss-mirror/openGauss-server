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

import getpass
import logging
import shlex
import socket
import subprocess
import threading
import time

import paramiko

n_stdin = 0
n_stdout = 1
n_stderr = 2


def bytes2text(bs):
    """
    Converts bytes (or array-like of bytes) to text.

    :param bs: Bytes or array-like of bytes.
    :return: Converted text.
    """
    if type(bs) in (list, tuple) and len(bs) > 0:
        if isinstance(bs[0], bytes):
            return b''.join(bs).decode(errors='ignore').strip()
        if isinstance(bs[0], str):
            return ''.join(bs).strip()
        else:
            raise TypeError
    elif isinstance(bs, bytes):
        return bs.decode(errors='ignore').strip()
    else:
        return ''


class ExecutorFactory:
    def __init__(self):
        """
        A factory class is used to produce executors.
        Here are two types of executors.
        One is implemented through Popen (generally used for local command execution)
        and the other is implemented through SSH (generally used for remote command execution).
        """
        self.host = None
        self.pwd = None
        self.port = 22
        self.me = getpass.getuser()  # Current executing user.
        self.user = self.me  # Default user is current user.

    def set_host(self, host):
        self.host = host
        return self

    def set_user(self, user):
        self.user = user
        return self

    def set_pwd(self, pwd):
        self.pwd = pwd
        return self

    def set_port(self, port):
        self.port = port
        return self

    def get_executor(self):
        if self._is_remote() or self.user != self.me:
            if None in (self.user, self.pwd, self.port, self.host):
                raise AssertionError

            return SSH(host=self.host,
                       user=self.user,
                       pwd=self.pwd,
                       port=self.port)
        else:
            return LocalExec()

    def _is_remote(self):
        if not self.host:
            return False  # Not setting host is treated as local.

        hostname = socket.gethostname()
        _, _, ip_address_list = socket.gethostbyname_ex(hostname)
        if self.host in ('127.0.0.1', 'localhost') or self.host in ip_address_list:
            return False
        else:
            return True


class Executor:
    """Executor is an abstract class."""

    class Wrapper:
        """inner abstract class for asynchronous execution."""

        def __init__(self, stream):
            self.stream = stream

        def read(self):
            pass

    def exec_command_sync(self, command, *args, **kwargs):
        pass


class SSH(Executor):
    def __init__(self, host, user, pwd, port=22, max_retry_times=5):
        """
        Use the paramiko library to establish an SSH connection with the remote server.
        You can run one or more commands.
        In addition, the `gsql` password information is not exposed.

        :param host: String type.
        :param user: String type.
        :param pwd: String type.
        :param port: Int type.
        :param max_retry_times: Int type. Maximum number of retries if the connection fails.
        """
        self.host = host
        self.user = user
        self.pwd = pwd
        self.port = port
        self.max_retry_times = max_retry_times
        self.retry_cnt = 0
        self.client = SSH._connect_ssh(host, user, pwd, port)

        # Init a thread local variable to save the exit status.
        self._exit_status = threading.local()
        self._exit_status.value = 0

    @staticmethod
    def _connect_ssh(host, user, pwd, port):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(host, port, user, pwd)
        return client

    @property
    def exit_status(self):
        return self._exit_status.value

    def _exec_command(self, command, **kwargs):
        if self.client is None:
            self.client = SSH._connect_ssh(self.host, self.user,
                                           self.pwd, self.port)
        try:
            if type(command) in (list, tuple):
                chan = self.client.get_transport().open_session()
                chan.invoke_shell()

                buff_size = 32768
                timeout = kwargs.get('timeout', None)
                stdout = list()
                stderr = list()
                cmds = list(command)
                # In interactive mode,
                # we cannot determine whether the process exits. We need to exit the shell manually.
                cmds.append('exit $?')
                for line in cmds:
                    chan.send(line + '\n')
                    while not chan.send_ready():  # Wait until the sending is complete.
                        time.sleep(0.1)

                # Wait until all commands are executed.
                start_time = time.monotonic()
                while not chan.exit_status_ready():
                    if chan.recv_ready():
                        stdout.append(chan.recv(buff_size))
                    if chan.recv_stderr_ready():
                        stderr.append(chan.recv_stderr(buff_size))
                    if timeout and (time.monotonic() - start_time) > timeout:
                        break
                    time.sleep(0.1)

                chan.close()
                self._exit_status.value = chan.recv_exit_status()
                result_tup = (bytes2text(stdout), bytes2text(stderr))
            else:
                blocking_fd = kwargs.pop('fd')

                chan = self.client.exec_command(command=command, **kwargs)
                while not chan[blocking_fd].channel.exit_status_ready():  # Blocking here.
                    time.sleep(0.1)

                self._exit_status.value = chan[blocking_fd].channel.recv_exit_status()
                result_tup = (bytes2text(chan[n_stdout].read()), bytes2text(chan[n_stderr].read()))

            self.retry_cnt = 0
            return result_tup
        except paramiko.SSHException as e:
            # reconnect
            self.client.close()
            self.client = SSH._connect_ssh(self.host, self.user,
                                           self.pwd, self.port)

            # Retry until the upper limit is reached.
            if self.retry_cnt >= self.max_retry_times:
                raise ConnectionError("Can not connect with remote host.")

            logging.warning("SSH: %s, so try to reconnect.", e)
            self.retry_cnt += 1
            return self._exec_command(command)

    def exec_command_sync(self, command, *args, **kwargs):
        """
        You can run one or more commands.

        :param command: Type: tuple, list or string.
        :param kwargs: blocking_fd means blocking and waiting for which standard streams.
        :return: Execution result.
        """
        blocking_fd = kwargs.pop('blocking_fd', n_stdout)
        if not isinstance(blocking_fd, int) or blocking_fd > n_stderr or blocking_fd < n_stdin:
            raise ValueError

        return self._exec_command(command, fd=blocking_fd, **kwargs)

    def close(self):
        if self.client:
            self.client.close()
            self.client = None


class LocalExec(Executor):
    _exit_status = None

    def __init__(self):
        """
        Use the subprocess.Popen library to open a pipe.
        You can run one or more commands.
        In addition, the `gsql` password information is not exposed.
        """
        # Init a thread local variable to save the exit status.
        LocalExec._exit_status = threading.local()
        LocalExec._exit_status.value = 0

    @staticmethod
    def exec_command_sync(command, *args, **kwargs):
        if type(command) in (list, tuple):
            stdout = list()
            stderr = list()
            cwd = None
            for line in command:
                # Have to use the `cwd` argument.
                # Otherwise, we can not change the current directory.
                if line.strip().startswith('cd '):
                    cwd = line.lstrip('cd ')
                    continue

                proc = subprocess.Popen(shlex.split(line),
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        shell=False,
                                        cwd=cwd)
                outs, errs = proc.communicate(timeout=kwargs.get('timeout', None))
                LocalExec._exit_status.value = proc.returncode  # Get the last one.
                if outs:
                    stdout.append(outs)
                if errs:
                    stderr.append(errs)
            return [bytes2text(stdout), bytes2text(stderr)]
        else:
            # Pipeline does not support running in shell=False, so we run it with the 'bash -c' command.
            split_cmd = ['bash', '-c', command] if '|' in command or ';' in command else shlex.split(command)
            proc = subprocess.Popen(split_cmd,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    shell=False)
            streams = proc.communicate(timeout=kwargs.get('timeout', None))
            LocalExec._exit_status.value = proc.returncode
            return [bytes2text(stream) for stream in streams]

    @property
    def exit_status(self):
        return LocalExec._exit_status.value
