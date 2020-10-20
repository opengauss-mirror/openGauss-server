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
import subprocess
import sys

import paramiko


class ExecutorFactory(object):
    def __init__(self):
        self.host = None
        self.pwd = None
        self.port = 22
        self.me = getpass.getuser()  # current executing user
        self.user = self.me  # default user is current user.

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
            return False  # not setting host is treated as local.

        import socket
        hostname = socket.gethostname()
        _, _, ip_address_list = socket.gethostbyname_ex(hostname)
        if self.host == '127.0.0.1' or self.host in ip_address_list:
            return False
        else:
            return True


class Executor(object):
    """Executor is an abstract class."""

    class Wrapper(object):
        """inner abstract class for asynchronized execution."""

        def __init__(self, stream):
            self.stream = stream

        def read(self):
            pass

    def exec_command_sync(self, command, *args, **kwargs):
        pass

    def exec_command_async(self, command, *args, **kwargs):
        pass


class SSH(Executor):
    def __init__(self, host, user, pwd, port=22, retry_limit=5):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.port = port
        self.retry_limit = retry_limit
        self.retry_counter = 0
        self.ssh = SSH._connect_ssh(host, user, pwd, port)

    @staticmethod
    def _connect_ssh(host, user, pwd, port):
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(host, port, user, pwd)
            return ssh
        except Exception as e:
            print(e, "failed to connect.")
            return None

    def _exec_command(self, command, **kwargs):
        if self.ssh is None:
            self.ssh = SSH._connect_ssh(self.host, self.user,
                                        self.pwd, self.port)
            if self.ssh is None:
                raise ConnectionError("Can not connect with remote host.")

        try:
            channels = self.ssh.exec_command(command=command, **kwargs)
            self.retry_counter = 0
            return channels
        except paramiko.SSHException as e:
            self.ssh.close()
            self.ssh = SSH._connect_ssh(self.host, self.user,
                                        self.pwd, self.port)
            if self.ssh is None:
                raise ConnectionError("Can not connect with remote host.")

                # retry until touch retry_limit
            if self.retry_counter >= self.retry_limit:
                raise ConnectionError("Can not connect with remote host.")

            print("{}, so ssh connection retry...".format(e), file=sys.stderr)
            self.retry_counter += 1
            return self._exec_command(command)

    def exec_command_sync(self, command, *args, **kwargs):
        blocking_fd = kwargs.pop('blocking_fd', 1)
        if not isinstance(blocking_fd, int) or blocking_fd > 2 or blocking_fd < 0:
            raise AssertionError

        chan = self._exec_command(command, **kwargs)
        while not chan[blocking_fd].channel.exit_status_ready():  # blocking here
            pass

        def stream2str(stream):
            return stream.read().decode(errors='ignore').strip()

        return stream2str(chan[1]), stream2str(chan[2])

    def exec_command_async(self, command, *args, **kwargs):
        chan = self._exec_command(command, **kwargs)

        class Inner(Executor.Wrapper):
            def read(self):
                return self.stream.read().decode(errors='ignore')

        return Inner(chan[1]), Inner(chan[2])

    def __del__(self):
        self.close()

    def close(self):
        if self.ssh:
            self.ssh.close()
            self.ssh = None


class LocalExec(Executor):
    def __init__(self, limit_time=5):
        self.limit_time = limit_time

    @staticmethod
    def exec_command_async(command, *args, **kwargs):
        class Inner(Executor.Wrapper):
            def read(self):
                if self.stream is None:
                    return ""
                return self.stream.readline().decode(errors='ignore')

        sub = subprocess.Popen(command,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               shell=True)

        return Inner(sub.stdout), Inner(sub.stderr)

    @staticmethod
    def exec_command_sync(command, *args, **kwargs):
        sub = subprocess.Popen(command,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               shell=True)
        ret = [stream.decode(errors='ignore').strip() if stream else '' for stream in sub.communicate()]
        return ret

    def close(self):
        pass
