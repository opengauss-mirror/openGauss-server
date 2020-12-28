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
import inspect
import logging
import os
import pydoc
import re
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta
from logging import handlers
from threading import Thread, Event

import dateutil.parser


class RepeatTimer(Thread):
    """
    This class inherit from threading.Thread, it is used for periodic execution 
    function at a specified time interval.
    """

    def __init__(self, interval, function, *args, **kwargs):
        Thread.__init__(self)
        self._interval = interval
        self._function = function
        self._args = args
        self._kwargs = kwargs
        self._finished = Event()

    def run(self):
        while not self._finished.is_set():
            self._finished.wait(self._interval)
            self._function(*self._args, **self._kwargs)
        self._finished.set()

    def cancel(self):
        self._finished.set()


class SuppressStreamObject:
    """
    This class supress standard stream object 'stdout' and 'stderr' in context.
    """

    def __init__(self):
        self.default_stdout_fd = sys.stdout.fileno()
        self.default_stderr_fd = sys.stderr.fileno()
        self.null_device_fd = [os.open(os.devnull, os.O_WRONLY), os.open(os.devnull, os.O_WRONLY)]
        self.standard_stream_fd = (os.dup(self.default_stdout_fd), os.dup(self.default_stderr_fd))

    def __enter__(self):
        os.dup2(self.null_device_fd[0], self.default_stdout_fd)
        os.dup2(self.null_device_fd[1], self.default_stderr_fd)

    def __exit__(self, *args):
        os.dup2(self.standard_stream_fd[0], self.default_stdout_fd)
        os.dup2(self.standard_stream_fd[1], self.default_stderr_fd)
        os.close(self.null_device_fd[0])
        os.close(self.null_device_fd[1])


class Daemon:
    """
    This class implement the daemon.
    """
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
        for path in (self.stdout, self.stderr, self.pid_file):
            dirname = os.path.dirname(path)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
        if os.path.exists(self.pid_file):
            raise RuntimeError("Process alreadly running, can't start again.")
        try:
            if os.fork() > 0:
                raise SystemExit(0)
        except OSError as e:
            raise RuntimeError('Process fork failed.')

        os.setsid()
        os.umask(0o0077)

        try:
            if os.fork() > 0:
                raise SystemExit(0)
        except OSError as e:
            raise RuntimeError('Process fork failed.')

        sys.stdout.flush()
        sys.stderr.flush()

        with open(self.stdout, mode='ab',buffering=0) as f:
            redirect_stdout_fd = f.fileno()
            os.dup2(redirect_stdout_fd, sys.stdout.fileno())
        with open(self.stderr, mode='ab',buffering=0) as f:
            redirect_stderr_fd = f.fileno()
            os.dup2(redirect_stderr_fd, sys.stderr.fileno())
        with open(self.pid_file, mode='w') as f:
            f.write(str(os.getpid()))

        atexit.register(lambda: os.remove(self.pid_file))

        signal.signal(signal.SIGTERM, self.signal_handler)

    @staticmethod
    def signal_handler(signo, frame):
        raise SystemExit(1)

    def start(self):
        try:
            self.daemon_process()
        except RuntimeError as e:
            sys.stderr.write(str(e) + '\n')
            raise SystemExit(1)

        self.function(*self.args, **self.kwargs)

    def stop(self):
        try:
            if not os.path.exists(self.pid_file):
                sys.stderr.write('Process not running.\n')
                raise SystemExit(1)
            else:
                with open(self.pid_file, mode='r') as f:
                    pid = int(f.read())
                    os.kill(pid, signal.SIGTERM)
        except Exception as e:
            if os.path.exists(self.pid_file):
                os.remove(self.pid_file)
                 

def transform_time_string(time_string, mode='timedelta'):
    """
    Transform time string to timedelta or second, only support 'weeks(W), days(D), 
    hours(H), minutes(M), seconds(S)
    :param time_string: string,  time string like '10S', '20H', '3W'.
    :param mode: string, 'timedelta' or 'to_second', 'timedelta' represent transform 
    time_string to timedelta, 'to_second' represent transform time_string to second.
    :return: 'mode' is 'timedelta', return datetime.timedelta; 'mode' is 'to_second', 
    return int(second).
    """
    if mode not in ('timedelta', 'to_second'):
        raise ValueError('wrong mode {mode} in time_transfer.'.format(mode=mode))

    time_prefix, time_suffix = re.match(r'(\d+)?([WDHMS])', time_string).groups()

    if time_suffix is None:
        raise ValueError('wrong format {time_string} for time_string in time_transfer.'.format(time_string=time_string))

    if time_prefix is None:
        time_prefix = 1
    else:
        time_prefix = int(time_prefix)

    timedelta_mapper = {'W': timedelta(weeks=1),
                        'D': timedelta(days=1),
                        'H': timedelta(hours=1),
                        'M': timedelta(minutes=1),
                        'S': timedelta(seconds=1)}

    second_mapper = {'W': 7 * 24 * 3600, 'D': 24 * 3600, 'H': 3600, 'M': 60, 'S': 1}

    if mode == 'timedelta':
        return timedelta_mapper.get(time_suffix) * time_prefix
    if mode == 'to_second':
        return second_mapper.get(time_suffix) * time_prefix


def detection_logger(log_name, log_path, level):
    """
    Create logger.
    :param log_name: string, log name.
    :param log_name: string, log path.
    :param level: string, log level such as 'INFO', 'WARN', 'ERROR'.
    """
    logger = logging.getLogger(log_name)
    agent_handler = handlers.RotatingFileHandler(filename=log_path,
                                                 maxBytes=1024 * 1024 * 100,
                                                 backupCount=5)
    agent_handler.setFormatter(logging.Formatter("[%(asctime)s %(levelname)s]-[%(name)s]: %(message)s"))
    logger.addHandler(agent_handler)
    logger.setLevel(getattr(logging, level.upper()) if hasattr(logging, level.upper()) else logging.INFO)
    return logger


def unify_byte_unit(byte_info):
    """
    Transfer unit of K、M、G、T、P to M
    :param byte_info: string, byte information like '100M', '2K', '30G'.
    :return: int, bytes size in unit of M, like '400M' -> 400.
    """
    byte_info = byte_info.upper()
    bytes_prefix, bytes_suffix = re.match(r'^(\d+|\d+\.\d+)([KMGTP])', byte_info).groups()
    if bytes_prefix is None or bytes_suffix is None or bytes_suffix not in 'KMGTP':
        raise ValueError('can not parse format of {bytes}'.format(bytes=byte_info))
    byte_unit_mapper = {'K': 1 / 1024, 'M': 1, 'G': 1024, 'T': 1024 * 1024, 'P': 1024 * 1024 * 1024}
    return byte_unit_mapper[bytes_suffix] * int(float(bytes_prefix))


def get_funcs(thing):
    """
    Acquire functions in python file.
    :param thing: python module.
    :return: list(function name, function object), function name and corresponding function object 
    in python module.
    """
    funcs = []
    _object, _ = pydoc.resolve(thing)
    _all = getattr(_object, '__all__', None)
    for key, value in inspect.getmembers(_object, inspect.isroutine):
        if _all is not None or inspect.isbuiltin(value) or inspect.getmodule(value) is _object:
            if pydoc.visiblename(key, _all, _object):
                funcs.append((key, value))
    return funcs


def check_certificate(certificate_path):
    """
    Check whether the certificate is expired or invalid.
    :param certificate_path: path of certificate.
    output: dict, check result which include 'check status' and 'check information'.
    """
    check_result = {}
    certificate_warn_threshold = 365
    child = subprocess.Popen(['openssl', 'x509', '-in', certificate_path, '-noout', '-dates'],
                             shell=False, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
    sub_chan = child.communicate()
    if sub_chan[1] or not sub_chan[0]:
        check_result['status'] = 'fail'
    else:
        check_result['status'] = 'success'
        not_after = sub_chan[0].decode('utf-8').split('\n')[1].split('=')[1].strip()
        end_time = dateutil.parser.parse(not_after).replace(tzinfo=None)
        certificate_remaining_days = (end_time - datetime.now()).days
        if 0 < certificate_remaining_days < certificate_warn_threshold:
            check_result['level'] = 'warn'
            check_result['info'] = "the '{certificate}' has {certificate_remaining_days} days before out of date." \
                .format(certificate=certificate_path,
                        certificate_remaining_days=certificate_remaining_days)
        elif certificate_remaining_days >= certificate_warn_threshold:
            check_result['level'] = 'info'
            check_result['info'] = "the '{certificate}' has {certificate_remaining_days} days before out of date." \
                .format(certificate=certificate_path,
                        certificate_remaining_days=certificate_remaining_days)
        else:
            check_result['level'] = 'error'
            check_result['info'] = "the '{certificate}' is out of date." \
                .format(certificate=certificate_path)
    return check_result
